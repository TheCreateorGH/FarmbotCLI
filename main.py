#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "rich>=13.0",
#     "paho-mqtt>=2.0",
#     "requests>=2.31",
#     "pysocks>=1.7",
#     "prompt_toolkit>=3.0",
# ]
# ///
"""
Script Name : farmbot_cli.py
Definition  : Rich command line interface for a FarmBot Genesis.
Explanation : Logs in against the FarmBot web app (REST), opens an MQTT
              connection to the FarmBot message broker and sends CeleryScript
              RPC requests to the bot. Supports moving, homing, photos, pins,
              peripherals, sensors, watering, sequences, saved places, grids,
              batch files, emergency stop, live position and live logs.

Usage       : python farmbot_cli.py                  -> interactive shell
              python farmbot_cli.py move 500 300 0   -> one-shot command
              python farmbot_cli.py help             -> list all commands

Credentials : Environment variables FARMBOT_EMAIL, FARMBOT_PASSWORD and
              (optional) FARMBOT_SERVER (default https://my.farm.bot).
              If they are not set the script asks for them.
Proxy       : Set HTTPS_PROXY (or FARMBOT_PROXY), e.g. http://user:pass@host:8080
              MQTT then tunnels through the proxy via HTTP CONNECT.

Output note : Bot logs arrive from a background thread. While the prompt is
              waiting for input they are printed ABOVE the prompt through
              prompt_toolkit's patch_stdout, so they never end up in the
              input field you are typing in.
"""

from __future__ import annotations

import getpass
import json
import os
import platform
import shlex
import sys
import threading
import time
import uuid
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path
from urllib.parse import unquote, urlparse

import paho.mqtt.client as mqtt
import requests
from prompt_toolkit import HTML, PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.patch_stdout import patch_stdout
from rich.console import Console
from rich.live import Live
from rich.markup import escape
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich.prompt import Confirm
from rich.table import Table

from dotenv import load_dotenv
load_dotenv()


# --------------------------------------------------------------------------- #
# Configuration (adjust to your machine)
# --------------------------------------------------------------------------- #

DEFAULT_SERVER = "https://my.farm.bot"

# Soft limits in mm. Moves outside of these ask for confirmation first.
# Genesis is roughly 2900 x 1400 x 400 mm - adjust to your real bed size.
LIMITS = {"x": (0.0, 2900.0), "y": (0.0, 1400.0), "z": (-400.0, 0.0)}

DEFAULT_SPEED = 100      # movement speed in percent (1-100)
SAFE_Z = 0.0             # height used when travelling between points
WATER_PIN = 8            # pin of the water valve (same as in the Lua scripts)
RPC_PRIORITY = 600       # priority used by the FarmBot web app for RPCs
MAX_GRID_POINTS = 500    # safety cap for the 'grid' command

# Local files (saved places and command history)
PLACES_FILE = Path.home() / ".farmbot_cli_places.json"
HISTORY_FILE = Path.home() / ".farmbot_cli_history"

console = Console()


class FarmBotError(Exception):
    """Any error that should be shown to the user without a traceback."""


# --------------------------------------------------------------------------- #
# CeleryScript node helpers
# --------------------------------------------------------------------------- #

def coordinate(x: float, y: float, z: float) -> dict:
    """Build a CeleryScript coordinate node."""
    return {"kind": "coordinate", "args": {"x": x, "y": y, "z": z}}


def node(kind: str, **args) -> dict:
    """Build a generic CeleryScript node."""
    return {"kind": kind, "args": args}


# --------------------------------------------------------------------------- #
# FarmBot connection (REST for data, MQTT for commands and live state)
# --------------------------------------------------------------------------- #

class FarmBot:
    def __init__(self, server: str) -> None:
        self.server = server.rstrip("/")
        self.token: str | None = None       # encoded JWT
        self.device: str | None = None      # e.g. "device_1234"
        self.mqtt_host: str | None = None
        self.mqtt_ws: str | None = None     # wss://host:443/ws/mqtt
        self.state: dict = {}               # last full status message
        self.show_logs = True               # print device logs as they arrive
        self.cache: dict[str, list] = {}    # REST lists that rarely change

        self._client: mqtt.Client | None = None
        self._connected = threading.Event()
        self._connect_error: str | None = None
        self._waiters: dict[str, dict] = {}  # rpc label -> {"event", "result"}

    # ---- REST -------------------------------------------------------------

    def login(self, email: str, password: str) -> None:
        """Request an API token; it also contains the MQTT broker and device id."""
        try:
            r = requests.post(
                f"{self.server}/api/tokens",
                json={"user": {"email": email, "password": password}},
                timeout=20,
            )
        except requests.RequestException as exc:
            raise FarmBotError(f"Could not reach {self.server}: {exc}") from exc
        if r.status_code != 200:
            raise FarmBotError(f"Login failed (HTTP {r.status_code}): {r.text[:200]}")
        token = r.json()["token"]
        self.token = token["encoded"]
        self.device = token["unencoded"]["bot"]
        self.mqtt_host = token["unencoded"]["mqtt"]
        self.mqtt_ws = token["unencoded"].get("mqtt_ws")

    def api_get(self, path: str):
        """GET a REST resource, e.g. api_get('sequences')."""
        try:
            r = requests.get(
                f"{self.server}/api/{path}",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=20,
            )
        except requests.RequestException as exc:
            raise FarmBotError(f"REST request failed: {exc}") from exc
        if r.status_code != 200:
            raise FarmBotError(f"GET /api/{path} failed (HTTP {r.status_code})")
        return r.json()

    def cached(self, path: str) -> list:
        """Like api_get, but remembers the result (use 'reload' to refresh)."""
        if path not in self.cache:
            self.cache[path] = self.api_get(path)
        return self.cache[path]

    # ---- MQTT -------------------------------------------------------------

    def connect(self) -> None:
        """Try MQTT over TCP (8883) first, then WebSockets over 443 (proxy/firewall friendly)."""
        attempts = [("tcp", self.mqtt_host, 8883, None)]
        if self.mqtt_ws:
            url = urlparse(self.mqtt_ws)
            attempts.append(("websockets", url.hostname, url.port or 443, url.path or "/ws/mqtt"))

        proxy = (os.environ.get("FARMBOT_PROXY") or os.environ.get("HTTPS_PROXY")
                 or os.environ.get("https_proxy") or os.environ.get("ALL_PROXY"))
        if proxy and "://" not in proxy:
            proxy = "http://" + proxy
        errors: list[str] = []

        for transport, host, port, path in attempts:
            label = f"{transport}://{host}:{port}" + (" via proxy" if proxy else "")
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"farmbot_cli_{uuid.uuid4().hex[:8]}",
                transport=transport,
            )
            client.username_pw_set(self.device, self.token)
            client.tls_set()
            if transport == "websockets":
                client.ws_set_options(path=path)
            if proxy:
                import socks  # provided by the "pysocks" package
                p = urlparse(proxy)
                kind = socks.SOCKS5 if p.scheme.startswith("socks5") else socks.HTTP
                client.proxy_set(
                    proxy_type=kind,
                    proxy_addr=p.hostname,
                    proxy_port=p.port or (1080 if kind == socks.SOCKS5 else 8080),
                    proxy_username=unquote(p.username) if p.username else None,
                    proxy_password=unquote(p.password) if p.password else None,
                )
            client.on_connect = self._on_connect
            client.on_message = self._on_message

            self._connected.clear()
            self._connect_error = None
            try:
                client.connect(host, port, keepalive=30)
            except Exception as exc:  # OSError, socks errors, ...
                errors.append(f"{label} -> {exc}")
                continue

            client.loop_start()
            if not self._connected.wait(15):
                client.loop_stop()
                errors.append(f"{label} -> timed out")
                continue
            if self._connect_error:  # broker answered but refused us (auth etc.)
                client.loop_stop()
                raise FarmBotError(self._connect_error)

            self._client = client
            console.print(f"[dim]MQTT connected via {escape(label)}[/dim]")
            break
        else:
            raise FarmBotError("MQTT connection failed:\n  " + "\n  ".join(errors))

        # The status topic is retained, so the state arrives almost instantly.
        deadline = time.time() + 5
        while not self.state and time.time() < deadline:
            time.sleep(0.1)

    def disconnect(self) -> None:
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()

    def _topic(self, name: str) -> str:
        return f"bot/{self.device}/{name}"

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            self._connect_error = f"MQTT refused the connection: {reason_code}"
            self._connected.set()
            return
        for name in ("from_device", "status", "logs"):
            client.subscribe(self._topic(name))
        self._connected.set()

    def _on_message(self, client, userdata, msg):
        """Runs in the MQTT thread - keep it fast and never block here."""
        try:
            data = json.loads(msg.payload)
        except (ValueError, TypeError):
            return
        channel = msg.topic.rsplit("/", 1)[-1]

        if channel == "status":
            self.state = data
        elif channel == "logs":
            kind = data.get("type", "info")
            if self.show_logs and kind != "debug":
                style = {"error": "red", "warn": "yellow", "success": "green"}.get(kind, "cyan")
                text = escape(str(data.get("message", "")))
                # console.print always ends with a newline. While the prompt is
                # active, patch_stdout puts this line above the input field.
                console.print(f"[dim]bot ›[/dim] [{style}]{text}[/{style}]", highlight=False)
        elif channel == "from_device":
            if data.get("kind") in ("rpc_ok", "rpc_error"):
                label = data.get("args", {}).get("label")
                waiter = self._waiters.get(label)
                if waiter:
                    waiter["result"] = data
                    waiter["event"].set()

    # ---- RPC --------------------------------------------------------------

    def rpc(self, body: list[dict], timeout: float = 180.0) -> dict:
        """Send an rpc_request and block until rpc_ok / rpc_error arrives."""
        label = str(uuid.uuid4())
        waiter = {"event": threading.Event(), "result": None}
        self._waiters[label] = waiter
        payload = {
            "kind": "rpc_request",
            "args": {"label": label, "priority": RPC_PRIORITY},
            "body": body,
        }
        self._client.publish(self._topic("from_clients"), json.dumps(payload))
        try:
            if not waiter["event"].wait(timeout):
                raise FarmBotError("Timed out waiting for the bot to answer.")
        finally:
            self._waiters.pop(label, None)

        result = waiter["result"]
        if result["kind"] == "rpc_error":
            details = result.get("body") or [{}]
            message = details[0].get("args", {}).get("message", "unknown error")
            raise FarmBotError(f"Bot reported an error: {message}")
        return result

    # ---- State helpers ----------------------------------------------------

    @property
    def position(self) -> dict:
        return self.state.get("location_data", {}).get("position", {})

    @property
    def info(self) -> dict:
        return self.state.get("informational_settings", {})

    @property
    def pins(self) -> dict:
        return self.state.get("pins", {})


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def parse_numbers(args: list[str], low: int, high: int, usage: str) -> list[float]:
    """Parse between `low` and `high` numeric arguments or raise a usage error."""
    if not low <= len(args) <= high:
        raise FarmBotError(f"Usage: {usage}")
    try:
        return [float(a) for a in args]
    except ValueError:
        raise FarmBotError(f"Numbers expected. Usage: {usage}") from None


def clamp_speed(value: float) -> int:
    return int(max(1, min(100, value)))


def known_position(bot: FarmBot) -> tuple[float, float, float]:
    """Current (x, y, z) or an error if the bot has not reported a position yet."""
    pos = bot.position
    if not all(isinstance(pos.get(a), (int, float)) for a in "xyz"):
        raise FarmBotError("Current position is unknown (no status received yet).")
    return pos["x"], pos["y"], pos["z"]


def check_limits(x: float, y: float, z: float) -> bool:
    """Return True if the target is fine, otherwise ask the user to confirm."""
    problems = [
        f"{axis.upper()}={val:g} outside {LIMITS[axis][0]:g}..{LIMITS[axis][1]:g}"
        for axis, val in (("x", x), ("y", y), ("z", z))
        if not LIMITS[axis][0] <= val <= LIMITS[axis][1]
    ]
    if not problems:
        return True
    console.print("[yellow]Target is outside the configured limits:[/yellow] " + ", ".join(problems))
    return Confirm.ask("Move anyway?", default=False)


def fmt_position(pos: dict) -> str:
    def f(axis: str) -> str:
        v = pos.get(axis)
        return f"{v:.1f}" if isinstance(v, (int, float)) else "?"
    return f"X {f('x')}   Y {f('y')}   Z {f('z')}"


def fmt_duration(seconds) -> str:
    if not isinstance(seconds, (int, float)):
        return "?"
    seconds = int(seconds)
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    return (f"{d}d " if d else "") + f"{h:02d}:{m:02d}:{s:02d}"


def fmt_time(value) -> str:
    """The API returns timestamps either as epoch numbers or ISO strings."""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, str):
        return value[:19].replace("T", " ")
    return "?"


def run_rpc(bot: FarmBot, body: list[dict], message: str, timeout: float = 180.0) -> None:
    """RPC with a spinner while the bot works."""
    with console.status(f"[bold green]{escape(message)}[/bold green]"):
        bot.rpc(body, timeout=timeout)


def move_body(x: float, y: float, z: float, speed: float) -> list[dict]:
    return [node(
        "move_absolute",
        location=coordinate(x, y, z),
        offset=coordinate(0, 0, 0),
        speed=clamp_speed(speed),
    )]


def move_to(bot: FarmBot, x: float, y: float, z: float, speed: float) -> None:
    """Checked, direct move to an absolute position."""
    if not check_limits(x, y, z):
        console.print("[red]Move cancelled.[/red]")
        return
    run_rpc(bot, move_body(x, y, z, speed), f"Moving to X {x:g}  Y {y:g}  Z {z:g} ...")
    time.sleep(0.3)  # give the status message a moment to update
    console.print(f"[green]✔ Arrived:[/green] {fmt_position(bot.position)}")


def travel_to(bot: FarmBot, x: float, y: float, z: float, speed: float = DEFAULT_SPEED) -> None:
    """Safe move: lift to travel height, move over the target, then lower.

    This avoids dragging a tool through plants or the soil on the way.
    """
    if not check_limits(x, y, z):
        console.print("[red]Move cancelled.[/red]")
        return
    travel_z = max(SAFE_Z, z)
    cx, cy, cz = known_position(bot)
    with console.status("[bold green]Travelling ...[/bold green]"):
        if cz < travel_z:                                   # 1) lift up where we are
            bot.rpc(move_body(cx, cy, travel_z, speed))
        bot.rpc(move_body(x, y, travel_z, speed))           # 2) move over the target
        if z != travel_z:                                   # 3) lower down
            bot.rpc(move_body(x, y, z, speed))
    time.sleep(0.3)
    console.print(f"[green]✔ Arrived:[/green] {fmt_position(bot.position)}")


# ---- Pin / peripheral / sensor lookup -------------------------------------

def named_pins(bot: FarmBot) -> dict[int, dict]:
    """Map pin number -> {"label", "kind", "mode"} from peripherals and sensors."""
    out: dict[int, dict] = {}
    try:
        for p in bot.cached("peripherals"):
            if isinstance(p.get("pin"), int):
                out[p["pin"]] = {"label": p.get("label", ""), "kind": "peripheral", "mode": 0}
        for s in bot.cached("sensors"):
            if isinstance(s.get("pin"), int):
                out[s["pin"]] = {"label": s.get("label", ""), "kind": "sensor", "mode": s.get("mode", 0)}
    except FarmBotError:
        pass  # names are a convenience only - raw pin numbers still work
    return out


def resolve_pin(bot: FarmBot, token: str) -> int:
    """Accept a pin number or a (partial) peripheral/sensor name."""
    if token.isdigit():
        return int(token)
    query = token.lower()
    named = named_pins(bot)
    exact = [pin for pin, i in named.items() if i["label"].lower() == query]
    partial = [pin for pin, i in named.items() if query in i["label"].lower()]
    hits = exact or partial
    if len(hits) == 1:
        return hits[0]
    if not hits:
        raise FarmBotError(f"No pin, peripheral or sensor called '{token}'. Try 'pins'.")
    raise FarmBotError("Ambiguous name, matches: " + ", ".join(named[h]["label"] for h in hits))


def read_pin_value(bot: FarmBot, pin: int, mode: int = 0):
    """Ask the bot to read a pin, then fetch the value from the state tree."""
    bot.rpc([node("read_pin", pin_number=pin, label="cli", pin_mode=mode)], timeout=20)
    time.sleep(0.4)  # let the updated status message arrive
    return bot.pins.get(str(pin), {}).get("value")


def pin_label(bot: FarmBot, pin: int) -> str:
    return named_pins(bot).get(pin, {}).get("label") or f"pin {pin}"


# ---- Saved places ----------------------------------------------------------

def load_places() -> dict:
    try:
        return json.loads(PLACES_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_places(places: dict) -> None:
    try:
        PLACES_FILE.write_text(json.dumps(places, indent=2), encoding="utf-8")
    except OSError as exc:
        raise FarmBotError(f"Could not write {PLACES_FILE}: {exc}") from exc


# --------------------------------------------------------------------------- #
# Commands - every command receives (bot, args) and raises FarmBotError on problems
# --------------------------------------------------------------------------- #

# ---- Info ------------------------------------------------------------------

def cmd_status(bot: FarmBot, args: list[str]) -> None:
    info = bot.info
    table = Table(title=f"FarmBot {bot.device}", show_header=False, border_style="green")
    table.add_column(style="bold")
    table.add_column()
    table.add_row("Position", fmt_position(bot.position))
    table.add_row("Busy", "[yellow]yes[/yellow]" if info.get("busy") else "no")
    table.add_row("Locked (E-stop)", "[red]YES[/red]" if info.get("locked") else "no")
    table.add_row("Sync status", escape(str(info.get("sync_status", "?"))))
    table.add_row("FarmBot OS", escape(str(info.get("controller_version", "?"))))
    table.add_row("Firmware", escape(str(info.get("firmware_version", "?"))))
    table.add_row("Uptime", fmt_duration(info.get("uptime")))
    table.add_row("CPU / Memory / Disk",
                  f"{info.get('cpu_usage', '?')}% / {info.get('memory_usage', '?')} MB / {info.get('disk_usage', '?')}%")
    table.add_row("SoC temperature", f"{info.get('soc_temp', '?')} °C")
    table.add_row("WiFi level", str(info.get("wifi_level", "?")))
    console.print(table)


def cmd_pos(bot: FarmBot, args: list[str]) -> None:
    console.print(f"Position: [bold]{fmt_position(bot.position)}[/bold]")


def _position_panel(bot: FarmBot) -> Panel:
    busy = "[yellow]busy[/yellow]" if bot.info.get("busy") else "[green]idle[/green]"
    return Panel(f"{fmt_position(bot.position)}\n{busy}", title="Live position (Ctrl+C to stop)")


def cmd_watch(bot: FarmBot, args: list[str]) -> None:
    with Live(_position_panel(bot), console=console, refresh_per_second=4) as live:
        try:
            while True:
                live.update(_position_panel(bot))
                time.sleep(0.25)
        except KeyboardInterrupt:
            pass


def cmd_device(bot: FarmBot, args: list[str]) -> None:
    device = bot.api_get("device")
    table = Table(title="Device (web app)", show_header=False, border_style="green")
    table.add_column(style="bold")
    table.add_column()
    for key in ("name", "timezone", "fbos_version", "last_saw_api", "last_saw_mq", "mounted_tool_id", "ota_hour"):
        if key in device:
            table.add_row(key, escape(str(device[key])))
    console.print(table)


def cmd_version(bot: FarmBot, args: list[str]) -> None:
    try:
        cli_version = package_version("fbcli")
    except PackageNotFoundError:
        cli_version = "dev"
    console.print(
        f"fbcli [bold]{escape(cli_version)}[/bold]  |  "
        f"Python {escape(platform.python_version())}  |  "
        f"Device {escape(str(bot.device or '?'))}"
    )


def cmd_ping(bot: FarmBot, args: list[str]) -> None:
    started = time.perf_counter()
    bot.api_get("device")
    elapsed_ms = (time.perf_counter() - started) * 1000
    console.print(f"[green]✔ API reachable[/green] ({elapsed_ms:.0f} ms)")


def cmd_limits(bot: FarmBot, args: list[str]) -> None:
    table = Table(title="Soft limits (edit LIMITS in the script)", border_style="green")
    table.add_column("Axis")
    table.add_column("Min", justify="right")
    table.add_column("Max", justify="right")
    for axis, (lo, hi) in LIMITS.items():
        table.add_row(axis.upper(), f"{lo:g}", f"{hi:g}")
    console.print(table)
    console.print(f"Safe travel Z: {SAFE_Z:g}   Default speed: {DEFAULT_SPEED}%")


def cmd_logs(bot: FarmBot, args: list[str]) -> None:
    usage = "logs on|off|<count>"
    if not args:
        raise FarmBotError(f"Usage: {usage}")
    arg = args[0].lower()
    if arg in ("on", "off"):
        bot.show_logs = arg == "on"
        console.print(f"Live bot logs are now [bold]{'ON' if bot.show_logs else 'OFF'}[/bold].")
    elif arg.isdigit():
        logs = sorted(bot.api_get("logs"), key=lambda l: l.get("created_at") or 0, reverse=True)[: int(arg)]
        table = Table(title=f"Last {len(logs)} logs", border_style="green")
        for col in ("Time", "Type", "Message"):
            table.add_column(col)
        for log in reversed(logs):  # oldest first, newest at the bottom
            table.add_row(fmt_time(log.get("created_at")), escape(str(log.get("type", ""))),
                          escape(str(log.get("message", ""))))
        console.print(table)
    else:
        raise FarmBotError(f"Usage: {usage}")


def cmd_photos(bot: FarmBot, args: list[str]) -> None:
    count = int(args[0]) if args and args[0].isdigit() else 5
    images = sorted(bot.api_get("images"), key=lambda i: i.get("created_at") or "", reverse=True)[:count]
    table = Table(title=f"Last {len(images)} photos", border_style="green")
    for col in ("ID", "Taken", "X", "Y", "Z", "URL"):
        table.add_column(col, justify="right" if col in ("ID", "X", "Y", "Z") else "left")
    for img in images:
        meta = img.get("meta") or {}
        table.add_row(str(img.get("id")), fmt_time(img.get("created_at")),
                      str(meta.get("x", "?")), str(meta.get("y", "?")), str(meta.get("z", "?")),
                      escape(str(img.get("attachment_url", ""))))
    console.print(table)


# ---- Movement ----------------------------------------------------------------

def cmd_move(bot: FarmBot, args: list[str]) -> None:
    vals = parse_numbers(args, 3, 4, "move <x> <y> <z> [speed%]")
    speed = vals[3] if len(vals) == 4 else DEFAULT_SPEED
    move_to(bot, vals[0], vals[1], vals[2], speed)


def cmd_rel(bot: FarmBot, args: list[str]) -> None:
    dx, dy, dz, *rest = parse_numbers(args, 3, 4, "rel <dx> <dy> <dz> [speed%]")
    speed = rest[0] if rest else DEFAULT_SPEED
    pos = bot.position
    if all(isinstance(pos.get(a), (int, float)) for a in "xyz"):
        if not check_limits(pos["x"] + dx, pos["y"] + dy, pos["z"] + dz):
            console.print("[red]Move cancelled.[/red]")
            return
    run_rpc(bot, [node("move_relative", x=dx, y=dy, z=dz, speed=clamp_speed(speed))], "Moving relative ...")
    time.sleep(0.3)
    console.print(f"[green]✔ Now at:[/green] {fmt_position(bot.position)}")


def cmd_jog(bot: FarmBot, args: list[str]) -> None:
    """Shortcut for single-axis relative moves: 'x 100', 'z -20', 'jog y 50 60'."""
    usage = "jog <x|y|z> <distance> [speed%]   (or simply: x 100)"
    if len(args) not in (2, 3) or args[0].lower() not in ("x", "y", "z"):
        raise FarmBotError(f"Usage: {usage}")
    axis = args[0].lower()
    vals = parse_numbers(args[1:], 1, 2, usage)
    delta = {"x": 0.0, "y": 0.0, "z": 0.0}
    delta[axis] = vals[0]
    cmd_rel(bot, [str(delta["x"]), str(delta["y"]), str(delta["z"])] + ([str(vals[1])] if len(vals) == 2 else []))


def _directional_jog(bot: FarmBot, args: list[str], axis: str, sign: int, usage: str) -> None:
    vals = parse_numbers(args, 1, 2, usage)
    distance = abs(vals[0]) * sign
    cmd_jog(bot, [axis, str(distance)] + ([str(vals[1])] if len(vals) == 2 else []))


def cmd_left(bot: FarmBot, args: list[str]) -> None:
    _directional_jog(bot, args, "x", -1, "left <distance> [speed%]")


def cmd_right(bot: FarmBot, args: list[str]) -> None:
    _directional_jog(bot, args, "x", 1, "right <distance> [speed%]")


def cmd_forward(bot: FarmBot, args: list[str]) -> None:
    _directional_jog(bot, args, "y", 1, "forward <distance> [speed%]")


def cmd_back(bot: FarmBot, args: list[str]) -> None:
    _directional_jog(bot, args, "y", -1, "back <distance> [speed%]")


def cmd_up(bot: FarmBot, args: list[str]) -> None:
    _directional_jog(bot, args, "z", 1, "up <distance> [speed%]")


def cmd_down(bot: FarmBot, args: list[str]) -> None:
    _directional_jog(bot, args, "z", -1, "down <distance> [speed%]")


def cmd_lift(bot: FarmBot, args: list[str]) -> None:
    """Raise Z to the safe travel height without changing X/Y."""
    x, y, _ = known_position(bot)
    move_to(bot, x, y, SAFE_Z, DEFAULT_SPEED)


def cmd_home(bot: FarmBot, args: list[str]) -> None:
    axis = args[0].lower() if args else "all"
    if axis not in ("x", "y", "z", "all"):
        raise FarmBotError("Usage: home [x|y|z|all]")
    run_rpc(bot, [node("home", axis=axis, speed=DEFAULT_SPEED)], f"Homing {axis} ...")
    console.print(f"[green]✔ Home done:[/green] {fmt_position(bot.position)}")


def cmd_findhome(bot: FarmBot, args: list[str]) -> None:
    axis = args[0].lower() if args else "all"
    if axis not in ("x", "y", "z", "all"):
        raise FarmBotError("Usage: findhome [x|y|z|all]")
    run_rpc(bot, [node("find_home", axis=axis, speed=DEFAULT_SPEED)], f"Finding home ({axis}) ...", 300)
    console.print(f"[green]✔ Home found:[/green] {fmt_position(bot.position)}")


def cmd_goto(bot: FarmBot, args: list[str]) -> None:
    if len(args) != 1:
        raise FarmBotError("Usage: goto <point id>   (IDs from the 'points' command)")
    point = next((p for p in bot.api_get("points") if str(p["id"]) == args[0]), None)
    if not point:
        raise FarmBotError(f"Point {args[0]} not found.")
    console.print(f"Going to [bold]{escape(str(point.get('name', point['id'])))}[/bold] at safe Z {SAFE_Z:g}")
    travel_to(bot, point["x"], point["y"], point["z"])


def cmd_grid(bot: FarmBot, args: list[str]) -> None:
    """Visit every point of a rectangular grid in a snake pattern, optionally taking photos."""
    usage = "grid <x0> <y0> <x1> <y1> <step> [z] [photo]"
    photo = bool(args) and args[-1].lower() == "photo"
    vals = parse_numbers(args[:-1] if photo else args, 5, 6, usage)
    x0, y0, x1, y1, step = vals[:5]
    z = vals[5] if len(vals) == 6 else SAFE_Z
    if step <= 0:
        raise FarmBotError("Step must be greater than 0.")

    def frange(lo: float, hi: float) -> list[float]:
        count = int((hi - lo) / step + 1e-9)
        return [lo + i * step for i in range(count + 1)]

    xs, ys = frange(min(x0, x1), max(x0, x1)), frange(min(y0, y1), max(y0, y1))
    # Snake pattern: every second row runs backwards, so the bot never jumps back.
    points = [(x, y) for j, y in enumerate(ys) for x in (xs if j % 2 == 0 else reversed(xs))]
    if len(points) > MAX_GRID_POINTS:
        raise FarmBotError(f"{len(points)} points is more than the safety cap ({MAX_GRID_POINTS}).")
    if not (check_limits(xs[0], ys[0], z) and check_limits(xs[-1], ys[-1], z)):
        console.print("[red]Grid cancelled.[/red]")
        return
    if not Confirm.ask(f"Visit {len(points)} points{' and take a photo at each' if photo else ''}?", default=False):
        return

    progress = Progress(TextColumn("[bold green]Grid[/bold green]"), BarColumn(), TaskProgressColumn(),
                        TextColumn("{task.fields[where]}"), TimeElapsedColumn(), console=console)
    with progress:
        task = progress.add_task("grid", total=len(points), where="")
        for x, y in points:
            progress.update(task, where=f"X {x:g} Y {y:g}")
            bot.rpc(move_body(x, y, z, DEFAULT_SPEED))
            if photo:
                bot.rpc([node("take_photo")], timeout=120)
            progress.advance(task)
    console.print("[green]✔ Grid finished.[/green]")


# ---- Saved places --------------------------------------------------------------

def cmd_save(bot: FarmBot, args: list[str]) -> None:
    if not args:
        raise FarmBotError("Usage: save <name>   (stores the current position)")
    x, y, z = known_position(bot)
    places = load_places()
    places[" ".join(args)] = {"x": x, "y": y, "z": z}
    save_places(places)
    console.print(f"[green]✔ Saved '{escape(' '.join(args))}':[/green] X {x:.1f} Y {y:.1f} Z {z:.1f}")


def cmd_go(bot: FarmBot, args: list[str]) -> None:
    usage = "go <name> [direct]   (default: safe travel via lift / move / lower)"
    direct = bool(args) and args[-1].lower() == "direct"
    name = " ".join(args[:-1] if direct else args)
    if not name:
        raise FarmBotError(f"Usage: {usage}")
    spot = load_places().get(name)
    if not spot:
        raise FarmBotError(f"No saved place '{name}'. Use 'places' to list them.")
    if direct:
        move_to(bot, spot["x"], spot["y"], spot["z"], DEFAULT_SPEED)
    else:
        travel_to(bot, spot["x"], spot["y"], spot["z"])


def cmd_places(bot: FarmBot, args: list[str]) -> None:
    places = load_places()
    if not places:
        console.print("No saved places yet. Move the bot somewhere and use [bold]save <name>[/bold].")
        return
    table = Table(title="Saved places", border_style="green")
    for col in ("Name", "X", "Y", "Z"):
        table.add_column(col, justify="left" if col == "Name" else "right")
    for name, p in sorted(places.items()):
        table.add_row(escape(name), f"{p['x']:g}", f"{p['y']:g}", f"{p['z']:g}")
    console.print(table)


def cmd_forget(bot: FarmBot, args: list[str]) -> None:
    name = " ".join(args)
    places = load_places()
    if name not in places:
        raise FarmBotError(f"No saved place '{name}'.")
    del places[name]
    save_places(places)
    console.print(f"[green]✔ Forgot '{escape(name)}'.[/green]")


# ---- Camera --------------------------------------------------------------------

def cmd_photo(bot: FarmBot, args: list[str]) -> None:
    run_rpc(bot, [node("take_photo")], "Taking photo ...", 120)
    console.print("[green]✔ Photo taken (check the Photos panel in the web app).[/green]")


# ---- Pins, peripherals, sensors ----------------------------------------------------

def cmd_pins(bot: FarmBot, args: list[str]) -> None:
    """All pins the bot knows about, with names from peripherals/sensors if available."""
    named = named_pins(bot)
    pins = sorted({int(p) for p in bot.pins} | set(named))
    table = Table(title="Pins (values are what the bot last reported)", border_style="green")
    for col in ("Pin", "Name", "Kind", "Mode", "Value"):
        table.add_column(col, justify="right" if col in ("Pin", "Value") else "left")
    for pin in pins:
        state = bot.pins.get(str(pin), {})
        mode = {0: "digital", 1: "analog"}.get(state.get("mode"), "?")
        table.add_row(str(pin), escape(named.get(pin, {}).get("label", "")), named.get(pin, {}).get("kind", ""),
                      mode, str(state.get("value", "?")))
    console.print(table)
    console.print("[dim]'?' = not read yet. Use 'peripherals', 'sensors' or 'pin read <n>' to refresh.[/dim]")


def _print_pin_table(bot: FarmBot, title: str, items: list[dict], analog_aware: bool) -> None:
    table = Table(title=title, border_style="green")
    for col in ("Pin", "Name", "Mode", "Value"):
        table.add_column(col, justify="right" if col in ("Pin", "Value") else "left")
    for item in sorted(items, key=lambda i: i.get("pin") or 0):
        pin = item.get("pin")
        mode = item.get("mode", 0) if analog_aware else 0
        value = bot.pins.get(str(pin), {}).get("value", "?")
        if not analog_aware and value in (0, 1):  # peripherals are on/off switches
            value = "[green]ON[/green]" if value == 1 else "off"
        table.add_row(str(pin), escape(str(item.get("label", ""))), "analog" if mode == 1 else "digital", str(value))
    console.print(table)


def _refresh_and_show(bot: FarmBot, path: str, title: str, analog_aware: bool) -> None:
    """Read every pin of the given resource list, then show a table."""
    try:
        items = [i for i in bot.cached(path) if isinstance(i.get("pin"), int)]
    except FarmBotError as exc:
        raise FarmBotError(f"Could not load {path}: {exc}") from exc
    if not items:
        console.print(f"No {path} configured in the web app.")
        return
    with console.status(f"[bold green]Reading {len(items)} {path} ...[/bold green]"):
        for item in items:
            try:
                bot.rpc([node("read_pin", pin_number=item["pin"], label=str(item.get("label", "cli")),
                              pin_mode=item.get("mode", 0) if analog_aware else 0)], timeout=20)
            except FarmBotError as exc:
                console.print(f"[yellow]Could not read pin {item['pin']}: {escape(str(exc))}[/yellow]")
        time.sleep(0.5)
    _print_pin_table(bot, title.capitalize(), items, analog_aware)


def cmd_peripherals(bot: FarmBot, args: list[str]) -> None:
    _refresh_and_show(bot, "peripherals", "peripherals", analog_aware=False)


def cmd_sensors(bot: FarmBot, args: list[str]) -> None:
    _refresh_and_show(bot, "sensors", "sensors", analog_aware=True)


def cmd_pin(bot: FarmBot, args: list[str]) -> None:
    usage = "pin write <n|name> <value> [analog] | pin read <n|name> | pin toggle <n|name>"
    if len(args) < 2:
        raise FarmBotError(f"Usage: {usage}")
    action = args[0].lower()
    pin = resolve_pin(bot, args[1])

    if action == "write":
        if len(args) < 3:
            raise FarmBotError(f"Usage: {usage}")
        try:
            value = int(float(args[2]))
        except ValueError:
            raise FarmBotError(f"Value must be a number. Usage: {usage}") from None
        mode = 1 if len(args) > 3 and args[3].lower().startswith("a") else 0
        run_rpc(bot, [node("write_pin", pin_number=pin, pin_value=value, pin_mode=mode)],
                f"Writing pin {pin} = {value} ...", 30)
        console.print(f"[green]✔ Pin {pin} set to {value}[/green]")
    elif action == "read":
        with console.status(f"[bold green]Reading pin {pin} ...[/bold green]"):
            value = read_pin_value(bot, pin)
        console.print(f"Pin {pin} ({escape(pin_label(bot, pin))}) = [bold]{value if value is not None else '?'}[/bold]")
    elif action == "toggle":
        run_rpc(bot, [node("toggle_pin", pin_number=pin)], f"Toggling pin {pin} ...", 30)
        console.print(f"[green]✔ Pin {pin} toggled[/green]")
    else:
        raise FarmBotError(f"Usage: {usage}")


def _switch(bot: FarmBot, args: list[str], value: int) -> None:
    """Shared implementation of 'on' and 'off'."""
    if not args:
        raise FarmBotError(f"Usage: {'on' if value else 'off'} <peripheral name | pin>")
    pin = resolve_pin(bot, " ".join(args))
    run_rpc(bot, [node("write_pin", pin_number=pin, pin_value=value, pin_mode=0)],
            f"Switching {pin_label(bot, pin)} {'on' if value else 'off'} ...", 30)
    state = "[green]ON[/green]" if value else "off"
    console.print(f"✔ {escape(pin_label(bot, pin))} (pin {pin}) is now {state}")


def cmd_on(bot: FarmBot, args: list[str]) -> None:
    _switch(bot, args, 1)


def cmd_off(bot: FarmBot, args: list[str]) -> None:
    _switch(bot, args, 0)


def cmd_toggle(bot: FarmBot, args: list[str]) -> None:
    if not args:
        raise FarmBotError("Usage: toggle <peripheral name | pin>")
    pin = resolve_pin(bot, " ".join(args))
    run_rpc(bot, [node("toggle_pin", pin_number=pin)], f"Toggling {pin_label(bot, pin)} ...", 30)
    console.print(f"[green]✔ {escape(pin_label(bot, pin))} (pin {pin}) toggled[/green]")


def cmd_water(bot: FarmBot, args: list[str]) -> None:
    usage = "water on | water off | water <seconds>"
    if len(args) != 1:
        raise FarmBotError(f"Usage: {usage}")
    what = args[0].lower()

    def valve(state: int) -> None:
        bot.rpc([node("write_pin", pin_number=WATER_PIN, pin_value=state, pin_mode=0)], timeout=30)

    if what == "on":
        valve(1)
        console.print("[cyan]Water valve ON (remember to switch it off!)[/cyan]")
    elif what == "off":
        valve(0)
        console.print("[green]Water valve OFF[/green]")
    else:
        try:
            seconds = float(what)
        except ValueError:
            raise FarmBotError(f"Usage: {usage}") from None
        valve(1)
        try:
            with console.status(f"[cyan]Watering for {seconds:g} s ...[/cyan]"):
                time.sleep(seconds)
        finally:
            valve(0)  # always close the valve, even on Ctrl+C
        console.print("[green]✔ Watering finished, valve closed.[/green]")


# ---- Sequences, points, tools -------------------------------------------------------

def cmd_sequences(bot: FarmBot, args: list[str]) -> None:
    table = Table(title="Sequences", border_style="green")
    table.add_column("ID", justify="right")
    table.add_column("Name")
    for seq in sorted(bot.api_get("sequences"), key=lambda s: s.get("name", "").lower()):
        table.add_row(str(seq["id"]), escape(seq.get("name", "")))
    console.print(table)


def cmd_run(bot: FarmBot, args: list[str]) -> None:
    if not args:
        raise FarmBotError('Usage: run <sequence id | "sequence name">')
    query = " ".join(args)
    sequences = bot.api_get("sequences")
    match = next((s for s in sequences if str(s["id"]) == query), None) \
        or next((s for s in sequences if s.get("name", "").lower() == query.lower()), None)
    if not match:
        raise FarmBotError(f"No sequence matches '{query}'. Use 'sequences' to list them.")
    run_rpc(bot, [node("execute", sequence_id=match["id"])], f"Running '{match['name']}' ...", 1800)
    console.print(f"[green]✔ Sequence '{escape(match['name'])}' finished.[/green]")


def cmd_points(bot: FarmBot, args: list[str]) -> None:
    wanted = args[0].lower() if args else "all"  # all | plants | points | slots
    kinds = {"plants": "Plant", "points": "GenericPointer", "slots": "ToolSlot"}
    points = bot.api_get("points")
    if wanted in kinds:
        points = [p for p in points if p.get("pointer_type") == kinds[wanted]]
    table = Table(title=f"Points ({wanted})", border_style="green")
    for col in ("ID", "Type", "Name", "X", "Y", "Z"):
        table.add_column(col, justify="right" if col in ("ID", "X", "Y", "Z") else "left")
    for p in sorted(points, key=lambda p: (p.get("pointer_type", ""), p.get("name", ""))):
        table.add_row(str(p["id"]), p.get("pointer_type", ""), escape(p.get("name", "")),
                      f"{p['x']:g}", f"{p['y']:g}", f"{p['z']:g}")
    console.print(table)


def cmd_tools(bot: FarmBot, args: list[str]) -> None:
    table = Table(title="Tools", border_style="green")
    table.add_column("ID", justify="right")
    table.add_column("Name")
    for tool in bot.api_get("tools"):
        table.add_row(str(tool["id"]), escape(tool.get("name", "")))
    console.print(table)


def cmd_reload(bot: FarmBot, args: list[str]) -> None:
    bot.cache.clear()
    console.print("[green]✔ Cached peripherals/sensors cleared; they will be reloaded on next use.[/green]")


# ---- Safety & system -----------------------------------------------------------------

def cmd_lock(bot: FarmBot, args: list[str]) -> None:
    """Emergency stop - do not wait long, the bot stops immediately."""
    try:
        bot.rpc([node("emergency_lock")], timeout=10)
    except FarmBotError:
        pass
    console.print("[bold red]⛔ Emergency stop sent. Bot is LOCKED. Use 'unlock' to continue.[/bold red]")


def cmd_unlock(bot: FarmBot, args: list[str]) -> None:
    run_rpc(bot, [node("emergency_unlock")], "Unlocking ...", 15)
    console.print("[green]✔ Bot unlocked.[/green]")


def cmd_sync(bot: FarmBot, args: list[str]) -> None:
    run_rpc(bot, [node("sync")], "Syncing ...", 60)
    console.print("[green]✔ Sync requested.[/green]")


def cmd_update(bot: FarmBot, args: list[str]) -> None:
    run_rpc(bot, [node("check_updates", package="farmbot_os")], "Checking for FarmBot OS updates ...", 30)
    console.print("[green]✔ Update check requested (the bot logs the result).[/green]")


def cmd_reboot(bot: FarmBot, args: list[str]) -> None:
    if Confirm.ask("Reboot FarmBot OS?", default=False):
        try:
            bot.rpc([node("reboot", package="farmbot_os")], timeout=10)
        except FarmBotError:
            pass  # the bot may go offline before it answers
        console.print("[yellow]Reboot requested.[/yellow]")


def cmd_shutdown(bot: FarmBot, args: list[str]) -> None:
    if Confirm.ask("Shut FarmBot down? You will need to power-cycle it to turn it on again.", default=False):
        try:
            bot.rpc([node("power_off")], timeout=10)
        except FarmBotError:
            pass
        console.print("[yellow]Shutdown requested.[/yellow]")


# ---- Scripting helpers ----------------------------------------------------------------

def cmd_wait(bot: FarmBot, args: list[str]) -> None:
    (seconds,) = parse_numbers(args, 1, 1, "wait <seconds>")
    with console.status(f"[bold green]Waiting {seconds:g} s ...[/bold green]"):
        time.sleep(seconds)


def cmd_batch(bot: FarmBot, args: list[str]) -> None:
    """Run commands from a text file (one per line, '#' starts a comment). Stops on the first error."""
    if len(args) != 1:
        raise FarmBotError("Usage: batch <file>")
    try:
        lines = Path(args[0]).expanduser().read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise FarmBotError(f"Cannot read {args[0]}: {exc}") from exc
    for number, line in enumerate(lines, 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.split()[0].lower() == "batch":
            raise FarmBotError(f"Line {number}: nested 'batch' is not allowed.")
        console.print(f"[dim]{number}:[/dim] [bold]{escape(line)}[/bold]")
        try:
            execute(bot, line)
        except FarmBotError as exc:
            raise FarmBotError(f"Batch stopped at line {number}: {exc}") from exc
    console.print("[green]✔ Batch finished.[/green]")

def cmd_clear(bot: FarmBot, args: list[str]) -> None:
    """Clear the terminal screen (the connection and all state stay untouched)."""
    console.clear()
    # Also wipe the scrollback buffer on terminals that support it, then show a short banner.
    if console.is_terminal:
        console.file.write("\033[3J")
        console.file.flush()
    console.print(f"[dim]FarmBot {escape(str(bot.device))} - type 'help' for commands[/dim]")

def cmd_help(bot: FarmBot, args: list[str]) -> None:
    table = Table(title="Commands", border_style="green")
    table.add_column("Command", style="bold cyan")
    table.add_column("Description")
    last_group = None
    for name, (_, usage, description, group) in COMMANDS.items():
        if group != last_group and last_group is not None:
            table.add_section()  # visual gap between command groups
        last_group = group
        table.add_row(escape(f"{name} {usage}".strip()), escape(description))
    table.add_section()
    table.add_row("x|y|z <distance>", "Shortcut for jog, e.g. 'x 100' or 'z -20'")
    table.add_row("exit", "Leave the shell")
    console.print(table)


# name -> (function, usage, description, group)
COMMANDS: dict = {
    # --- Info
    "status":      (cmd_status,      "", "Device status (position, busy, versions, resources)", "info"),
    "pos":         (cmd_pos,         "", "Print the current position once", "info"),
    "watch":       (cmd_watch,       "", "Live position display", "info"),
    "device":      (cmd_device,      "", "Device info from the web app", "info"),
    "version":     (cmd_version,     "", "Show CLI and Python version", "info"),
    "ping":        (cmd_ping,        "", "Check API reachability and response time", "info"),
    "limits":      (cmd_limits,      "", "Show configured soft limits", "info"),
    "logs":        (cmd_logs,        "on|off|<count>", "Live logs on/off, or show the last <count> logs", "info"),
    "photos":      (cmd_photos,      "[count]", "List the latest photos with position and URL", "info"),
    # --- Movement
    "move":        (cmd_move,        "<x> <y> <z> [speed%]", "Move to absolute coordinates", "move"),
    "rel":         (cmd_rel,         "<dx> <dy> <dz> [speed%]", "Move relative to current position", "move"),
    "jog":         (cmd_jog,         "<x|y|z> <dist> [speed%]", "Move a single axis relative", "move"),
    "left":        (cmd_left,        "<distance> [speed%]", "Jog X negative", "move"),
    "right":       (cmd_right,       "<distance> [speed%]", "Jog X positive", "move"),
    "forward":     (cmd_forward,     "<distance> [speed%]", "Jog Y positive", "move"),
    "back":        (cmd_back,        "<distance> [speed%]", "Jog Y negative", "move"),
    "up":          (cmd_up,          "<distance> [speed%]", "Jog Z positive", "move"),
    "down":        (cmd_down,        "<distance> [speed%]", "Jog Z negative", "move"),
    "lift":        (cmd_lift,        "", "Raise Z to the safe travel height", "move"),
    "home":        (cmd_home,        "[x|y|z|all]", "Go to home (0) position", "move"),
    "findhome":    (cmd_findhome,    "[x|y|z|all]", "Find home using endstops/encoders", "move"),
    "goto":        (cmd_goto,        "<point id>", "Safe travel to a saved point/plant", "move"),
    "grid":        (cmd_grid,        "<x0> <y0> <x1> <y1> <step> [z] [photo]", "Visit a grid in snake order, optionally photo each", "move"),
    # --- Places
    "save":        (cmd_save,        "<name>", "Save the current position locally", "places"),
    "go":          (cmd_go,          "<name> [direct]", "Safe travel to a saved place", "places"),
    "places":      (cmd_places,      "", "List saved places", "places"),
    "forget":      (cmd_forget,      "<name>", "Delete a saved place", "places"),
    # --- Camera
    "photo":       (cmd_photo,       "", "Take a photo with the camera", "camera"),
    # --- Pins & peripherals
    "pins":        (cmd_pins,        "", "List all known pins with names and last values", "pins"),
    "peripherals": (cmd_peripherals, "", "Read and list all peripherals (lights, vacuum, ...)", "pins"),
    "sensors":     (cmd_sensors,     "", "Read and list all sensors", "pins"),
    "on":          (cmd_on,          "<name|pin>", "Switch a peripheral/pin on", "pins"),
    "off":         (cmd_off,         "<name|pin>", "Switch a peripheral/pin off", "pins"),
    "toggle":      (cmd_toggle,      "<name|pin>", "Toggle a peripheral/pin", "pins"),
    "pin":         (cmd_pin,         "write|read|toggle <n|name> [value]", "Low level pin control", "pins"),
    "water":       (cmd_water,       "on|off|<seconds>", f"Water valve on pin {WATER_PIN}", "pins"),
    # --- Sequences & data
    "sequences":   (cmd_sequences,   "", "List sequences", "data"),
    "run":         (cmd_run,         '<id|"name">', "Execute a sequence", "data"),
    "points":      (cmd_points,      "[all|plants|points|slots]", "List points from the web app", "data"),
    "tools":       (cmd_tools,       "", "List tools", "data"),
    "reload":      (cmd_reload,      "", "Forget cached peripherals/sensors", "data"),
    # --- Safety & system
    "lock":        (cmd_lock,        "", "EMERGENCY STOP", "system"),
    "unlock":      (cmd_unlock,      "", "Release emergency stop", "system"),
    "sync":        (cmd_sync,        "", "Sync bot with the web app", "system"),
    "update":      (cmd_update,      "", "Check for a FarmBot OS update", "system"),
    "reboot":      (cmd_reboot,      "", "Reboot FarmBot OS", "system"),
    "shutdown":    (cmd_shutdown,    "", "Power off FarmBot OS", "system"),
    # --- Scripting
    "wait":        (cmd_wait,        "<seconds>", "Pause (useful in batch files)", "script"),
    "batch":       (cmd_batch,       "<file>", "Run commands from a text file", "script"),
    "clear":       (cmd_clear,       "", "Clear the screen (alias: cls)", "script"),
    "help":        (cmd_help,        "", "Show this help", "script"),
}
# Short aliases
ALIASES = {"stop": "lock", "estop": "lock", "where": "pos", "seq": "sequences", "plant": "points", "cls": "clear"}


def execute(bot: FarmBot, line: str) -> bool:
    """Run one command line. Raises FarmBotError; returns False when the shell should exit."""
    try:
        tokens = shlex.split(line)
    except ValueError as exc:
        raise FarmBotError(str(exc)) from exc
    if not tokens:
        return True
    name, args = tokens[0].lower(), tokens[1:]
    if name in ("exit", "quit", "q"):
        return False
    if name in ("x", "y", "z"):          # 'x 100' == 'jog x 100'
        name, args = "jog", [name] + args
    name = ALIASES.get(name, name)
    if name == "plants":
        name, args = "points", ["plants"]
    entry = COMMANDS.get(name)
    if not entry:
        raise FarmBotError(f"Unknown command '{name}'. Type 'help'.")
    entry[0](bot, args)
    return True


def run_command(bot: FarmBot, line: str) -> bool:
    """execute() with friendly error handling for the interactive shell."""
    try:
        return execute(bot, line)
    except FarmBotError as exc:
        # escape(): usage texts contain brackets like [speed%] which Rich would treat as markup
        console.print(f"[red]✘ {escape(str(exc))}[/red]")
    except KeyboardInterrupt:
        console.print("[yellow]Interrupted. The bot may still be moving - use 'lock' to stop it.[/yellow]")
    return True


# --------------------------------------------------------------------------- #
# Interactive shell
# --------------------------------------------------------------------------- #

def toolbar_text(bot: FarmBot) -> str:
    """Status line below the prompt, refreshed twice a second."""
    parts = [fmt_position(bot.position), "BUSY" if bot.info.get("busy") else "idle"]
    if bot.info.get("locked"):
        parts.append("LOCKED (E-STOP)")
    return " " + "   |   ".join(parts) + " "


def interactive(bot: FarmBot) -> None:
    words = sorted(set(COMMANDS) | set(ALIASES) | {"exit", "x", "y", "z"})
    session = PromptSession(
        history=FileHistory(str(HISTORY_FILE)),           # arrow up/down remembers old commands
        completer=WordCompleter(words, ignore_case=True),  # Tab completes command names
        bottom_toolbar=lambda: toolbar_text(bot),
        refresh_interval=0.5,
    )
    while True:
        try:
            # patch_stdout: anything printed by the MQTT thread while we wait for input
            # appears above the prompt instead of inside the input field.
            with patch_stdout(raw=True):
                line = session.prompt(HTML("<ansigreen><b>farmbot</b></ansigreen>&gt; "))
        except (EOFError, KeyboardInterrupt):
            console.print()
            break
        if not run_command(bot, line):
            break


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> int:
    server = os.environ.get("FARMBOT_SERVER", DEFAULT_SERVER)
    email = os.environ.get("FARMBOT_EMAIL") or console.input("FarmBot e-mail: ").strip()
    password = os.environ.get("FARMBOT_PASSWORD") or getpass.getpass("FarmBot password: ")

    bot = FarmBot(server)
    try:
        with console.status("[bold green]Logging in and connecting ...[/bold green]"):
            bot.login(email, password)
            bot.connect()
    except FarmBotError as exc:
        console.print(f"[red]✘ {escape(str(exc))}[/red]")
        return 1

    console.print(Panel.fit(
        f"Connected to [bold]{escape(str(bot.device))}[/bold]\n"
        f"Position: {fmt_position(bot.position)}\nType [bold]help[/bold] for commands, Tab completes.",
        title="🌱 FarmBot CLI", border_style="green"))

    try:
        if len(sys.argv) > 1:
            # One-shot mode: farmbot_cli.py move 100 200 0
            run_command(bot, shlex.join(sys.argv[1:]))
        else:
            interactive(bot)
    finally:
        bot.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
