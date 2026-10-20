import unittest
from unittest.mock import patch

import main as cli


class DummyBot:
    def __init__(self):
        self.device = "device_123"
        self.api_calls = []

    def api_get(self, path: str):
        self.api_calls.append(path)
        if path == "points":
            return [{"id": 7, "name": "Plant A", "x": 11, "y": 22, "z": -33}]
        if path == "device":
            return {"name": "Demo Farm"}
        return []


class CliCommandTests(unittest.TestCase):
    def test_goto_uses_point_z_coordinate(self):
        bot = DummyBot()
        with patch.object(cli, "travel_to") as travel_to:
            cli.execute(bot, "goto 7")
        travel_to.assert_called_once_with(bot, 11, 22, -33)

    def test_ping_command_calls_device_api(self):
        bot = DummyBot()
        cli.execute(bot, "ping")
        self.assertIn("device", bot.api_calls)

    def test_version_command_is_registered(self):
        bot = DummyBot()
        should_continue = cli.execute(bot, "version")
        self.assertTrue(should_continue)

    def test_directional_shortcuts_delegate_to_jog(self):
        bot = DummyBot()
        cases = [
            ("left 10", ["x", "-10.0"]),
            ("right 10 80", ["x", "10.0", "80.0"]),
            ("forward 12", ["y", "12.0"]),
            ("back 7", ["y", "-7.0"]),
            ("up 5", ["z", "5.0"]),
            ("down 3 20", ["z", "-3.0", "20.0"]),
        ]
        for command, expected_args in cases:
            with self.subTest(command=command):
                with patch.object(cli, "cmd_jog") as cmd_jog:
                    cli.execute(bot, command)
                cmd_jog.assert_called_once_with(bot, expected_args)


if __name__ == "__main__":
    unittest.main()
