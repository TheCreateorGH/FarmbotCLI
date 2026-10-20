from importlib import import_module


def main() -> int:
    return import_module("main").main()
