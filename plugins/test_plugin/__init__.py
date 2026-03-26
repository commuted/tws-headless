"""
plugins/test_plugin — canonical test plugin for the TWS headless test suite.

Importable as a package (plugins.test_plugin) or loadable as a bare file by
PluginLoader.load_from_file(), because it uses absolute imports only.
"""

from plugins.test_plugin.plugin import TestPlugin

__all__ = ["TestPlugin"]
