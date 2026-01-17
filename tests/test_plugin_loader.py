"""
Tests for plugin_loader.py - Dynamic plugin loading utilities
"""

import pytest
import tempfile
import os
from pathlib import Path

from ib.plugin_loader import (
    PluginLoader,
    load_plugin,
)
from ib.plugins.base import PluginBase


# Sample plugin code for testing
VALID_PLUGIN_CODE = '''
"""Test plugin"""
from ib.plugins.base import PluginBase, TradeSignal

class TestPlugin(PluginBase):
    """A valid test plugin"""

    def __init__(self, **kwargs):
        super().__init__("test_plugin", **kwargs)

    @property
    def description(self) -> str:
        return "A test plugin"

    def start(self) -> bool:
        return True

    def stop(self) -> bool:
        return True

    def freeze(self) -> bool:
        return True

    def resume(self) -> bool:
        return True

    def handle_request(self, request_type: str, payload: dict) -> dict:
        return {"success": True}

    def calculate_signals(self, market_data: dict) -> list:
        return []
'''

INVALID_PLUGIN_CODE = '''
"""Not a valid plugin - no PluginBase subclass"""

class NotAPlugin:
    def __init__(self):
        pass
'''


class TestPluginLoader:
    """Tests for PluginLoader class"""

    def test_create_loader(self):
        """Test creating a plugin loader"""
        loader = PluginLoader()
        assert loader is not None

    def test_load_from_file(self):
        """Test loading plugin from file"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False
        ) as f:
            f.write(VALID_PLUGIN_CODE)
            f.flush()

            try:
                loader = PluginLoader()
                plugin = loader.load_from_file(f.name)

                assert plugin is not None
                assert plugin.name == "test_plugin"
                assert isinstance(plugin, PluginBase)
            finally:
                os.unlink(f.name)

    def test_load_nonexistent_file(self):
        """Test loading from nonexistent file"""
        loader = PluginLoader()

        plugin = loader.load_from_file("/nonexistent/path/plugin.py")

        assert plugin is None

    def test_load_invalid_plugin(self):
        """Test loading file with no PluginBase subclass"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False
        ) as f:
            f.write(INVALID_PLUGIN_CODE)
            f.flush()

            try:
                loader = PluginLoader()
                plugin = loader.load_from_file(f.name)

                assert plugin is None
            finally:
                os.unlink(f.name)

    def test_load_empty_file(self):
        """Test loading empty file"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False
        ) as f:
            f.write("")
            f.flush()

            try:
                loader = PluginLoader()
                plugin = loader.load_from_file(f.name)

                assert plugin is None
            finally:
                os.unlink(f.name)


class TestPluginLoaderDirectory:
    """Tests for loading plugins from directory"""

    def test_load_from_directory(self):
        """Test loading all plugins from directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple plugin files
            for i in range(3):
                plugin_code = f'''
from ib.plugins.base import PluginBase, TradeSignal

class Plugin{i}(PluginBase):
    def __init__(self, **kwargs):
        super().__init__("plugin_{i}", **kwargs)

    @property
    def description(self):
        return "Plugin {i}"

    def start(self):
        return True

    def stop(self):
        return True

    def freeze(self):
        return True

    def resume(self):
        return True

    def handle_request(self, request_type, payload):
        return {{"success": True}}

    def calculate_signals(self, market_data):
        return []
'''
                with open(os.path.join(tmpdir, f"plugin_{i}.py"), "w") as f:
                    f.write(plugin_code)

            loader = PluginLoader()
            plugins = loader.load_from_directory(tmpdir)

            assert len(plugins) == 3

    def test_load_from_empty_directory(self):
        """Test loading from empty directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = PluginLoader()
            plugins = loader.load_from_directory(tmpdir)

            assert plugins == []

    def test_load_from_nonexistent_directory(self):
        """Test loading from nonexistent directory"""
        loader = PluginLoader()

        plugins = loader.load_from_directory("/nonexistent/path")

        assert plugins == []


class TestConvenienceFunctions:
    """Tests for module-level convenience functions"""

    def test_load_plugin_function(self):
        """Test load_plugin convenience function"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False
        ) as f:
            f.write(VALID_PLUGIN_CODE)
            f.flush()

            try:
                plugin = load_plugin(f.name)

                assert plugin is not None
                assert plugin.name == "test_plugin"
            finally:
                os.unlink(f.name)


class TestPluginLoaderWithMessageBus:
    """Tests for loading plugins with MessageBus"""

    def test_load_with_message_bus(self):
        """Test loading plugin with message_bus kwarg"""
        from ib.message_bus import MessageBus

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False
        ) as f:
            f.write(VALID_PLUGIN_CODE)
            f.flush()

            try:
                bus = MessageBus()
                loader = PluginLoader()
                plugin = loader.load_from_file(f.name, message_bus=bus)

                assert plugin is not None
                assert plugin._message_bus is bus
            finally:
                os.unlink(f.name)
