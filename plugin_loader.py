"""
plugin_loader.py - Dynamic plugin loading utilities

Provides utilities for loading plugins from Python files at runtime.
"""

import importlib.util
import inspect
import logging
import sys
from pathlib import Path
from typing import Optional, Type, List, Dict, Any

from .plugins.base import PluginBase

logger = logging.getLogger(__name__)


class PluginLoader:
    """
    Dynamic plugin loader.

    Loads PluginBase subclasses from Python files at runtime.

    Usage:
        loader = PluginLoader()

        # Load from file path
        plugin = loader.load_from_file("/path/to/my_plugin.py")

        # Load from directory (all plugins in directory)
        plugins = loader.load_from_directory("/path/to/plugins/")

        # Discover plugins in a package
        plugins = loader.discover("plugins.custom")
    """

    def __init__(self):
        """Initialize the plugin loader"""
        self._loaded_modules: Dict[str, Any] = {}

    def load_from_file(
        self,
        file_path: str,
        **kwargs,
    ) -> Optional[PluginBase]:
        """
        Load a plugin from a Python file.

        The file must contain exactly one class that inherits from PluginBase.
        If multiple plugin classes are found, the first one is used.

        Args:
            file_path: Path to the Python file
            **kwargs: Arguments to pass to the plugin constructor

        Returns:
            Plugin instance or None if loading failed
        """
        path = Path(file_path).resolve()

        if not path.exists():
            logger.error(f"Plugin file not found: {path}")
            return None

        if not path.suffix == ".py":
            logger.error(f"Plugin file must be a Python file: {path}")
            return None

        try:
            # Generate a unique module name
            module_name = f"plugin_{path.stem}_{id(path)}"

            # Load the module
            spec = importlib.util.spec_from_file_location(module_name, path)
            if not spec or not spec.loader:
                logger.error(f"Failed to create module spec for {path}")
                return None

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            # Find PluginBase subclasses
            plugin_classes = self._find_plugin_classes(module)

            if not plugin_classes:
                logger.error(f"No PluginBase subclass found in {path}")
                del sys.modules[module_name]
                return None

            if len(plugin_classes) > 1:
                logger.warning(
                    f"Multiple PluginBase subclasses found in {path}, "
                    f"using first one: {plugin_classes[0].__name__}"
                )

            # Instantiate the plugin
            plugin_class = plugin_classes[0]
            plugin = plugin_class(**kwargs)

            # Track the loaded module
            self._loaded_modules[plugin.name] = module

            logger.info(f"Loaded plugin '{plugin.name}' from {path}")
            return plugin

        except Exception as e:
            logger.error(f"Error loading plugin from {path}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None

    def load_from_directory(
        self,
        directory: str,
        recursive: bool = False,
        **kwargs,
    ) -> List[PluginBase]:
        """
        Load all plugins from a directory.

        Searches for Python files containing PluginBase subclasses.

        Args:
            directory: Path to the directory
            recursive: Whether to search subdirectories
            **kwargs: Arguments to pass to plugin constructors

        Returns:
            List of loaded plugin instances
        """
        dir_path = Path(directory).resolve()

        if not dir_path.exists():
            logger.error(f"Plugin directory not found: {dir_path}")
            return []

        if not dir_path.is_dir():
            logger.error(f"Path is not a directory: {dir_path}")
            return []

        plugins = []
        pattern = "**/*.py" if recursive else "*.py"

        for py_file in dir_path.glob(pattern):
            # Skip __init__.py and other special files
            if py_file.name.startswith("_"):
                continue

            try:
                plugin = self.load_from_file(str(py_file), **kwargs)
                if plugin:
                    plugins.append(plugin)
            except Exception as e:
                logger.error(f"Error loading plugin from {py_file}: {e}")

        logger.info(f"Loaded {len(plugins)} plugins from {dir_path}")
        return plugins

    def discover(
        self,
        package_name: str,
        **kwargs,
    ) -> List[PluginBase]:
        """
        Discover plugins in an installed package.

        Searches the package for PluginBase subclasses and instantiates them.

        Args:
            package_name: Name of the package (e.g., "plugins.custom")
            **kwargs: Arguments to pass to plugin constructors

        Returns:
            List of discovered plugin instances
        """
        try:
            package = importlib.import_module(package_name)
        except ImportError as e:
            logger.error(f"Failed to import package '{package_name}': {e}")
            return []

        if not hasattr(package, "__path__"):
            logger.error(f"'{package_name}' is not a package")
            return []

        plugins = []

        # Get package directory
        package_path = Path(package.__path__[0])

        for py_file in package_path.glob("*.py"):
            if py_file.name.startswith("_"):
                continue

            module_name = f"{package_name}.{py_file.stem}"

            try:
                module = importlib.import_module(module_name)
                plugin_classes = self._find_plugin_classes(module)

                for plugin_class in plugin_classes:
                    try:
                        plugin = plugin_class(**kwargs)
                        plugins.append(plugin)
                        logger.info(f"Discovered plugin '{plugin.name}' in {module_name}")
                    except Exception as e:
                        logger.error(f"Error instantiating {plugin_class.__name__}: {e}")

            except ImportError as e:
                logger.error(f"Error importing {module_name}: {e}")

        return plugins

    def _find_plugin_classes(self, module) -> List[Type[PluginBase]]:
        """
        Find all PluginBase subclasses in a module.

        Args:
            module: The module to search

        Returns:
            List of PluginBase subclasses (not including PluginBase itself)
        """
        plugin_classes = []

        for name, obj in inspect.getmembers(module):
            if (inspect.isclass(obj) and
                issubclass(obj, PluginBase) and
                obj is not PluginBase and
                not inspect.isabstract(obj)):
                plugin_classes.append(obj)

        return plugin_classes

    def unload(self, plugin_name: str) -> bool:
        """
        Unload a dynamically loaded plugin module.

        Args:
            plugin_name: Name of the plugin to unload

        Returns:
            True if unloaded successfully
        """
        if plugin_name not in self._loaded_modules:
            logger.warning(f"Plugin '{plugin_name}' not found in loaded modules")
            return False

        module = self._loaded_modules[plugin_name]
        module_name = module.__name__

        # Remove from sys.modules
        if module_name in sys.modules:
            del sys.modules[module_name]

        del self._loaded_modules[plugin_name]
        logger.info(f"Unloaded plugin module for '{plugin_name}'")
        return True

    def get_loaded_modules(self) -> List[str]:
        """Get list of loaded plugin names"""
        return list(self._loaded_modules.keys())

    def validate_plugin_file(self, file_path: str) -> Dict[str, Any]:
        """
        Validate a plugin file without loading it.

        Checks that the file:
        - Exists and is a Python file
        - Contains at least one PluginBase subclass
        - The class can be instantiated (basic check)

        Args:
            file_path: Path to the Python file

        Returns:
            Validation result dict with 'valid', 'message', and 'plugin_classes' keys
        """
        path = Path(file_path).resolve()

        if not path.exists():
            return {
                "valid": False,
                "message": f"File not found: {path}",
                "plugin_classes": [],
            }

        if not path.suffix == ".py":
            return {
                "valid": False,
                "message": "File must be a Python file (.py)",
                "plugin_classes": [],
            }

        try:
            # Read and compile the file
            with open(path) as f:
                source = f.read()

            compile(source, path, "exec")

        except SyntaxError as e:
            return {
                "valid": False,
                "message": f"Syntax error: {e}",
                "plugin_classes": [],
            }

        # Try to load and find plugin classes
        try:
            module_name = f"plugin_validate_{path.stem}_{id(path)}"
            spec = importlib.util.spec_from_file_location(module_name, path)
            if not spec or not spec.loader:
                return {
                    "valid": False,
                    "message": "Failed to create module spec",
                    "plugin_classes": [],
                }

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            plugin_classes = self._find_plugin_classes(module)

            # Cleanup
            del sys.modules[module_name]

            if not plugin_classes:
                return {
                    "valid": False,
                    "message": "No PluginBase subclass found",
                    "plugin_classes": [],
                }

            class_names = [c.__name__ for c in plugin_classes]

            return {
                "valid": True,
                "message": f"Found {len(plugin_classes)} plugin class(es)",
                "plugin_classes": class_names,
            }

        except Exception as e:
            return {
                "valid": False,
                "message": f"Error loading module: {e}",
                "plugin_classes": [],
            }


def load_plugin(file_path: str, **kwargs) -> Optional[PluginBase]:
    """
    Convenience function to load a plugin from a file.

    Args:
        file_path: Path to the Python file
        **kwargs: Arguments to pass to the plugin constructor

    Returns:
        Plugin instance or None
    """
    loader = PluginLoader()
    return loader.load_from_file(file_path, **kwargs)


def discover_plugins(package_name: str, **kwargs) -> List[PluginBase]:
    """
    Convenience function to discover plugins in a package.

    Args:
        package_name: Name of the package
        **kwargs: Arguments to pass to plugin constructors

    Returns:
        List of plugin instances
    """
    loader = PluginLoader()
    return loader.discover(package_name, **kwargs)
