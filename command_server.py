"""
command_server.py - Socket-based command interface

Provides a Unix socket server that accepts commands from external programs.
Commands can trigger actions like liquidation, status queries, or shutdown.
"""

import json
import logging
import os
import socket
import threading
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, Optional, Any, List

logger = logging.getLogger(__name__)


# Default socket path
DEFAULT_SOCKET_PATH = "/tmp/ib_portfolio.sock"


class CommandStatus(Enum):
    """Command execution status"""
    SUCCESS = "success"
    ERROR = "error"
    PENDING = "pending"


@dataclass
class CommandResult:
    """Result of a command execution"""
    status: CommandStatus
    message: str = ""
    data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "message": self.message,
            "data": self.data,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


class CommandServer:
    """
    Unix socket server for receiving commands.

    Listens on a Unix domain socket and dispatches commands
    to registered handlers.

    Usage:
        server = CommandServer()
        server.register_handler("status", lambda args: CommandResult(...))
        server.start()
        ...
        server.stop()
    """

    def __init__(
        self,
        socket_path: str = DEFAULT_SOCKET_PATH,
        tcp_port: Optional[int] = None,
    ):
        """
        Initialize the command server.

        Args:
            socket_path: Path to Unix domain socket
            tcp_port: Optional TCP port (if set, uses TCP instead of Unix socket)
        """
        self.socket_path = socket_path
        self.tcp_port = tcp_port
        self._handlers: Dict[str, Callable[[List[str]], CommandResult]] = {}
        self._server_socket: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()

        # Register built-in commands
        self.register_handler("help", self._handle_help)
        self.register_handler("ping", self._handle_ping)

    def register_handler(
        self,
        command: str,
        handler: Callable[[List[str]], CommandResult],
    ):
        """
        Register a command handler.

        Args:
            command: Command name (case-insensitive)
            handler: Function that takes args list and returns CommandResult
        """
        self._handlers[command.lower()] = handler

    def unregister_handler(self, command: str):
        """Remove a command handler"""
        self._handlers.pop(command.lower(), None)

    @property
    def commands(self) -> List[str]:
        """List of registered commands"""
        return sorted(self._handlers.keys())

    def start(self) -> bool:
        """
        Start the command server.

        Returns:
            True if started successfully
        """
        if self._running:
            logger.warning("Command server already running")
            return False

        try:
            if self.tcp_port:
                self._start_tcp()
            else:
                self._start_unix()

            self._running = True
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

            logger.info(f"Command server started on {self._address_str}")
            return True

        except Exception as e:
            logger.error(f"Failed to start command server: {e}")
            return False

    def _start_unix(self):
        """Start Unix domain socket server"""
        # Remove existing socket file
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

        self._server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind(self.socket_path)
        self._server_socket.listen(5)
        self._server_socket.settimeout(1.0)  # Allow periodic checks

        # Set permissions (owner only)
        os.chmod(self.socket_path, 0o600)

    def _start_tcp(self):
        """Start TCP socket server"""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind(("127.0.0.1", self.tcp_port))
        self._server_socket.listen(5)
        self._server_socket.settimeout(1.0)

    @property
    def _address_str(self) -> str:
        """String representation of server address"""
        if self.tcp_port:
            return f"tcp://127.0.0.1:{self.tcp_port}"
        return f"unix://{self.socket_path}"

    def stop(self):
        """Stop the command server"""
        if not self._running:
            return

        logger.info("Stopping command server...")
        self._running = False

        # Close socket to interrupt accept()
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass

        # Wait for thread
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

        # Cleanup Unix socket file
        if not self.tcp_port and os.path.exists(self.socket_path):
            try:
                os.unlink(self.socket_path)
            except Exception:
                pass

        logger.info("Command server stopped")

    def _run(self):
        """Main server loop"""
        while self._running:
            try:
                client_socket, _ = self._server_socket.accept()
                # Handle client in a separate thread
                threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                    daemon=True,
                ).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    logger.error(f"Error accepting connection: {e}")

    def _handle_client(self, client_socket: socket.socket):
        """Handle a client connection"""
        try:
            client_socket.settimeout(30.0)

            # Read command (newline-terminated)
            data = b""
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b"\n" in data:
                    break

            if not data:
                return

            # Parse command
            command_line = data.decode("utf-8").strip()
            result = self._execute_command(command_line)

            # Send response
            response = result.to_json() + "\n"
            client_socket.sendall(response.encode("utf-8"))

        except Exception as e:
            logger.error(f"Error handling client: {e}")
            try:
                error_result = CommandResult(
                    status=CommandStatus.ERROR,
                    message=str(e),
                )
                client_socket.sendall((error_result.to_json() + "\n").encode("utf-8"))
            except Exception:
                pass
        finally:
            try:
                client_socket.close()
            except Exception:
                pass

    def _execute_command(self, command_line: str) -> CommandResult:
        """Parse and execute a command"""
        parts = command_line.split()
        if not parts:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Empty command",
            )

        command = parts[0].lower()
        args = parts[1:]

        handler = self._handlers.get(command)
        if not handler:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Unknown command: {command}. Use 'help' for available commands.",
            )

        try:
            return handler(args)
        except Exception as e:
            logger.error(f"Error executing command '{command}': {e}")
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Command failed: {e}",
            )

    # Built-in command handlers

    def _handle_help(self, args: List[str]) -> CommandResult:
        """Handle 'help' command"""
        commands_list = ", ".join(self.commands)
        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"Available commands: {commands_list}",
            data={"commands": self.commands},
        )

    def _handle_ping(self, args: List[str]) -> CommandResult:
        """Handle 'ping' command"""
        return CommandResult(
            status=CommandStatus.SUCCESS,
            message="pong",
        )


def send_command(
    command: str,
    socket_path: str = DEFAULT_SOCKET_PATH,
    tcp_port: Optional[int] = None,
    timeout: float = 10.0,
) -> CommandResult:
    """
    Send a command to the running server.

    Args:
        command: Command string to send
        socket_path: Path to Unix socket
        tcp_port: TCP port (if using TCP instead of Unix socket)
        timeout: Connection timeout in seconds

    Returns:
        CommandResult from server
    """
    try:
        if tcp_port:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", tcp_port))
        else:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(socket_path)

        sock.settimeout(timeout)

        # Send command
        sock.sendall((command + "\n").encode("utf-8"))

        # Receive response
        data = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
            if b"\n" in data:
                break

        sock.close()

        # Parse response
        response = json.loads(data.decode("utf-8").strip())
        return CommandResult(
            status=CommandStatus(response.get("status", "error")),
            message=response.get("message", ""),
            data=response.get("data", {}),
        )

    except FileNotFoundError:
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Server not running (socket not found: {socket_path})",
        )
    except ConnectionRefusedError:
        return CommandResult(
            status=CommandStatus.ERROR,
            message="Connection refused - server may not be running",
        )
    except Exception as e:
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Failed to send command: {e}",
        )
