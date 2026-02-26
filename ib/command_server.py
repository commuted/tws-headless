"""
command_server.py - Socket-based command interface

Provides a Unix socket server that accepts commands from external programs.
Commands can trigger actions like liquidation, status queries, or shutdown.
"""

import json
import logging
import os
import secrets
import socket
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, Optional, Any, List

from .auth import TokenStore, Authenticator

logger = logging.getLogger(__name__)


# Default socket path
DEFAULT_SOCKET_PATH = "/tmp/ib_portfolio.sock"


class CommandStatus(Enum):
    """Command execution status"""
    SUCCESS = "success"
    ERROR = "error"
    PENDING = "pending"
    UNAUTHORIZED = "unauthorized"


@dataclass
class CommandResult:
    """Result of a command execution"""
    status: CommandStatus
    message: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    request_token: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "status": self.status.value,
            "message": self.message,
            "data": self.data,
        }
        if self.request_token is not None:
            result["request_token"] = self.request_token
        return result

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class RequestEntry:
    """A tracked request in the queue"""
    token: str
    command: str
    status: str = "active"  # "active" or "completed"
    result: Optional[CommandResult] = None
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None


class RequestQueue:
    """
    Tracks in-flight and recently completed requests.
    Dynamically sized: all active requests + up to 8 completed.
    Detects duplicate tokens and suggests alternatives.
    """
    MAX_COMPLETED = 8

    def __init__(self):
        self._active: Dict[str, RequestEntry] = {}
        self._completed: deque = deque(maxlen=self.MAX_COMPLETED)
        self._lock = threading.Lock()

    def try_enqueue(self, token: str, command: str) -> Optional[str]:
        """
        Add request. Returns None on success, suggested token on duplicate.
        """
        with self._lock:
            # Check active and completed for duplicates
            completed_tokens = {e.token for e in self._completed}
            if token in self._active or token in completed_tokens:
                suggested = self._generate_token_locked()
                return suggested
            self._active[token] = RequestEntry(token=token, command=command)
            return None

    def complete(self, token: str, result: Optional[CommandResult] = None) -> None:
        """Move request from active to completed. deque(maxlen=8) auto-evicts oldest."""
        with self._lock:
            entry = self._active.pop(token, None)
            if entry is not None:
                entry.status = "completed"
                entry.result = result
                entry.completed_at = time.time()
                self._completed.append(entry)

    def generate_token(self) -> str:
        """Generate a unique token not in the queue."""
        with self._lock:
            return self._generate_token_locked()

    def _generate_token_locked(self) -> str:
        """Generate a unique token (must hold self._lock)."""
        completed_tokens = {e.token for e in self._completed}
        while True:
            token = secrets.token_hex(8)
            if token not in self._active and token not in completed_tokens:
                return token

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._active) + len(self._completed)


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
        token_file: Optional[Path] = None,
    ):
        """
        Initialize the command server.

        Args:
            socket_path: Path to Unix domain socket
            tcp_port: Optional TCP port (if set, uses TCP instead of Unix socket)
            token_file: Path to authentication token file (None = no auth)
        """
        self.socket_path = socket_path
        self.tcp_port = tcp_port
        self._handlers: Dict[str, Callable[[List[str]], CommandResult]] = {}
        self._server_socket: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()

        # Authentication
        self._token_store: Optional[TokenStore] = None
        self._authenticator: Authenticator
        if token_file:
            self._token_store = TokenStore(token_file)
            if not self._token_store.exists():
                logger.warning(
                    f"Token file {token_file} does not exist. "
                    "Authentication enabled but no token configured."
                )
            self._authenticator = Authenticator(self._token_store)
            logger.info(f"Authentication enabled with token file: {token_file}")
        else:
            self._authenticator = Authenticator()  # Auth disabled

        # Request tracking
        self._request_queue = RequestQueue()

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

    @property
    def auth_enabled(self) -> bool:
        """Check if authentication is enabled"""
        return self._authenticator.is_enabled

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

    def _parse_request_token(self, command_line: str) -> tuple:
        """Extract optional REQ <token> prefix. Returns (token, remaining)."""
        parts = command_line.strip().split(None, 2)
        if len(parts) >= 2 and parts[0].upper() == "REQ":
            return parts[1], parts[2] if len(parts) > 2 else ""
        return self._request_queue.generate_token(), command_line

    def _handle_client(self, client_socket: socket.socket):
        """Handle a client connection"""
        request_token = None
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

            # Extract request token (strips REQ prefix if present)
            request_token, remaining = self._parse_request_token(command_line)

            # Check for duplicate token
            suggested = self._request_queue.try_enqueue(request_token, remaining)
            if suggested is not None:
                result = CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Duplicate request token: {request_token}",
                    data={"suggested_token": suggested},
                    request_token=request_token,
                )
                response = result.to_json() + "\n"
                client_socket.sendall(response.encode("utf-8"))
                return

            result = self._execute_command(remaining)
            result.request_token = request_token

            # Send response
            response = result.to_json() + "\n"
            client_socket.sendall(response.encode("utf-8"))

        except Exception as e:
            logger.error(f"Error handling client: {e}")
            try:
                error_result = CommandResult(
                    status=CommandStatus.ERROR,
                    message=str(e),
                    request_token=request_token,
                )
                client_socket.sendall((error_result.to_json() + "\n").encode("utf-8"))
            except Exception:
                pass
        finally:
            if request_token is not None:
                self._request_queue.complete(request_token)
            try:
                client_socket.close()
            except Exception:
                pass

    def _execute_command(self, command_line: str) -> CommandResult:
        """Parse, authenticate, and execute a command"""
        # Authenticate first
        auth_result, remaining_command = self._authenticator.parse_command(command_line)

        if not auth_result.is_success:
            return CommandResult(
                status=CommandStatus.UNAUTHORIZED,
                message=auth_result.error or "Authentication failed",
            )

        # Parse the actual command
        parts = remaining_command.split()
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
    token: Optional[str] = None,
    request_token: Optional[str] = None,
) -> CommandResult:
    """
    Send a command to the running server.

    Args:
        command: Command string to send
        socket_path: Path to Unix socket
        tcp_port: TCP port (if using TCP instead of Unix socket)
        timeout: Connection timeout in seconds
        token: Authentication token (if server requires auth)
        request_token: Optional request token for tracking/dedup

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

        # Build wire command: REQ first, then AUTH, then command
        full_command = command
        if token:
            full_command = f"AUTH {token} {full_command}"
        if request_token:
            full_command = f"REQ {request_token} {full_command}"

        # Send command
        sock.sendall((full_command + "\n").encode("utf-8"))

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
            request_token=response.get("request_token"),
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
