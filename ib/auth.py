"""
auth.py - Token-based authentication for command server

Provides secure pre-shared token authentication for Unix socket
and TCP command interfaces.

Usage:
    from auth import TokenStore

    # Generate and save a new token
    store = TokenStore("/path/to/token.key")
    token = store.generate_and_save()

    # Later, validate incoming tokens
    if store.validate(incoming_token):
        # Execute command
        pass
"""

import logging
import os
import secrets
import hmac
import tempfile
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Default token length (32 bytes = 256 bits, URL-safe base64 encoded = ~43 chars)
DEFAULT_TOKEN_LENGTH = 32

# Secure file permissions (owner read/write only)
SECURE_FILE_MODE = 0o600


@dataclass
class AuthResult:
    """Result of authentication attempt"""

    authenticated: bool
    error: Optional[str] = None

    @property
    def is_success(self) -> bool:
        return self.authenticated


def generate_token(length: int = DEFAULT_TOKEN_LENGTH) -> str:
    """
    Generate a cryptographically secure token.

    Args:
        length: Number of random bytes (actual string will be longer due to encoding)

    Returns:
        URL-safe base64 encoded token string
    """
    return secrets.token_urlsafe(length)


def constant_time_compare(a: str, b: str) -> bool:
    """
    Compare two strings in constant time to prevent timing attacks.

    Args:
        a: First string
        b: Second string

    Returns:
        True if strings are equal
    """
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


class TokenStore:
    """
    Manages authentication token storage and validation.

    Tokens are stored in a file with secure permissions (0o600).
    Uses constant-time comparison to prevent timing attacks.
    """

    def __init__(
        self,
        token_file: Path,
        file_mode: int = SECURE_FILE_MODE,
    ):
        """
        Initialize token store.

        Args:
            token_file: Path to token file
            file_mode: File permissions (default: 0o600 - owner read/write only)
        """
        self.token_file = Path(token_file)
        self.file_mode = file_mode
        self._cached_token: Optional[str] = None

    def exists(self) -> bool:
        """Check if token file exists"""
        return self.token_file.exists()

    def load(self) -> Optional[str]:
        """
        Load token from file.

        Returns:
            Token string or None if file doesn't exist
        """
        if not self.token_file.exists():
            return None

        try:
            token = self.token_file.read_text().strip()
            self._cached_token = token
            return token
        except Exception as e:
            logger.error(f"Failed to load token from {self.token_file}: {e}")
            return None

    def save(self, token: str) -> bool:
        """
        Save token to file with secure permissions.

        Args:
            token: Token string to save

        Returns:
            True if saved successfully
        """
        try:
            # Ensure parent directory exists
            self.token_file.parent.mkdir(parents=True, exist_ok=True)

            # Write to a temp file, set permissions, then atomically rename
            # so the token is never world-readable even briefly.
            fd, tmp_path = tempfile.mkstemp(dir=self.token_file.parent)
            try:
                os.write(fd, (token + "\n").encode())
                os.close(fd)
                fd = -1
                os.chmod(tmp_path, self.file_mode)
                Path(tmp_path).rename(self.token_file)
            except Exception:
                if fd >= 0:
                    os.close(fd)
                Path(tmp_path).unlink(missing_ok=True)
                raise

            self._cached_token = token
            logger.info(f"Token saved to {self.token_file}")
            return True

        except Exception as e:
            logger.error(f"Failed to save token to {self.token_file}: {e}")
            return False

    def generate_and_save(self, length: int = DEFAULT_TOKEN_LENGTH) -> Optional[str]:
        """
        Generate a new token and save it.

        Args:
            length: Token length in bytes

        Returns:
            Generated token string or None on failure
        """
        token = generate_token(length)
        if self.save(token):
            return token
        return None

    def validate(self, token: str) -> bool:
        """
        Validate a token against the stored token.

        Uses constant-time comparison to prevent timing attacks.

        Args:
            token: Token to validate

        Returns:
            True if token is valid
        """
        stored_token = self._cached_token or self.load()

        if stored_token is None:
            logger.warning("No token configured for validation")
            return False

        if not token:
            return False

        return constant_time_compare(token, stored_token)

    def delete(self) -> bool:
        """
        Delete the token file.

        Returns:
            True if deleted successfully
        """
        try:
            if self.token_file.exists():
                self.token_file.unlink()
                self._cached_token = None
                logger.info(f"Token file deleted: {self.token_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete token file: {e}")
            return False

    def check_permissions(self) -> bool:
        """
        Check if token file has secure permissions.

        Returns:
            True if permissions are secure (owner-only access)
        """
        if not self.token_file.exists():
            return True  # No file, no problem

        try:
            stat = self.token_file.stat()
            mode = stat.st_mode & 0o777

            # Check if only owner has access
            if mode != self.file_mode:
                logger.warning(
                    f"Token file has insecure permissions: {oct(mode)} "
                    f"(expected {oct(self.file_mode)})"
                )
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to check token file permissions: {e}")
            return False


class Authenticator:
    """
    Handles authentication for command server.

    Extracts and validates tokens from command strings.
    """

    AUTH_PREFIX = "AUTH"

    def __init__(self, token_store: Optional[TokenStore] = None):
        """
        Initialize authenticator.

        Args:
            token_store: TokenStore instance (None = authentication disabled)
        """
        self.token_store = token_store
        self._auth_required = token_store is not None

    @property
    def is_enabled(self) -> bool:
        """Check if authentication is enabled"""
        return self._auth_required

    def parse_command(self, command_line: str) -> tuple[AuthResult, str]:
        """
        Parse and authenticate a command line.

        Expected format when auth enabled: AUTH <token> <command> [args]
        When auth disabled: <command> [args]

        Args:
            command_line: Raw command line from client

        Returns:
            Tuple of (AuthResult, remaining_command)
            If auth fails, remaining_command will be empty string
        """
        if not self._auth_required:
            return AuthResult(authenticated=True), command_line.strip()

        parts = command_line.strip().split(None, 2)

        # Check for AUTH prefix
        if len(parts) < 2 or parts[0].upper() != self.AUTH_PREFIX:
            return AuthResult(
                authenticated=False,
                error="Authentication required. Format: AUTH <token> <command>",
            ), ""

        token = parts[1]
        remaining = parts[2] if len(parts) > 2 else ""

        # Validate token
        if not self.token_store.validate(token):
            logger.warning("Authentication failed: invalid token")
            return AuthResult(
                authenticated=False,
                error="Invalid authentication token",
            ), ""

        return AuthResult(authenticated=True), remaining

    def wrap_command(self, token: str, command: str) -> str:
        """
        Wrap a command with authentication token.

        Args:
            token: Authentication token
            command: Command to wrap

        Returns:
            Authenticated command string
        """
        return f"{self.AUTH_PREFIX} {token} {command}"


def create_token_file(path: Path, length: int = DEFAULT_TOKEN_LENGTH) -> Optional[str]:
    """
    Create a new token file.

    Convenience function for CLI usage.

    Args:
        path: Path for token file
        length: Token length in bytes

    Returns:
        Generated token or None on failure
    """
    store = TokenStore(path)
    return store.generate_and_save(length)


def load_token(path: Path) -> Optional[str]:
    """
    Load token from file.

    Convenience function for CLI usage.

    Args:
        path: Path to token file

    Returns:
        Token string or None
    """
    store = TokenStore(path)
    return store.load()
