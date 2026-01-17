"""
Tests for auth.py - Token-based authentication
"""

import pytest
import os
import tempfile
from pathlib import Path

from auth import (
    generate_token,
    constant_time_compare,
    TokenStore,
    Authenticator,
    AuthResult,
    create_token_file,
    load_token,
    DEFAULT_TOKEN_LENGTH,
    SECURE_FILE_MODE,
)


class TestGenerateToken:
    """Tests for token generation"""

    def test_generates_token(self):
        """Test that generate_token returns a string"""
        token = generate_token()

        assert isinstance(token, str)
        assert len(token) > 0

    def test_token_is_url_safe(self):
        """Test that generated tokens are URL-safe"""
        token = generate_token()

        # URL-safe base64 uses only these characters
        valid_chars = set(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        )
        assert all(c in valid_chars for c in token)

    def test_tokens_are_unique(self):
        """Test that generated tokens are unique"""
        tokens = [generate_token() for _ in range(100)]

        assert len(set(tokens)) == 100

    def test_custom_length(self):
        """Test generating tokens with custom length"""
        short_token = generate_token(8)
        long_token = generate_token(64)

        # Longer input should produce longer output
        assert len(long_token) > len(short_token)


class TestConstantTimeCompare:
    """Tests for constant-time comparison"""

    def test_equal_strings(self):
        """Test comparison of equal strings"""
        assert constant_time_compare("abc123", "abc123") is True

    def test_unequal_strings(self):
        """Test comparison of unequal strings"""
        assert constant_time_compare("abc123", "abc124") is False

    def test_different_lengths(self):
        """Test comparison of different length strings"""
        assert constant_time_compare("abc", "abcd") is False

    def test_empty_strings(self):
        """Test comparison of empty strings"""
        assert constant_time_compare("", "") is True

    def test_empty_vs_nonempty(self):
        """Test comparison of empty vs non-empty"""
        assert constant_time_compare("", "abc") is False


class TestAuthResult:
    """Tests for AuthResult dataclass"""

    def test_success_result(self):
        """Test successful auth result"""
        result = AuthResult(authenticated=True)

        assert result.authenticated is True
        assert result.is_success is True
        assert result.error is None

    def test_failure_result(self):
        """Test failed auth result"""
        result = AuthResult(authenticated=False, error="Invalid token")

        assert result.authenticated is False
        assert result.is_success is False
        assert result.error == "Invalid token"


class TestTokenStore:
    """Tests for TokenStore class"""

    def test_init(self):
        """Test TokenStore initialization"""
        store = TokenStore(Path("/tmp/test.token"))

        assert store.token_file == Path("/tmp/test.token")
        assert store.file_mode == SECURE_FILE_MODE

    def test_exists_false(self):
        """Test exists when file doesn't exist"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "nonexistent.token")

            assert store.exists() is False

    def test_exists_true(self):
        """Test exists when file exists"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            try:
                store = TokenStore(Path(f.name))
                assert store.exists() is True
            finally:
                os.unlink(f.name)

    def test_save_and_load(self):
        """Test saving and loading a token"""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "test.token"
            store = TokenStore(token_path)

            # Save
            result = store.save("my_secret_token")
            assert result is True

            # Load
            loaded = store.load()
            assert loaded == "my_secret_token"

    def test_save_creates_directory(self):
        """Test that save creates parent directories"""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "subdir" / "deep" / "test.token"
            store = TokenStore(token_path)

            result = store.save("token")

            assert result is True
            assert token_path.exists()

    def test_save_sets_permissions(self):
        """Test that save sets secure file permissions"""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "test.token"
            store = TokenStore(token_path)

            store.save("token")

            mode = token_path.stat().st_mode & 0o777
            assert mode == SECURE_FILE_MODE

    def test_load_nonexistent(self):
        """Test loading from nonexistent file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "nonexistent.token")

            assert store.load() is None

    def test_generate_and_save(self):
        """Test generating and saving a token"""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "test.token"
            store = TokenStore(token_path)

            token = store.generate_and_save()

            assert token is not None
            assert len(token) > 0

            # Should be loadable
            loaded = store.load()
            assert loaded == token

    def test_validate_success(self):
        """Test token validation success"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "test.token")
            token = store.generate_and_save()

            assert store.validate(token) is True

    def test_validate_failure(self):
        """Test token validation failure"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "test.token")
            store.generate_and_save()

            assert store.validate("wrong_token") is False

    def test_validate_empty_token(self):
        """Test validation of empty token"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "test.token")
            store.generate_and_save()

            assert store.validate("") is False

    def test_validate_no_stored_token(self):
        """Test validation when no token is stored"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "nonexistent.token")

            assert store.validate("any_token") is False

    def test_delete(self):
        """Test token file deletion"""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "test.token"
            store = TokenStore(token_path)
            store.generate_and_save()

            assert token_path.exists()

            result = store.delete()

            assert result is True
            assert not token_path.exists()

    def test_delete_nonexistent(self):
        """Test deleting nonexistent file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "nonexistent.token")

            result = store.delete()

            assert result is True  # No error

    def test_check_permissions_secure(self):
        """Test checking secure permissions"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "test.token")
            store.generate_and_save()

            assert store.check_permissions() is True

    def test_check_permissions_insecure(self):
        """Test checking insecure permissions"""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "test.token"
            store = TokenStore(token_path)
            store.save("token")

            # Make permissions insecure
            os.chmod(token_path, 0o644)

            assert store.check_permissions() is False

    def test_cached_token(self):
        """Test that token is cached after load"""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "test.token"
            store = TokenStore(token_path)
            original_token = store.generate_and_save()

            # Load to cache
            store.load()

            # Delete file
            token_path.unlink()

            # Should still validate from cache
            assert store.validate(original_token) is True


class TestAuthenticator:
    """Tests for Authenticator class"""

    def test_disabled_by_default(self):
        """Test authenticator disabled when no token store"""
        auth = Authenticator()

        assert auth.is_enabled is False

    def test_enabled_with_store(self):
        """Test authenticator enabled with token store"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "test.token")
            store.generate_and_save()

            auth = Authenticator(store)

            assert auth.is_enabled is True

    def test_parse_command_no_auth(self):
        """Test parsing command when auth disabled"""
        auth = Authenticator()

        result, command = auth.parse_command("status")

        assert result.is_success is True
        assert command == "status"

    def test_parse_command_with_args_no_auth(self):
        """Test parsing command with args when auth disabled"""
        auth = Authenticator()

        result, command = auth.parse_command("sell SPY 100")

        assert result.is_success is True
        assert command == "sell SPY 100"

    def test_parse_command_auth_success(self):
        """Test parsing authenticated command"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "test.token")
            token = store.generate_and_save()

            auth = Authenticator(store)

            result, command = auth.parse_command(f"AUTH {token} status")

            assert result.is_success is True
            assert command == "status"

    def test_parse_command_auth_with_args(self):
        """Test parsing authenticated command with args"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "test.token")
            token = store.generate_and_save()

            auth = Authenticator(store)

            result, command = auth.parse_command(f"AUTH {token} sell SPY 100")

            assert result.is_success is True
            assert command == "sell SPY 100"

    def test_parse_command_missing_auth(self):
        """Test parsing command without AUTH prefix when required"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "test.token")
            store.generate_and_save()

            auth = Authenticator(store)

            result, command = auth.parse_command("status")

            assert result.is_success is False
            assert "Authentication required" in result.error
            assert command == ""

    def test_parse_command_invalid_token(self):
        """Test parsing command with invalid token"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "test.token")
            store.generate_and_save()

            auth = Authenticator(store)

            result, command = auth.parse_command("AUTH wrong_token status")

            assert result.is_success is False
            assert "Invalid" in result.error
            assert command == ""

    def test_parse_command_auth_only(self):
        """Test parsing AUTH with token but no command"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "test.token")
            token = store.generate_and_save()

            auth = Authenticator(store)

            result, command = auth.parse_command(f"AUTH {token}")

            assert result.is_success is True
            assert command == ""

    def test_parse_command_case_insensitive(self):
        """Test AUTH prefix is case insensitive"""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(Path(tmpdir) / "test.token")
            token = store.generate_and_save()

            auth = Authenticator(store)

            result, command = auth.parse_command(f"auth {token} status")

            assert result.is_success is True
            assert command == "status"

    def test_wrap_command(self):
        """Test wrapping command with token"""
        auth = Authenticator()

        wrapped = auth.wrap_command("my_token", "status")

        assert wrapped == "AUTH my_token status"

    def test_wrap_command_with_args(self):
        """Test wrapping command with args"""
        auth = Authenticator()

        wrapped = auth.wrap_command("my_token", "sell SPY 100")

        assert wrapped == "AUTH my_token sell SPY 100"


class TestConvenienceFunctions:
    """Tests for convenience functions"""

    def test_create_token_file(self):
        """Test create_token_file function"""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "test.token"

            token = create_token_file(token_path)

            assert token is not None
            assert token_path.exists()

    def test_load_token(self):
        """Test load_token function"""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "test.token"
            original = create_token_file(token_path)

            loaded = load_token(token_path)

            assert loaded == original

    def test_load_token_nonexistent(self):
        """Test loading nonexistent token"""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "nonexistent.token"

            loaded = load_token(token_path)

            assert loaded is None


class TestSecurityProperties:
    """Tests for security-related properties"""

    def test_token_entropy(self):
        """Test that tokens have sufficient entropy"""
        # Generate many tokens and check they're all unique
        tokens = [generate_token() for _ in range(1000)]

        assert len(set(tokens)) == 1000

    def test_token_length(self):
        """Test minimum token length for security"""
        token = generate_token(DEFAULT_TOKEN_LENGTH)

        # URL-safe base64 expands by ~4/3, so 32 bytes -> ~43 chars
        assert len(token) >= 40

    def test_file_permissions_restrictive(self):
        """Test that default permissions are restrictive"""
        # 0o600 = owner read/write only
        assert SECURE_FILE_MODE == 0o600
