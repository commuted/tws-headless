"""
environment.py - Paper vs. live trading environment separation.

Single source of truth for keeping paper and live operation from comingling.
Paper and live must never share a command socket, an execution database, a log
file, or an account. This module derives a canonical environment (PAPER | LIVE)
and provides the namespaced resource paths and safety guardrails used across the
engine.

Design:
  - Pre-connect resources (command socket, log file) are keyed on the *environment*
    (paper|live), which is known at startup from the port (or an explicit --env).
  - Post-connect resources (execution DB) are keyed on the actual *account_id*,
    consistent with the existing plugin-store/state separation.
  - Once the account is known, check_env_consistency() confirms the account's
    paper/live nature matches the declared environment (hard fail on mismatch).

All functions here are pure so they can be unit-tested without a live connection.
"""

from enum import Enum
from pathlib import Path
from typing import Optional, Union


class TradingEnv(str, Enum):
    """Trading environment. Inherits str so it formats cleanly into paths/logs."""
    PAPER = "paper"
    LIVE = "live"


# Standard IB TWS/Gateway ports.
#   7497 = paper TWS, 4002 = paper Gateway
#   7496 = live  TWS, 4001 = live  Gateway
PAPER_PORTS = {7497, 4002}
LIVE_PORTS = {7496, 4001}

# IB paper account numbers are prefixed "DU" (individual) or "DF" (advisor/FA).
# Live account numbers use other prefixes (typically "U").
_PAPER_ACCOUNT_PREFIXES = ("DU", "DF")


def _coerce_env(value: Union[str, "TradingEnv"]) -> "TradingEnv":
    """Normalise a str or TradingEnv to a TradingEnv.

    Note: str(TradingEnv.PAPER) is 'TradingEnv.PAPER', not 'paper', so we must
    branch on the enum type rather than stringifying blindly.
    """
    if isinstance(value, TradingEnv):
        return value
    return TradingEnv(str(value).lower())


def env_from_port(port: Optional[int]) -> Optional[TradingEnv]:
    """Derive environment from a TWS/Gateway port. Returns None for unknown ports."""
    if port in PAPER_PORTS:
        return TradingEnv.PAPER
    if port in LIVE_PORTS:
        return TradingEnv.LIVE
    return None


def env_from_account(account_id: str) -> TradingEnv:
    """Derive environment from an IB account id (DU/DF prefix -> paper, else live)."""
    acct = (account_id or "").strip().upper()
    if acct.startswith(_PAPER_ACCOUNT_PREFIXES):
        return TradingEnv.PAPER
    return TradingEnv.LIVE


def resolve_env(
    port: Optional[int],
    explicit: Optional[Union[str, TradingEnv]] = None,
) -> TradingEnv:
    """Resolve the environment at startup.

    An explicit --env value always wins. Otherwise derive from the port. If neither
    yields an environment (unknown port, no --env), raise so the caller must be
    explicit rather than guess.
    """
    if explicit:
        return _coerce_env(explicit)
    env = env_from_port(port)
    if env is not None:
        return env
    raise ValueError(
        f"Cannot determine trading environment: port {port} is not a known "
        f"paper ({sorted(PAPER_PORTS)}) or live ({sorted(LIVE_PORTS)}) port. "
        "Pass --env paper|live explicitly."
    )


def socket_path_for(env: Union[str, TradingEnv]) -> str:
    """Command-socket path for an environment: ~/.tws_headless_{env}.sock"""
    env = _coerce_env(env)
    return str(Path.home() / f".tws_headless_{env.value}.sock")


def execution_db_path_for(account_id: str) -> Path:
    """Execution/fills DB path for an account: ~/.ib_executions_{account_id}.db"""
    return Path.home() / f".ib_executions_{account_id}.db"


def log_path_for(env: Union[str, TradingEnv], base_dir: Optional[Path] = None) -> Path:
    """Engine log path for an environment: {base_dir}/logs/{env}/engine.log

    base_dir defaults to the project root (parent of this file's package).
    """
    env = _coerce_env(env)
    root = base_dir if base_dir is not None else Path(__file__).resolve().parent.parent
    return root / "logs" / env.value / "engine.log"


def check_env_consistency(
    declared_env: Union[str, TradingEnv],
    account_id: str,
    allow_mismatch: bool = False,
) -> Optional[str]:
    """Confirm the connected account matches the declared environment.

    Returns None when consistent (or when overridden). Returns a human-readable
    error message when the account's paper/live nature contradicts declared_env
    and the mismatch is not explicitly allowed.
    """
    declared = _coerce_env(declared_env)
    actual = env_from_account(account_id)
    if actual == declared:
        return None
    if allow_mismatch:
        return None
    return (
        f"Environment mismatch: engine launched as {declared.value.upper()} but "
        f"account {account_id!r} is a {actual.value.upper()} account. Refusing to "
        "start to avoid comingling paper and live operation. Pass "
        "--allow-env-mismatch to override (not recommended)."
    )


def require_live_confirmation(
    env: Union[str, TradingEnv],
    order_mode: object,
    confirmed: bool,
) -> Optional[str]:
    """Gate real orders against a live account behind explicit confirmation.

    Returns None when the operation is allowed. Returns an error message when the
    environment is LIVE and the order mode would place real orders (immediate or
    queued) without --live-confirmed. order_mode may be an OrderExecutionMode enum
    or its string value.
    """
    if _coerce_env(env) != TradingEnv.LIVE:
        return None
    mode_value = str(getattr(order_mode, "value", order_mode)).lower()
    if mode_value not in ("immediate", "queued"):
        return None  # dry_run is always safe
    if confirmed:
        return None
    return (
        f"Refusing to run order mode {mode_value!r} against a LIVE account without "
        "confirmation. Pass --live-confirmed to place real orders."
    )
