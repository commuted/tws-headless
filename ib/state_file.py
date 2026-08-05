"""
state_file.py — portable STATE snapshot for relocating a stopped engine.

The engine's runtime state is split across four stores, three of which live in
$HOME rather than the repo:

  1. plugins/<slot>/<account>/{state,holdings,instruments}.json  (in repo, gitignored)
  2. ~/.ib_plugin_store_{account}.db   — which plugins reload, and in what status
  3. ~/.ib_executions_{account}.db     — fills/executions history
  4. ~/.ib_forex_cost_basis.json       — forex cost basis

So a repo image alone cannot relocate a running platform: a git clone carries
none of (1) because it is gitignored, and nothing at all of (2)-(4). This module
collects the parts needed to *restart* into one file, so that STATE file + repo
image is sufficient.

What is deliberately NOT carried: the executions DB and historical/bars.db. The
first is an audit log of past fills and the second a rebuildable market-data
cache; neither is needed to resume trading, and both are large. They are listed
in the snapshot's ``not_carried`` manifest so a restore can tell you what was
left behind rather than leaving you to discover it.

Restore is deliberately not a merge. It writes back what the snapshot says is
true and lets the engine's existing startup reconciliation
(``PluginExecutive.reconcile_with_account``) settle any remaining disagreement
against live IB positions — restore is the "what we believe" step, reconcile is
the "what is actually there" step.

Collection works against a *stopped* system: everything here reads files and
SQLite directly, with no engine connection, which is what lets ibctl.py collect
a snapshot from an engine that has already exited.
"""

import json
import logging
import os
import socket
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Bump when the on-disk shape changes incompatibly. restore_state() refuses a
# snapshot it does not understand rather than half-applying it.
STATE_VERSION = 1

DEFAULT_STATE_FILENAME = "STATE.json"

# Per-plugin runtime files worth carrying. alerts.jsonl is excluded on purpose:
# it is an append-only alert log, not state the engine reads back at startup.
PLUGIN_STATE_FILES = ("state.json", "holdings.json", "instruments.json")

FOREX_COST_BASIS_PATH = Path.home() / ".ib_forex_cost_basis.json"


def repo_root() -> Path:
    """Project root (parent of the ib/ package)."""
    return Path(__file__).resolve().parent.parent


def default_state_path(root: Optional[Path] = None) -> Path:
    """Default STATE file location: <repo root>/STATE.json.

    Kept beside the repo (not inside git — it is gitignored) because it holds
    live position data. It travels with a filesystem image of the repo, and is
    copied deliberately rather than committed.
    """
    return (root or repo_root()) / DEFAULT_STATE_FILENAME


def plugin_dir_path(explicit: Optional[Path] = None) -> Path:
    """Resolve the plugin directory the same way PluginBase does."""
    if explicit is not None:
        return Path(explicit)
    return Path(os.environ.get("IB_PLUGIN_DIR", repo_root() / "plugins"))


def plugin_store_path_for(account_id: str) -> Path:
    """Plugin registry DB for an account — mirrors configure_plugin_store()."""
    return Path.home() / f".ib_plugin_store_{account_id}.db"


def _git_commit(root: Path) -> Optional[str]:
    """Current HEAD of the source repo, for provenance. None if unavailable."""
    try:
        out = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        if out.returncode == 0:
            return out.stdout.strip()
    except Exception as e:
        logger.debug(f"Could not read git HEAD for provenance: {e}")
    return None


def _read_file_entry(path: Path, rel: str) -> Optional[Dict[str, Any]]:
    """Read one runtime file as a snapshot entry, JSON-parsed when possible."""
    try:
        text = path.read_text()
    except Exception as e:
        logger.warning(f"Could not read {path}: {e}")
        return None
    entry: Dict[str, Any] = {"path": rel}
    try:
        entry["json"] = json.loads(text)
    except json.JSONDecodeError:
        # Keep it verbatim rather than dropping it; restore writes text back
        # unchanged. A malformed state file is still the plugin's state file.
        logger.warning(f"{path} is not valid JSON; carrying it verbatim")
        entry["text"] = text
    return entry


def _collect_plugin_files(plugin_dir: Path, account_id: str) -> List[Dict[str, Any]]:
    """Collect per-plugin runtime files for one account.

    Picks up both layouts: the account-scoped plugins/<slot>/<account>/ used
    since account separation, and the older flat plugins/<slot>/ files that
    predate it (still present for some slots).
    """
    entries: List[Dict[str, Any]] = []
    if not plugin_dir.is_dir():
        logger.warning(f"Plugin directory does not exist: {plugin_dir}")
        return entries

    for slot_dir in sorted(p for p in plugin_dir.iterdir() if p.is_dir()):
        candidates = [slot_dir / name for name in PLUGIN_STATE_FILES]
        candidates += [slot_dir / account_id / name for name in PLUGIN_STATE_FILES]
        for path in candidates:
            if not path.is_file():
                continue
            rel = path.relative_to(plugin_dir).as_posix()
            entry = _read_file_entry(path, rel)
            if entry is not None:
                entries.append(entry)
    return entries


def _collect_registry(store_path: Path) -> List[Dict[str, Any]]:
    """Read the plugin registry directly from SQLite (no engine required)."""
    if not store_path.is_file():
        logger.warning(f"Plugin store not found: {store_path}")
        return []
    from .plugin_store import PluginStore
    return PluginStore(store_path).list_registry()


def _describe_uncarried(path: Path, why: str) -> Dict[str, Any]:
    """Manifest entry for state we know about but deliberately leave behind."""
    info: Dict[str, Any] = {"path": str(path), "reason": why, "exists": path.exists()}
    if path.exists():
        stat = path.stat()
        info["size_bytes"] = stat.st_size
        info["modified_at"] = datetime.fromtimestamp(stat.st_mtime).isoformat()
    return info


def collect_state(
    account_id: str,
    plugin_dir: Optional[Path] = None,
    plugin_store: Optional[Path] = None,
    root: Optional[Path] = None,
) -> Dict[str, Any]:
    """Build a STATE snapshot for one account by reading on-disk stores.

    Requires no engine connection, so it works on a stopped system.
    """
    root = root or repo_root()
    plugin_dir = plugin_dir_path(plugin_dir)
    store_path = Path(plugin_store) if plugin_store else plugin_store_path_for(account_id)

    from .environment import env_from_account

    registry = _collect_registry(store_path)
    files = _collect_plugin_files(plugin_dir, account_id)

    forex: Optional[Any] = None
    if FOREX_COST_BASIS_PATH.is_file():
        entry = _read_file_entry(FOREX_COST_BASIS_PATH, FOREX_COST_BASIS_PATH.name)
        if entry is not None:
            forex = entry.get("json", entry.get("text"))

    from .environment import execution_db_path_for

    snapshot: Dict[str, Any] = {
        "state_version": STATE_VERSION,
        "created_at": datetime.now().isoformat(),
        "source": {
            "host": socket.gethostname(),
            "account_id": account_id,
            "env": env_from_account(account_id).value,
            "repo_root": str(root),
            "repo_commit": _git_commit(root),
            "plugin_dir": str(plugin_dir),
            "plugin_store": str(store_path),
        },
        "plugin_registry": registry,
        "plugin_files": files,
        "forex_cost_basis": forex,
        "not_carried": [
            _describe_uncarried(
                execution_db_path_for(account_id),
                "fills/executions history — not required to resume trading",
            ),
            _describe_uncarried(
                root / "historical" / "bars.db",
                "market-data cache — refetched on demand",
            ),
        ],
    }
    logger.info(
        f"Collected STATE for {account_id}: {len(registry)} registry entries, "
        f"{len(files)} plugin files"
    )
    return snapshot


def write_state(snapshot: Dict[str, Any], path: Optional[Path] = None) -> Path:
    """Write a snapshot atomically so a crash mid-write cannot truncate it."""
    path = Path(path) if path else default_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(snapshot, indent=2, default=str))
    tmp.replace(path)
    logger.info(f"Wrote STATE file: {path}")
    return path


def load_state(path: Optional[Path] = None) -> Dict[str, Any]:
    """Read a STATE file. Raises if missing, malformed, or a version we can't apply."""
    path = Path(path) if path else default_state_path()
    snapshot = json.loads(path.read_text())
    version = snapshot.get("state_version")
    if version != STATE_VERSION:
        raise ValueError(
            f"STATE file {path} has state_version {version!r}, but this engine "
            f"understands {STATE_VERSION}. Refusing to apply a snapshot it may "
            "only partially understand."
        )
    return snapshot


def _reanchor(path_str: str, mappings: List[tuple]) -> str:
    """Rewrite a path under a source root to the same path under a target root.

    First mapping that contains the path wins; a path under none of them is
    returned unchanged.
    """
    p = Path(path_str)
    for src, dst in mappings:
        try:
            rel = p.relative_to(src)
        except ValueError:
            continue
        return str(Path(dst) / rel)
    return path_str


def restore_state(
    snapshot: Dict[str, Any],
    plugin_dir: Optional[Path] = None,
    plugin_store: Optional[Path] = None,
    expected_account: Optional[str] = None,
    allow_account_mismatch: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Write a snapshot back to the on-disk stores.

    This restores what the snapshot believes is true; live IB positions are
    settled afterwards by the engine's startup reconciliation, not here.

    An account mismatch is a hard error by default. Restoring a paper snapshot
    over a live account's stores (or vice versa) would cross exactly the
    paper/live boundary the rest of the engine works to keep separate.

    Registry ``class_path`` entries are RE-ANCHORED from the source machine's
    roots (``source.plugin_dir``, ``source.repo_root``) to this machine's.
    The registry stores absolute paths, so without this a snapshot restored
    onto a machine whose repo lives anywhere else round-trips the *source*
    machine's paths — and ``load_registered_plugins`` then skips every slot
    with nothing but a log warning, leaving a platform that starts cleanly
    and silently trades nothing. That failure defeated the module's stated
    purpose ("STATE file + repo image is sufficient") for any relocation that
    changed the path, which is most of them. A class_path that still does not
    exist after re-anchoring is reported in ``class_path_missing`` HERE, at
    restore time, when the operator is watching — not at the next engine
    start, when nobody is.
    """
    report: Dict[str, Any] = {
        "account_id": snapshot.get("source", {}).get("account_id"),
        "registry_restored": [],
        "registry_failed": [],
        "registry_extra": [],
        "class_path_rewritten": [],
        "class_path_missing": [],
        "files_restored": [],
        "files_failed": [],
        "forex_cost_basis_restored": False,
        "not_carried": snapshot.get("not_carried", []),
        "dry_run": dry_run,
    }

    account_id = report["account_id"]
    if not account_id:
        raise ValueError("STATE file has no source.account_id; refusing to restore.")

    if expected_account and expected_account != account_id:
        message = (
            f"STATE file was collected from account {account_id!r} but this engine "
            f"is connected to {expected_account!r}. Refusing to restore state across "
            "accounts."
        )
        if not allow_account_mismatch:
            raise ValueError(message)
        logger.warning(f"{message} Proceeding because the mismatch was allowed.")

    plugin_dir = plugin_dir_path(plugin_dir)
    store_path = Path(plugin_store) if plugin_store else plugin_store_path_for(account_id)

    # Re-anchor registry class_paths from the source machine's roots to ours.
    # source.plugin_dir is tried before source.repo_root: the plugin dir is
    # normally inside the repo, and the more specific mapping must win when the
    # target overrides IB_PLUGIN_DIR to somewhere outside it.
    source = snapshot.get("source", {})
    mappings = []
    if source.get("plugin_dir"):
        mappings.append((source["plugin_dir"], str(plugin_dir)))
    if source.get("repo_root"):
        mappings.append((source["repo_root"], str(repo_root())))

    registry = []
    for entry in snapshot.get("plugin_registry", []):
        entry = dict(entry)
        original = entry.get("class_path", "")
        rewritten = _reanchor(original, mappings)
        if rewritten != original:
            entry["class_path"] = rewritten
            report["class_path_rewritten"].append(
                {"slot": entry["slot"], "from": original, "to": rewritten})
        if not Path(entry.get("class_path", "")).is_file():
            report["class_path_missing"].append(
                {"slot": entry["slot"], "class_path": entry.get("class_path", "")})
        registry.append(entry)

    # Plugin registry: the snapshot wins for every slot it names.
    from .plugin_store import PluginStore

    if not dry_run:
        # The target may be a bare machine where nothing has created this path yet.
        store_path.parent.mkdir(parents=True, exist_ok=True)
        store = PluginStore(store_path)
        existing = {entry["slot"] for entry in store.list_registry()}
        for entry in registry:
            ok = store.upsert_registry(
                entry["slot"],
                entry["class_path"],
                entry.get("version", ""),
                entry.get("status", "unloaded"),
                entry.get("config"),
            )
            (report["registry_restored"] if ok else report["registry_failed"]).append(entry["slot"])
    else:
        store = PluginStore(store_path) if store_path.is_file() else None
        existing = {e["slot"] for e in store.list_registry()} if store else set()
        report["registry_restored"] = [e["slot"] for e in registry]

    # Slots present on this machine but absent from the snapshot are left alone —
    # deleting registry rows is destructive and not something a restore should do
    # silently. They are reported instead, because any of them in 'started' status
    # will be auto-reloaded and will trade alongside the restored set.
    snapshot_slots = {entry["slot"] for entry in registry}
    report["registry_extra"] = sorted(existing - snapshot_slots)

    # Per-plugin runtime files.
    for entry in snapshot.get("plugin_files", []):
        target = plugin_dir / entry["path"]
        try:
            if not dry_run:
                target.parent.mkdir(parents=True, exist_ok=True)
                if "json" in entry:
                    target.write_text(json.dumps(entry["json"], indent=2, default=str))
                else:
                    target.write_text(entry.get("text", ""))
            report["files_restored"].append(entry["path"])
        except Exception as e:
            logger.error(f"Failed to restore {target}: {e}")
            report["files_failed"].append(entry["path"])

    forex = snapshot.get("forex_cost_basis")
    if forex is not None:
        try:
            if not dry_run:
                if isinstance(forex, str):
                    FOREX_COST_BASIS_PATH.write_text(forex)
                else:
                    FOREX_COST_BASIS_PATH.write_text(json.dumps(forex, indent=2, default=str))
            report["forex_cost_basis_restored"] = True
        except Exception as e:
            logger.error(f"Failed to restore forex cost basis: {e}")

    logger.info(
        f"Restored STATE for {account_id}: {len(report['registry_restored'])} registry "
        f"entries, {len(report['files_restored'])} plugin files"
        + (" (dry run)" if dry_run else "")
    )
    return report


def format_restore_report(report: Dict[str, Any]) -> str:
    """Human-readable restore summary for logs and ibctl output."""
    lines = [
        f"STATE restore{' (dry run)' if report.get('dry_run') else ''} "
        f"for account {report.get('account_id')}",
        f"  registry entries restored : {len(report.get('registry_restored', []))}",
        f"  plugin files restored     : {len(report.get('files_restored', []))}",
    ]
    if report.get("forex_cost_basis_restored"):
        lines.append("  forex cost basis          : restored")
    for item in report.get("class_path_rewritten", []):
        lines.append(
            f"  class_path re-anchored    : {item['slot']}: {item['from']} -> {item['to']}")
    for item in report.get("class_path_missing", []):
        lines.append(
            f"  MISSING class_path        : {item['slot']}: {item['class_path']} — "
            "this slot will be SKIPPED at engine start; fix the path or place "
            "the repo image before starting")
    if report.get("registry_failed"):
        lines.append(
            f"  REGISTRY FAILED           : {', '.join(report['registry_failed'])}")
    if report.get("files_failed"):
        lines.append(f"  FAILED files              : {', '.join(report['files_failed'])}")
    if report.get("registry_extra"):
        lines.append(
            "  slots on this machine but not in the STATE file (left untouched — "
            "any in 'started' status will also auto-reload and trade): "
            + ", ".join(report["registry_extra"])
        )
    for item in report.get("not_carried", []):
        if item.get("exists"):
            lines.append(f"  not carried: {item['path']} ({item['reason']})")
    return "\n".join(lines)
