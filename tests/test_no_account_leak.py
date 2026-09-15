"""
Guard: no real IB account number may be committed.

Account ids are personal identifiers. They leak easily — a copied log line, a
test written against whatever the live gateway happened to report, a worked
example in a README. On 2026-09-14 eighteen occurrences of a live account
reached tests/ that way, alongside six older ones already in the tree.

Rather than name the real ids (which would itself commit them), this asserts
that every account-shaped token in tracked files is a recognisable placeholder.
Anything new fails, and the fix is to use a placeholder — not to extend the
list with a real id.
"""
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

# IB account ids: DU/DUP prefix for paper, U for live, 6-9 digits.
ACCOUNT_RE = re.compile(r"\b(?:DUP|DU|U)[0-9]{6,9}\b")

# Obvious fakes only: repeated digits, or a plain ascending/descending run.
PLACEHOLDERS = {
    "U123456", "U1234567", "U7654321", "U9876543", "U8765432", "U9999999",
    "DU123456", "DU789012", "DU1234567", "DU9999999", "DU999999",
    "DU0000001", "DU1111111", "DUP1234567",
}

SKIP_SUFFIXES = {".db", ".png", ".jpg", ".gz", ".zip", ".pyc", ".ipynb"}


def _tracked_text_files():
    out = subprocess.run(["git", "ls-files", "-z"], cwd=REPO,
                         capture_output=True, text=True, check=True).stdout
    for rel in filter(None, out.split("\0")):
        p = REPO / rel
        if p.suffix.lower() in SKIP_SUFFIXES or not p.is_file():
            continue
        yield rel, p


def test_no_unrecognised_account_ids_are_committed():
    found = {}
    for rel, path in _tracked_text_files():
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for tok in ACCOUNT_RE.findall(text):
            if tok not in PLACEHOLDERS:
                found.setdefault(tok, []).append(rel)

    assert not found, (
        "account-shaped tokens that are not recognised placeholders:\n"
        + "\n".join(f"  {tok}: {sorted(set(files))}" for tok, files in found.items())
        + "\n\nIf this is a real account id, replace it with a placeholder. "
          "Do NOT add it to PLACEHOLDERS."
    )


def test_the_guard_actually_matches_account_ids():
    """A regex that matched nothing would pass the test above vacuously."""
    assert ACCOUNT_RE.findall("account U9876543 and DU1234567 here") == \
        ["U9876543", "DU1234567"]
    assert ACCOUNT_RE.findall("no ids here, port 4001, 20260914") == []


def test_placeholders_are_all_account_shaped():
    """A typo in PLACEHOLDERS would silently widen the allowlist."""
    for tok in PLACEHOLDERS:
        assert ACCOUNT_RE.fullmatch(tok), f"{tok!r} is not an account-shaped token"
