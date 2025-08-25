# etl/transform.py
from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

import yaml

# Transform version per ADR (§4) — INT, not string
TRANSFORM_VERSION = 1


@lru_cache(maxsize=1)
def _load_coa_mapping() -> dict[str, Any]:
    """Load Chart of Accounts mapping from YAML file (cached)."""
    coa_path = Path(__file__).parent / "coa.yaml"
    result = yaml.safe_load(coa_path.read_text(encoding="utf-8"))
    return cast(dict[str, Any], result)


def _get_cash_account(account_type: str, account_subtype: str) -> str:
    coa = _load_coa_mapping()
    account_mappings = cast(dict[str, Any], coa["account_mappings"])
    if (
        account_type not in account_mappings
        or account_subtype not in account_mappings[account_type]
    ):
        msg = f"Unmapped Plaid account type/subtype: {account_type}/{account_subtype}"
        raise ValueError(msg)
    return cast(str, account_mappings[account_type][account_subtype])


def _get_expense_account(categories: list[str] | None) -> str:
    coa = _load_coa_mapping()
    category_mappings = cast(dict[str, Any], coa["category_mappings"])
    if not categories:
        return cast(str, category_mappings["default"])
    main = categories[0]
    sub = categories[1] if len(categories) > 1 else None
    if main in category_mappings:
        mapping = category_mappings[main]
        if isinstance(mapping, dict) and sub and sub in mapping:
            return cast(str, mapping[sub])
        if isinstance(mapping, str):
            return mapping
    return cast(str, category_mappings["default"])


def _get_income_account(categories: list[str] | None) -> str:
    """Use Deposit submapping when present; otherwise fallback."""
    coa = _load_coa_mapping()
    cm = cast(dict[str, Any], coa["category_mappings"])
    if not categories:
        return "Income:Miscellaneous"
    main = categories[0]
    sub = categories[1] if len(categories) > 1 else None
    if main == "Deposit":
        mapping = cm.get("Deposit", {})
        if isinstance(mapping, dict) and sub and sub in mapping:
            return cast(str, mapping[sub])
        # Default income bucket for deposits if subcategory unknown
        return "Income:Miscellaneous"
    return "Income:Miscellaneous"


def _compute_source_hash(raw_json: dict[str, Any]) -> str:
    canonical = json.dumps(raw_json, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def map_plaid_to_journal(
    plaid_txns: list[dict[str, Any]],
    accounts: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    """Transform Plaid transactions to double-entry journal entries."""
    entries: list[dict[str, Any]] = []

    for txn in plaid_txns:
        # Determinism: skip pending
        if txn.get("pending", False):
            continue

        account_id = txn["account_id"]
        if account_id not in accounts:
            msg = f"Account {account_id} not found in accounts metadata"
            raise ValueError(msg)

        acc = accounts[account_id]
        acc_type = acc["type"]
        acc_subtype = acc["subtype"]
        currency = acc["currency"]

        cash_account = _get_cash_account(acc_type, acc_subtype)

        # Decimal precision
        amt_dec = Decimal(str(txn["amount"]))
        categories = txn.get("category") or []

        # Direction: treat amount as magnitude; inflow if Deposit category or negative
        is_inflow = ("Deposit" in categories) or (amt_dec < 0)
        magnitude = abs(amt_dec)

        txn_date = date.fromisoformat(txn["date"])
        source_hash = _compute_source_hash(txn)

        if acc_type == "depository":
            if is_inflow:
                income_account = _get_income_account(categories)
                lines = [
                    {"account": cash_account, "side": "debit", "amount": magnitude},
                    {"account": income_account, "side": "credit", "amount": magnitude},
                ]
            else:
                expense_account = _get_expense_account(categories)
                lines = [
                    {"account": expense_account, "side": "debit", "amount": magnitude},
                    {"account": cash_account, "side": "credit", "amount": magnitude},
                ]
        else:
            # Keep MVP narrow; expand in later milestones
            msg = f"Unmapped Plaid account type/subtype: {acc_type}/{acc_subtype}"
            raise ValueError(msg)

        entries.append({
            "txn_id": txn["transaction_id"],
            "txn_date": txn_date,
            "description": txn.get("name") or txn.get("merchant_name") or "",
            "currency": currency,
            "source_hash": source_hash,
            "transform_version": TRANSFORM_VERSION,
            "lines": lines,
        })

    return entries


def sort_deterministically(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort by (txn_date, txn_id)."""
    return sorted(entries, key=lambda e: (e["txn_date"], e["txn_id"]))
