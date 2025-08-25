from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal

import pytest

# Transform API under test:
# - map_plaid_to_journal(plaid_txns: list[dict], accounts: dict[account_id -> {type,subtype,currency,name}]) -> list[entry]
# - sort_deterministically(entries) -> list[entry]
from etl.transform import map_plaid_to_journal, sort_deterministically


def accounts_fixture():
    return {
        "acc_123": {"type": "depository", "subtype": "checking", "currency": "USD", "name": "Checking"},
        "acc_cc": {"type": "credit", "subtype": "credit card", "currency": "USD", "name": "Visa"},
    }


def test_every_entry_balances_double_entry() -> None:
    """Every journal entry must balance: sum(amount where side=debit) == sum(amount where side=credit)."""
    plaid_txns = [
        {
            "transaction_id": str(uuid.uuid4()),
            "account_id": "acc_123",
            "amount": 25.50,  # outflow from depository
            "date": "2024-01-15",
            "name": "Coffee Shop",
            "pending": False,
        },
        {
            "transaction_id": str(uuid.uuid4()),
            "account_id": "acc_123",
            "amount": -100.00,  # inflow to depository (e.g., salary) - negative in Plaid
            "date": "2024-01-16",
            "name": "Salary Deposit",
            "pending": False,
        },
    ]

    journal_entries = map_plaid_to_journal(plaid_txns, accounts_fixture())

    for entry in journal_entries:
        debits = sum(l["amount"] for l in entry["lines"] if l["side"] == "debit")
        credits = sum(l["amount"] for l in entry["lines"] if l["side"] == "credit")
        assert debits == credits, f"Entry {entry['txn_id']} unbalanced: {debits} != {credits}"


def test_stable_sorting_by_date_then_txn_id() -> None:
    """Entries sort deterministically by (posted_date, txn_id)."""
    plaid_txns = [
        {"transaction_id": "txn_b", "account_id": "acc_123", "amount": 10.00, "date": "2024-01-15", "name": "B", "pending": False},
        {"transaction_id": "txn_a", "account_id": "acc_123", "amount": 20.00, "date": "2024-01-15", "name": "A", "pending": False},
        {"transaction_id": "txn_c", "account_id": "acc_123", "amount": 30.00, "date": "2024-01-14", "name": "C", "pending": False},
    ]
    entries = map_plaid_to_journal(plaid_txns, accounts_fixture())
    sorted_entries = sort_deterministically(entries)

    expected_order = ["txn_c", "txn_a", "txn_b"]
    actual_order = [e["txn_id"] for e in sorted_entries]
    assert actual_order == expected_order


def test_mapping_covers_known_account_types_and_subtypes() -> None:
    """Transform must fail fast for unknown account type/subtype."""
    unknown_accounts = {"acc_unknown": {"type": "mystery", "subtype": "void", "currency": "USD", "name": "???"}}
    plaid_txns = [
        {"transaction_id": str(uuid.uuid4()), "account_id": "acc_unknown", "amount": 50.00, "date": "2024-01-15", "name": "X", "pending": False}
    ]
    with pytest.raises(ValueError, match="Unmapped Plaid account type/subtype"):
        map_plaid_to_journal(plaid_txns, unknown_accounts)


def test_pending_transactions_are_skipped() -> None:
    """Pending transactions are ignored for deterministic balances."""
    plaid_txns = [
        {"transaction_id": str(uuid.uuid4()), "account_id": "acc_123", "amount": 12.00, "date": "2024-01-15", "name": "Pending", "pending": True},
        {"transaction_id": str(uuid.uuid4()), "account_id": "acc_123", "amount": 8.00, "date": "2024-01-16", "name": "Posted", "pending": False},
    ]
    entries = map_plaid_to_journal(plaid_txns, accounts_fixture())
    # Only the posted one should appear
    assert all(e["txn_date"] == date(2024, 1, 16) for e in entries)
    assert len(entries) == 1


def test_amount_precision_preserved_as_decimal() -> None:
    """All monetary amounts should be Decimal in transform outputs."""
    plaid_txns = [
        {"transaction_id": str(uuid.uuid4()), "account_id": "acc_123", "amount": 12.345, "date": "2024-01-15", "name": "Precise", "pending": False}
    ]
    entries = map_plaid_to_journal(plaid_txns, accounts_fixture())
    for entry in entries:
        for line in entry["lines"]:
            assert isinstance(line["amount"], Decimal)


def test_date_conversion_to_date_type() -> None:
    """txn_date must be converted to date objects."""
    plaid_txns = [
        {"transaction_id": str(uuid.uuid4()), "account_id": "acc_123", "amount": 25.00, "date": "2024-01-15", "name": "Dated", "pending": False}
    ]
    entries = map_plaid_to_journal(plaid_txns, accounts_fixture())
    for entry in entries:
        assert isinstance(entry["txn_date"], date)
        assert entry["txn_date"] == date(2024, 1, 15)


def test_currency_propagated_from_account() -> None:
    """Currency must be persisted from account metadata per ADR §1."""
    accounts = {
        "acc_usd": {"type": "depository", "subtype": "checking", "currency": "USD", "name": "USD Checking"},
        "acc_cad": {"type": "depository", "subtype": "checking", "currency": "CAD", "name": "CAD Checking"},
    }
    t_usd = str(uuid.uuid4())
    t_cad = str(uuid.uuid4())
    plaid_txns = [
        {"transaction_id": t_usd, "account_id": "acc_usd", "amount": 25.00, "date": "2024-01-15", "name": "USD Txn", "pending": False},
        {"transaction_id": t_cad, "account_id": "acc_cad", "amount": 30.00, "date": "2024-01-16", "name": "CAD Txn", "pending": False},
    ]
    expected = {t_usd: "USD", t_cad: "CAD"}

    entries = map_plaid_to_journal(plaid_txns, accounts)

    for e in entries:
        assert e["currency"] == expected[e["txn_id"]]


def test_cash_line_direction_for_depository_accounts() -> None:
    """Cash line is credit on outflow (expense), debit on inflow (deposit)."""
    accs = {
        "acc_123": {"type": "depository", "subtype": "checking", "currency": "USD", "name": "Checking"},
    }
    txn_out = "txn_out"
    txn_in = "txn_in"
    plaid_txns = [
        # Outflow: typical purchase/expense
        {"transaction_id": txn_out, "account_id": "acc_123", "amount": 25.50, "date": "2024-01-15",
         "name": "Coffee", "category": ["Food and Drink"], "pending": False},
        # Inflow: salary/deposit (negative amount in Plaid for deposits)
        {"transaction_id": txn_in, "account_id": "acc_123", "amount": -100.00, "date": "2024-01-16",
         "name": "Salary", "category": ["Deposit"], "pending": False},
    ]

    entries = {e["txn_id"]: e for e in map_plaid_to_journal(plaid_txns, accs)}

    def cash_side(entry):
        cash_lines = [l for l in entry["lines"] if "checking" in l["account"].lower() or "cash" in l["account"].lower() or "bank" in l["account"].lower()]
        assert len(cash_lines) == 1, f"expected 1 cash line, got {cash_lines}"
        return cash_lines[0]["side"]

    assert cash_side(entries[txn_out]) == "credit"  # cash decreases on expense
    assert cash_side(entries[txn_in])  == "debit"   # cash increases on deposit