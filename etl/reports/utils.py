"""Utility functions for report generation."""

import importlib


def weasyprint_available() -> bool:
    """Check if WeasyPrint and its dependencies are available."""
    try:
        importlib.import_module("weasyprint")
    except Exception:
        return False
    else:
        return True
