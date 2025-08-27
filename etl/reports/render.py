"""Stub implementation for report rendering - will raise NotImplementedError until GREEN phase."""

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


def render_balance_sheet(period: str, engine: "Engine") -> str:
    """Generate deterministic HTML for balance sheet.
    
    Args:
        period: Period string (e.g., "2024Q1") 
        engine: Database engine
        
    Returns:
        HTML string with deterministic formatting
        
    Raises:
        NotImplementedError: Until GREEN phase implementation
    """
    raise NotImplementedError("render_balance_sheet not implemented yet")


def render_cash_flow(period: str, engine: "Engine") -> str:
    """Generate deterministic HTML for cash flow statement.
    
    Args:
        period: Period string (e.g., "2024Q1")
        engine: Database engine
        
    Returns:
        HTML string with deterministic formatting
        
    Raises:
        NotImplementedError: Until GREEN phase implementation
    """
    raise NotImplementedError("render_cash_flow not implemented yet")


def write_pdf(html: str, out_path: Path) -> Path:
    """Convert HTML to PDF using WeasyPrint.
    
    Args:
        html: HTML string to convert
        out_path: Output PDF path
        
    Returns:
        Path to created PDF file
        
    Raises:
        NotImplementedError: Until GREEN phase implementation
    """
    raise NotImplementedError("write_pdf not implemented yet")