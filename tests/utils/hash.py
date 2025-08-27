"""HTML hashing utilities for deterministic report testing."""

import hashlib
import re


def canonicalize_html(html: str) -> str:
    """Canonicalize HTML for stable hashing.

    Args:
        html: Raw HTML string

    Returns:
        Normalized HTML string with stable whitespace
    """
    # Remove extra whitespace between tags
    html = re.sub(r">\s+<", "><", html.strip())

    # Normalize internal whitespace to single spaces
    html = re.sub(r"\s+", " ", html)

    # Remove leading/trailing whitespace from lines
    lines = [line.strip() for line in html.split("\n") if line.strip()]

    return "\n".join(lines)


def hash_html(html: str) -> str:
    """Generate SHA256 hash of canonicalized HTML.

    Args:
        html: HTML string to hash

    Returns:
        Hexadecimal SHA256 hash
    """
    canonical = canonicalize_html(html)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
