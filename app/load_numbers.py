def normalize_load_number(value: str | None) -> str | None:
    """Return the canonical identity used to assemble shipments."""
    if value is None:
        return None

    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("load_number cannot be empty")
    return normalized
