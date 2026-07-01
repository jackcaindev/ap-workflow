def strip_code_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        # Drop the opening fence line (handles ```json or bare ```)
        text = text.split("\n", 1)[1] if "\n" in text else ""
        if text.endswith("```"):
            text = text[: -3]
    return text.strip()
