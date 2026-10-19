import re
from collections.abc import Iterable


def censor_text(text: str, banned_words: Iterable[str]) -> str:
    """Replace banned words with asterisks, ignoring letter case."""
    words = sorted(
        {word.strip() for word in banned_words if word.strip()},
        key=len,
        reverse=True,
    )
    if not words:
        return text

    # Lookarounds prevent a ban on "cat" from changing "concatenate".
    pattern = re.compile(
        rf"(?<!\w)({'|'.join(re.escape(word) for word in words)})(?!\w)",
        flags=re.IGNORECASE,
    )
    return pattern.sub(lambda match: "*" * len(match.group(0)), text)
