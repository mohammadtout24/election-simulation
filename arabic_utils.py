"""Shared Arabic text normalization.

Used two places: the Excel import script (to match candidates/votes/seats
across files that were typed inconsistently) and app.py's seat-allocation
matching (so DISTRICT/RELIGION comparisons stay correct even if a future
district/year has the same kind of spelling drift). Only folds together
characters that are pure input-method/rendering variants of the same
letter -- never touches letters that could change a word's meaning.
"""
import re
import unicodedata

# Farsi keyboard glyphs that are visually near-identical to their Arabic
# counterparts but are different Unicode codepoints: ی (U+06CC) vs ي
# (U+064A), ک (U+06A9) vs ك (U+0643).
_VARIANT_MAP = str.maketrans({
    "أ": "ا",  # أ -> ا
    "إ": "ا",  # إ -> ا
    "آ": "ا",  # آ -> ا
    "ٱ": "ا",  # ٱ -> ا
    "ی": "ي",  # ی -> ي
    "ى": "ي",  # ى -> ي
    "ک": "ك",  # ک -> ك
})


def normalize_arabic(value) -> str:
    # NFC first: some source files spell a letter+hamza as two combining
    # codepoints (e.g. base yeh + combining hamza above) instead of the
    # single precomposed character (e.g. yeh-with-hamza) -- same rendered
    # text, different bytes. NFC folds them to the same canonical form.
    text = unicodedata.normalize("NFC", str(value or ""))
    text = re.sub(r"\s+", " ", text.strip())
    return text.translate(_VARIANT_MAP)
