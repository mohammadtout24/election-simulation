"""Import the March 2026 voter registry (election 2026/*.xlsx) into voters_2026.

One file per electoral (sub-)district; each row is one registered voter. Data is
loaded with COPY, file by file, replacing any earlier rows of the same file so the
script can be re-run safely. Usage:

    PYTHONIOENCODING=utf-8 python migrations/import_voters_2026.py [--dry-run]
"""
import io
import os
import re
import sys
import unicodedata

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from import_from_excel import BASE_DIR, DATABASE_URL  # noqa: E402
from arabic_utils import normalize_arabic  # noqa: E402


def norm_place(value) -> str:
    """Normalize a place name for matching voters_2026.town against
    election_pen_votes.neighborhood -- the two come from different source
    files (2026 voter registry vs. 2022 ballot export) and disagree on
    presentation-form ligatures (e.g. ﻻ "لا") and spacing that
    normalize_arabic alone (NFC) doesn't fold; NFKC does."""
    text_ = unicodedata.normalize("NFKC", str(value or ""))
    text_ = normalize_arabic(text_)
    text_ = re.sub(r"[ً-ْـ]", "", text_)
    return re.sub(r"\s+", "", text_)

SRC_DIR = os.path.join(BASE_DIR, "election 2026")

COLUMN_MAP = {
    "الاسم": "first_name",
    "الشهرة": "family_name",
    "اسم الاب": "father_name",
    "اسم الام وشهرتها": "mother_name",
    "تاريخ الولادة": "birth_date",
    "المذهب الشخصي": "personal_sect",
    "رقم السجل": "registry_number",
    "الجنس": "gender",
    "مذهب السجل": "registry_sect",
    "البلدة أو الحي": "town",
    "القضاء": "qada",
    "المحافظة": "governorate",
    "الدائرة الانتخابية": "electoral_district",
    "البلد": "country",
}
DB_COLUMNS = list(COLUMN_MAP.values())

DDL = """
CREATE TABLE IF NOT EXISTS voters_2026 (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    first_name TEXT, family_name TEXT, father_name TEXT, mother_name TEXT,
    birth_date TEXT, personal_sect TEXT, registry_number TEXT, gender TEXT,
    registry_sect TEXT, town TEXT, town_norm TEXT, qada TEXT, governorate TEXT,
    electoral_district TEXT, country TEXT, phone_number TEXT
);
"""
INDEXES = """
CREATE INDEX IF NOT EXISTS idx_voters_2026_file ON voters_2026 (source_file);
CREATE INDEX IF NOT EXISTS idx_voters_2026_town ON voters_2026 (electoral_district, town);
CREATE INDEX IF NOT EXISTS idx_voters_2026_town_norm ON voters_2026 (town_norm);
"""


def load_file(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    missing = [c for c in COLUMN_MAP if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    df = df[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)
    for col in DB_COLUMNS:
        df[col] = df[col].fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    return df


def main(dry_run: bool):
    engine = create_engine(DATABASE_URL, future=True)
    if not dry_run:
        with engine.begin() as conn:
            conn.execute(text(DDL))
            conn.execute(text("ALTER TABLE voters_2026 ADD COLUMN IF NOT EXISTS town_norm TEXT"))
            conn.execute(text("ALTER TABLE voters_2026 ADD COLUMN IF NOT EXISTS phone_number TEXT"))

    files = sorted(f for f in os.listdir(SRC_DIR) if f.lower().endswith(".xlsx") and not f.startswith("~$"))
    total = 0
    for name in files:
        source = name[:-5]
        try:
            df = load_file(os.path.join(SRC_DIR, name))
        except Exception as exc:
            print(f"{source}: SKIPPED ({exc})", flush=True)
            continue
        if dry_run:
            print(f"{source}: OK (dry run) rows={len(df)}", flush=True)
            total += len(df)
            continue

        buf = io.StringIO()
        out = df.copy()
        out.insert(0, "source_file", source)
        out["town_norm"] = out["town"].map(norm_place)
        out.to_csv(buf, index=False, header=False)
        buf.seek(0)
        raw = engine.raw_connection()
        try:
            cur = raw.cursor()
            cur.execute("DELETE FROM voters_2026 WHERE source_file = %s", (source,))
            cur.copy_expert(
                f"COPY voters_2026 (source_file, {', '.join(DB_COLUMNS)}, town_norm) FROM STDIN WITH (FORMAT csv)", buf
            )
            raw.commit()
        except Exception:
            raw.rollback()
            raise
        finally:
            raw.close()
        total += len(df)
        print(f"{source}: IMPORTED rows={len(df)}", flush=True)

    if not dry_run:
        with engine.begin() as conn:
            conn.execute(text(INDEXES))
    print(f"TOTAL rows={total}", flush=True)


if __name__ == "__main__":
    main("--dry-run" in sys.argv)
