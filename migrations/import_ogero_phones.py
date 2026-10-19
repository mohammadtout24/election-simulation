"""Add Ogero phone numbers to uniquely named voters; see docs/voter-phones.md."""
import argparse
from pathlib import Path
import re
import sys
import unicodedata

import pandas as pd
from sqlalchemy import create_engine, inspect, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from arabic_utils import normalize_arabic  # noqa: E402
from migrations.import_from_excel import DATABASE_URL  # noqa: E402

_SPELLING_VARIANTS = str.maketrans({"ة": "ه", "ؤ": "و", "ئ": "ي"})


def normalize_name(value):
    """Fold Arabic keyboard/rendering variants while retaining word boundaries."""
    name = normalize_arabic(unicodedata.normalize("NFKC", str(value or "")))
    name = re.sub(r"[\u0610-\u061a\u064b-\u065fـ\u0670\u200b-\u200f\u202a-\u202e\u2066-\u2069\ufeff]", "", name)
    return " ".join(name.split()).casefold()


def variant_name_key(value):
    """Fallback key only: spelling/spacing variants need global uniqueness."""
    return normalize_name(value).translate(_SPELLING_VARIANTS).replace(" ", "")


def normalize_phone(value):
    """Keep numbers as text and restore the zero lost in numeric Excel cells."""
    number = unicodedata.normalize("NFKC", str(value or "").strip())
    number = "".join(str(unicodedata.decimal(c)) if c.isdecimal() else c for c in number)
    if number.startswith("-"):
        return ""
    number = re.sub(r"\.0$", "", number)
    number = re.sub(r"[\s().-]", "", number)
    if number.startswith("+961"):
        number = number[4:]
    elif number.startswith("00961"):
        number = number[5:]
    elif number.startswith("961") and len(number) in (10, 11):
        number = number[3:]
    if re.fullmatch(r"[1-9][0-9]{6}", number):
        return "0" + number
    if re.fullmatch(r"[0-9]{8}", number) and int(number):
        return number
    return ""


def build_phonebook(rows):
    phonebook = {}
    stats = {"source_rows": 0, "invalid_rows": 0}
    for name, phone in rows:
        stats["source_rows"] += 1
        key, number = normalize_name(name), normalize_phone(phone)
        if not key or not number:
            stats["invalid_rows"] += 1
            continue
        phonebook.setdefault(key, set()).add(number)
    return phonebook, stats


def load_phonebook(path):
    df = pd.read_excel(path, dtype=str, keep_default_na=False)
    df.columns = [str(c).strip().casefold() for c in df.columns]
    if not {"name", "phone"}.issubset(df.columns):
        raise ValueError("The workbook must have name and phone columns.")
    phonebook, stats = build_phonebook(df[["name", "phone"]].itertuples(index=False, name=None))
    if not phonebook:
        raise ValueError("The workbook has no valid name/phone records.")
    return phonebook, stats


def match_voters(voters, phonebook):
    # Keep exact and fallback candidates separately. Count fallback collisions
    # across the entire registry, including voters with other exact spellings.
    exact_candidates, variant_candidates = {}, {}
    variant_keys = {variant_name_key(name) for name in phonebook}
    stats = {"voters_scanned": 0, "exact_name_matches": 0,
             "variant_name_matches": 0, "ambiguous_names": 0, "unmatched_names": 0}
    for voter in voters:
        stats["voters_scanned"] += 1
        parts = [normalize_name(voter.get(c)) for c in ("first_name", "father_name", "family_name")]
        if not all(parts):
            continue
        key = " ".join(parts)
        candidate = (voter["id"], voter.get("phone_number") or "")
        if key in phonebook:
            exact_candidates[key] = None if key in exact_candidates else candidate
        variant = variant_name_key(key)
        if variant in variant_keys:
            variant_candidates[variant] = None if variant in variant_candidates else candidate

    matched = {}
    for key, phones in phonebook.items():
        if key in exact_candidates:
            candidate = exact_candidates[key]
            match_type = "exact_name_matches"
        else:
            variant = variant_name_key(key)
            if variant not in variant_candidates:
                stats["unmatched_names"] += 1
                continue
            candidate = variant_candidates[variant]
            match_type = "variant_name_matches"
        if candidate is None:
            stats["ambiguous_names"] += 1
            continue
        voter_id, existing = candidate
        stats[match_type] += 1
        if voter_id not in matched:
            matched[voter_id] = (existing, {p.strip() for p in existing.split(",") if p.strip()})
        matched[voter_id][1].update(phones)

    updates = []
    for voter_id, (existing, numbers) in matched.items():
        value = ", ".join(sorted(numbers))
        if value != existing:
            updates.append({"voter_id": voter_id, "phone_number": value})
    stats.update(matched_voters=len(matched), updates=len(updates))
    return updates, stats


def import_phonebook(engine, phonebook, dry_run=False):
    with engine.begin() as conn:
        if not dry_run and conn.dialect.name == "postgresql":
            # Keep the uniqueness check and updates consistent with concurrent
            # registry imports. Ordinary voter-table reads remain available.
            conn.execute(text("LOCK TABLE voters_2026 IN SHARE ROW EXCLUSIVE MODE"))
        columns = {c["name"] for c in inspect(conn).get_columns("voters_2026")}
        phone_column = "phone_number" if "phone_number" in columns else "NULL AS phone_number"
        stmt = text(f"""
            SELECT id, first_name, father_name, family_name, {phone_column}
            FROM voters_2026
        """).execution_options(yield_per=10000)
        with conn.execute(stmt) as result:
            updates, stats = match_voters(result.mappings(), phonebook)
        if not dry_run:
            if "phone_number" not in columns:
                conn.execute(text("ALTER TABLE voters_2026 ADD COLUMN phone_number TEXT"))
            for start in range(0, len(updates), 1000):
                conn.execute(text("""
                    UPDATE voters_2026 SET phone_number = :phone_number
                    WHERE id = :voter_id
                """), updates[start:start + 1000])
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workbook", type=Path, help="Path to Ogero numbers.xlsx")
    parser.add_argument("--dry-run", action="store_true", help="Preview counts without changing the database")
    args = parser.parse_args()
    phonebook, source_stats = load_phonebook(args.workbook)
    engine = create_engine(DATABASE_URL, future=True)
    try:
        stats = import_phonebook(engine, phonebook, dry_run=args.dry_run)
    finally:
        engine.dispose()
    print(f"Source rows: {source_stats['source_rows']:,}; invalid rows skipped: {source_stats['invalid_rows']:,}")
    print(f"Voters scanned: {stats['voters_scanned']:,}; unique full-name matches: {stats['matched_voters']:,}")
    print(f"Exact names matched: {stats['exact_name_matches']:,}; Arabic spelling/spacing variants matched: {stats['variant_name_matches']:,}")
    print(f"Ambiguous names skipped: {stats['ambiguous_names']:,}; unmatched names: {stats['unmatched_names']:,}")
    action = "Would update" if args.dry_run else "Updated"
    print(f"{action} {stats['updates']:,} voters.")


if __name__ == "__main__":
    main()
