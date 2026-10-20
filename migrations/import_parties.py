"""Import each candidate's political party into election_members.party (2018 and 2022).

Source: the "Political Party" column of <District>_members_final.xlsx (falls
back to <District>_members.xlsx). Candidates are matched to the database by
normalized name (and list, when a name is repeated). "Not Found"/blank values
are stored as NULL. Only the party column is written. Usage:

    PYTHONIOENCODING=utf-8 python migrations/import_parties.py [--dry-run] [2018|2022]
"""
import os
import re
import sys

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from import_from_excel import BASE_DIR, DATABASE_URL, canon_group, find_file, normalize_district_code  # noqa: E402
from arabic_utils import normalize_arabic  # noqa: E402

YEARS = ("2018", "2022")
EMPTY = {"", "nan", "none", "not found", "غير محدد", "-"}


def clean(value) -> str:
    return re.sub(r"\s+", " ", str(value if value is not None else "")).strip()


def name_key(value) -> str:
    return normalize_arabic(clean(value)).replace(" ", "")


def load_parties(folder: str, district: str):
    path = find_file(folder, district, "_members_final") or find_file(folder, district, "_members")
    if not path:
        return None, None
    df = pd.read_excel(path)
    df.columns = [str(c).strip().upper() for c in df.columns]
    if "POLITICAL PARTY" not in df.columns or "MEMBER" not in df.columns:
        return None, path
    rows = []
    for _, r in df.iterrows():
        party = clean(r["POLITICAL PARTY"])
        if party.lower() in EMPTY:
            continue
        rows.append((name_key(r["MEMBER"]), canon_group(r.get("GROUP", "")), party))
    return rows, path


def main(dry_run: bool, years=YEARS):
    engine = create_engine(DATABASE_URL, future=True)
    if not dry_run:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE election_members ADD COLUMN IF NOT EXISTS party TEXT"))

    for year in years:
        import_year(engine, year, dry_run)


def import_year(engine, YEAR: str, dry_run: bool):
    year_dir = os.path.join(BASE_DIR, YEAR)
    total_set = total_cands = 0
    for raw in sorted(os.listdir(year_dir)):
        folder = os.path.join(year_dir, raw)
        if not os.path.isdir(folder) or raw.lower() == "results":
            continue
        district = normalize_district_code(raw)
        rows, path = load_parties(folder, district)
        if rows is None:
            print(f"{YEAR} {district}: SKIPPED (no party column in {path})")
            continue
        with engine.connect() as conn:
            members = conn.execute(text("""
                SELECT candidate_id, member, COALESCE(original_group_name, group_name) AS grp
                FROM election_members WHERE year = :y AND district_code = :d
            """), {"y": int(YEAR), "d": district}).all()

        by_name = {}
        for cid, member, grp in members:
            by_name.setdefault(name_key(member), []).append((cid, canon_group(grp)))

        updates, unmatched = {}, []
        for key, grp, party in rows:
            cands = by_name.get(key, [])
            if len(cands) == 1:
                updates[cands[0][0]] = party
            elif len(cands) > 1:
                same_list = [cid for cid, g in cands if g == grp]
                if len(same_list) == 1:
                    updates[same_list[0]] = party
                else:
                    unmatched.append(key)
            else:
                unmatched.append(key)

        if not dry_run:
            with engine.begin() as conn:
                conn.execute(text("UPDATE election_members SET party = NULL WHERE year = :y AND district_code = :d"),
                             {"y": int(YEAR), "d": district})
                if updates:
                    conn.execute(text("""
                        UPDATE election_members SET party = :p
                        WHERE year = :y AND district_code = :d AND candidate_id = :cid
                    """), [{"p": p, "y": int(YEAR), "d": district, "cid": cid} for cid, p in updates.items()])
        total_set += len(updates)
        total_cands += len(members)
        note = f" -- {len(unmatched)} file row(s) not matched to a candidate" if unmatched else ""
        print(f"{YEAR} {district}: {'OK (dry run)' if dry_run else 'IMPORTED'} "
              f"party set for {len(updates)} of {len(members)} candidates{note}")
    print(f"{YEAR} TOTAL: party known for {total_set} of {total_cands} candidates")


if __name__ == "__main__":
    chosen = tuple(a for a in sys.argv[1:] if a in YEARS) or YEARS
    main("--dry-run" in sys.argv, chosen)
