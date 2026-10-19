"""Import per-ballot-pen (قلم) vote counts from the district *_data.xlsx files
into the election_pen_votes table.

Candidate ids come from import_from_excel.load_district(), so they always match
election_members/election_votes. Only this table is touched; run it after
import_from_excel.py. Usage:

    PYTHONIOENCODING=utf-8 python migrations/import_pen_votes.py [--dry-run]
"""
import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from import_from_excel import (  # noqa: E402
    BASE_DIR, DATABASE_URL, find_file, load_district, normalize_district_code, std_cols,
)

DDL = """
CREATE TABLE IF NOT EXISTS election_pen_votes (
    id BIGSERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    district_code TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    ballot_pen TEXT NOT NULL,
    neighborhood TEXT,
    voting_place TEXT,
    votes INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pen_votes_cand ON election_pen_votes (year, district_code, candidate_id);
CREATE INDEX IF NOT EXISTS idx_pen_votes_pen ON election_pen_votes (year, district_code, ballot_pen);
"""


def _clean(series):
    return series.astype(str).str.replace(r"\s+", " ", regex=True).str.strip()


def pen_rows(year: str, district: str, folder: str):
    data, error = load_district(year, district, folder)
    if error:
        return None, error
    raw = std_cols(pd.read_excel(find_file(folder, district, "_data")))
    if "BALLOT PEN" not in raw.columns:
        return None, "no BALLOT PEN column"

    raw["MEMBER"] = _clean(raw["MEMBER"])
    raw["GROUP"] = _clean(raw["GROUP"])
    raw["VOTES"] = pd.to_numeric(raw["VOTES"], errors="coerce").fillna(0).astype(int)
    raw["BALLOT PEN"] = _clean(raw["BALLOT PEN"])
    for col in ("NEIGHBORHOOD", "VOTING PLACE"):
        raw[col] = _clean(raw[col]) if col in raw.columns else ""

    # Some source rows (South3/2022) have the town and the polling-place name
    # in each other's column. The pen name always starts with the town, so
    # when it starts with VOTING PLACE and not with NEIGHBORHOOD, swap them.
    swapped = [
        bool(vp) and vp != "nan" and pen.startswith(vp) and not pen.startswith(nb)
        for pen, nb, vp in zip(raw["BALLOT PEN"], raw["NEIGHBORHOOD"], raw["VOTING PLACE"])
    ]
    raw.loc[swapped, ["NEIGHBORHOOD", "VOTING PLACE"]] = raw.loc[swapped, ["VOTING PLACE", "NEIGHBORHOOD"]].values

    ids = data["votes"][["MEMBER", "GROUP", "candidate_id"]].drop_duplicates(["MEMBER", "GROUP"])
    merged = raw.merge(ids, on=["MEMBER", "GROUP"], how="left")
    merged = merged[merged["candidate_id"].notna() & (merged["VOTES"] > 0)]
    agg = merged.groupby(
        ["candidate_id", "BALLOT PEN", "NEIGHBORHOOD", "VOTING PLACE"], as_index=False
    )["VOTES"].sum()
    return agg, None


def main(dry_run: bool):
    engine = create_engine(DATABASE_URL, future=True)
    if not dry_run:
        with engine.begin() as conn:
            conn.execute(text(DDL))

    for year in ("2018", "2022"):
        year_dir = os.path.join(BASE_DIR, year)
        for raw_district in sorted(os.listdir(year_dir)):
            folder = os.path.join(year_dir, raw_district)
            if not os.path.isdir(folder) or raw_district.lower() == "results":
                continue
            district = normalize_district_code(raw_district)
            agg, error = pen_rows(year, district, folder)
            if error:
                print(f"{year} {district}: SKIPPED ({error})")
                continue
            if dry_run:
                print(f"{year} {district}: OK (dry run) rows={len(agg)} pens={agg['BALLOT PEN'].nunique()}")
                continue
            payload = [
                {"year": int(year), "d": district, "cid": r["candidate_id"], "pen": r["BALLOT PEN"],
                 "nb": r["NEIGHBORHOOD"], "vp": r["VOTING PLACE"], "votes": int(r["VOTES"])}
                for _, r in agg.iterrows()
            ]
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM election_pen_votes WHERE year = :year AND district_code = :d"),
                    {"year": int(year), "d": district},
                )
                conn.execute(text("""
                    INSERT INTO election_pen_votes
                        (year, district_code, candidate_id, ballot_pen, neighborhood, voting_place, votes)
                    VALUES (:year, :d, :cid, :pen, :nb, :vp, :votes)
                """), payload)
            print(f"{year} {district}: IMPORTED rows={len(payload)} pens={agg['BALLOT PEN'].nunique()}")


if __name__ == "__main__":
    main("--dry-run" in sys.argv)
