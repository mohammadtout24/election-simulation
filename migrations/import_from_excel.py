"""One-time (re-runnable) import: rebuild election_seats / election_members /
election_votes for every district-year folder under 2018/ and 2022/ directly
from the source Excel files.

Run from the project root:  python migrations/import_from_excel.py

Safe to re-run: each district-year is fully replaced (DELETE + INSERT) inside
its own transaction, so a partial failure on one district does not corrupt
another. Every candidate gets a freshly generated, stable candidate_id, which
finally gives every row a real unique identity -- including the two
district/year combos that already contain two different candidates who
happen to share the exact same name.
"""
import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from arabic_utils import normalize_arabic

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:lims@localhost:5432/election_db",
)


def find_file(folder: str, district: str, suffix: str):
    """Case-insensitive lookup of "<district><suffix>.xlsx", preferring the
    plain file over any "_final" variant (confirmed against the live DB:
    Beirut1/2022 has both, and the database matches the plain file's 39
    candidates exactly, not the 30-row "_final" file)."""
    target = f"{district}{suffix}.xlsx".lower()
    for name in os.listdir(folder):
        if name.lower() == target:
            return os.path.join(folder, name)
    return None


def std_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def load_district(year: str, district: str, folder: str):
    seats_path = find_file(folder, district, "_seats")
    members_path = find_file(folder, district, "_members")
    data_path = find_file(folder, district, "_data")
    if not (seats_path and members_path and data_path):
        return None, f"missing file(s): seats={seats_path}, members={members_path}, data={data_path}"

    seats_df = std_cols(pd.read_excel(seats_path))
    members_df = std_cols(pd.read_excel(members_path))
    data_df = std_cols(pd.read_excel(data_path))

    for col in ("RELIGION", "REGION"):
        if col not in seats_df.columns:
            return None, f"{seats_path} missing column {col}"
    for col in ("MEMBER", "RELIGION", "REGION", "GROUP"):
        if col not in members_df.columns:
            return None, f"{members_path} missing column {col}"
    for col in ("MEMBER", "GROUP", "VOTES"):
        if col not in data_df.columns:
            return None, f"{data_path} missing column {col}"

    seats_df["RELIGION"] = seats_df["RELIGION"].astype(str).str.strip()
    seats_df["REGION"] = seats_df["REGION"].astype(str).str.strip()

    members_df["MEMBER"] = members_df["MEMBER"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    members_df["RELIGION"] = members_df["RELIGION"].astype(str).str.strip()
    members_df["REGION"] = members_df["REGION"].astype(str).str.strip()
    members_df["GROUP"] = members_df["GROUP"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    members_df = members_df[members_df["MEMBER"] != ""].reset_index(drop=True)
    members_df["candidate_id"] = [f"{year}:{district}:{i+1}" for i in range(len(members_df))]

    data_df["MEMBER"] = data_df["MEMBER"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    data_df["GROUP"] = data_df["GROUP"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    data_df["VOTES"] = pd.to_numeric(data_df["VOTES"], errors="coerce").fillna(0).astype(int)

    # "List only" ballots (a valid preferential-vote category under Lebanese
    # electoral law: voting for a list without marking a personal preference)
    # are not attributed to any individual candidate anywhere in this app.
    # Keep them as unmatched/candidate_id-NULL rows rather than warning about
    # them -- that already matches how the current database stores them.
    is_list_only = data_df["MEMBER"].str.startswith("لائحة فقط")
    data_df["_is_list_only"] = is_list_only

    votes_agg = data_df.groupby(["MEMBER", "GROUP", "_is_list_only"], as_index=False)["VOTES"].sum()

    # Tier 1: exact (name, list) match -- disambiguates the handful of real
    # same-name-different-list candidates (e.g. Mount4/2018, North2/2018).
    exact = votes_agg.merge(
        members_df[["MEMBER", "GROUP", "candidate_id"]], on=["MEMBER", "GROUP"], how="left"
    )
    name_counts = members_df["MEMBER"].value_counts()
    unique_names = set(name_counts[name_counts == 1].index)
    name_to_id = dict(zip(members_df["MEMBER"], members_df["candidate_id"]))

    # Tier 2: name-only match when the (name, list) pair doesn't line up
    # exactly but the raw name alone is unambiguous in the members list.
    # Tier 3: same, but comparing normalized names -- catches Farsi-keyboard
    # letter variants (ی/ي, ک/ك) and hamza variants (أ/إ/آ/ا) that are typed
    # inconsistently between the members file and the raw votes export.
    norm_counts = members_df["MEMBER"].apply(normalize_arabic).value_counts()
    unique_norms = set(norm_counts[norm_counts == 1].index)
    norm_to_id = dict(zip(members_df["MEMBER"].apply(normalize_arabic), members_df["candidate_id"]))

    unmatched_mask = exact["candidate_id"].isna()
    fallback_ids, still_unmatched = [], 0
    for _, row in exact[unmatched_mask].iterrows():
        norm_name = normalize_arabic(row["MEMBER"])
        if row["_is_list_only"]:
            fallback_ids.append(None)
        elif row["MEMBER"] in unique_names:
            fallback_ids.append(name_to_id[row["MEMBER"]])
        elif norm_name in unique_norms:
            fallback_ids.append(norm_to_id[norm_name])
        else:
            fallback_ids.append(None)
            still_unmatched += 1
    exact.loc[unmatched_mask, "candidate_id"] = fallback_ids

    # Keep every row (including candidate_id-NULL "list only" / genuinely
    # unresolved rows) so total recorded votes are preserved for the record,
    # matching how the live database already stores them; they simply never
    # join to a candidate in get_candidates_df().
    votes_merged = exact.drop(columns=["_is_list_only"])

    warning = None
    if still_unmatched:
        bad = exact[unmatched_mask & ~exact["_is_list_only"] & exact["candidate_id"].isna()]
        bad_names = ", ".join(bad["MEMBER"].head(5).tolist())
        bad_votes = int(bad["VOTES"].sum())
        warning = f"{still_unmatched} real vote row(s) ({bad_votes} votes) unmatched, e.g. {bad_names}"

    return {
        "seats": seats_df[["RELIGION", "REGION"]],
        "members": members_df,
        "votes": votes_merged,
        "warning": warning,
    }, None


def import_all(dry_run: bool = False):
    engine = create_engine(DATABASE_URL, future=True)
    results = []

    for year in ("2018", "2022"):
        year_dir = os.path.join(BASE_DIR, year)
        for district in sorted(os.listdir(year_dir)):
            folder = os.path.join(year_dir, district)
            if not os.path.isdir(folder) or district.lower() == "results":
                continue

            data, error = load_district(year, district, folder)
            if error:
                results.append((year, district, "SKIPPED", error))
                continue

            seats_df, members_df, votes_df = data["seats"], data["members"], data["votes"]
            detail_suffix = f" -- WARNING: {data['warning']}" if data["warning"] else ""

            if dry_run:
                results.append((
                    year, district, "OK (dry run)",
                    f"seats={len(seats_df)} members={len(members_df)} votes_rows={len(votes_df)}{detail_suffix}",
                ))
                continue

            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM election_seats WHERE year = :year AND district_code = :d"),
                    {"year": int(year), "d": district},
                )
                conn.execute(
                    text("DELETE FROM election_votes WHERE year = :year AND district_code = :d"),
                    {"year": int(year), "d": district},
                )
                conn.execute(
                    text("DELETE FROM election_members WHERE year = :year AND district_code = :d"),
                    {"year": int(year), "d": district},
                )

                conn.execute(
                    text("""
                        INSERT INTO election_seats (year, district_code, district, religion)
                        VALUES (:year, :d, :district, :religion)
                    """),
                    [
                        {"year": int(year), "d": district, "district": r["REGION"], "religion": r["RELIGION"]}
                        for _, r in seats_df.iterrows()
                    ],
                )

                conn.execute(
                    text("""
                        INSERT INTO election_members
                            (year, district_code, candidate_id, member, group_name, original_group_name, religion, district)
                        VALUES (:year, :d, :candidate_id, :member, :group_name, :group_name, :religion, :district)
                    """),
                    [
                        {
                            "year": int(year), "d": district,
                            "candidate_id": r["candidate_id"], "member": r["MEMBER"],
                            "group_name": r["GROUP"] or "Independent",
                            "religion": r["RELIGION"] or "Unknown",
                            "district": r["REGION"] or "General",
                        }
                        for _, r in members_df.iterrows()
                    ],
                )

                conn.execute(
                    text("""
                        INSERT INTO election_votes (year, district_code, candidate_id, member, votes)
                        VALUES (:year, :d, :candidate_id, :member, :votes)
                    """),
                    [
                        {
                            "year": int(year), "d": district,
                            "candidate_id": r["candidate_id"] if pd.notna(r["candidate_id"]) else None,
                            "member": r["MEMBER"],
                            "votes": int(r["VOTES"]),
                        }
                        for _, r in votes_df.iterrows()
                    ],
                )

            results.append((
                year, district, "IMPORTED",
                f"seats={len(seats_df)} members={len(members_df)} votes_rows={len(votes_df)}{detail_suffix}",
            ))

    return results


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    for year, district, status, detail in import_all(dry_run=dry):
        print(f"{year} {district}: {status} ({detail})")
