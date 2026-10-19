from flask import Flask, render_template, abort, request, jsonify, send_file, g
import pandas as pd
import numpy as np
import json
import os
import re
import math
import difflib
from io import BytesIO
from datetime import datetime
from sqlalchemy import create_engine, text, bindparam
import geopandas as gpd
import plotly.express as px
import plotly
import plotly.graph_objects as go
import plotly.utils
from itertools import combinations
from arabic_utils import normalize_arabic
import unicodedata


def norm_place(value) -> str:
    """Normalize a place name for matching voters_2026.town_norm against
    election_pen_votes.neighborhood -- see migrations/import_voters_2026.py
    (same logic, kept in sync) for why NFKC is needed on top of normalize_arabic."""
    text_ = unicodedata.normalize("NFKC", str(value or ""))
    text_ = normalize_arabic(text_)
    text_ = re.sub(r"[ً-ْـ]", "", text_)
    return re.sub(r"\s+", "", text_)

app = Flask(__name__)

# --- COLORS (Matching the requested map) ---
REGION_COLORS = {
    "beirut_one": "#90A4AE", "beirut_two": "#90A4AE", "akkar": "#90A4AE",
    "minieh_dannieh": "#90A4AE", "miniyeh_danniyeh": "#90A4AE", "tripoli": "#90A4AE",
    "zgharta": "#90A4AE", "koura": "#90A4AE", "jbayl": "#90A4AE",
    "kesrouan": "#90A4AE", "matn": "#90A4AE", "baabda": "#90A4AE",
    "saida": "#90A4AE", "jezzine": "#90A4AE", "bcharreh": "#1B5E20",
    "aley": "#1B5E20", "chouf": "#1B5E20", "batroun": "#FF7043",
    "baalbek_hermel": "#FFEB3B", "zahleh": "#FFEB3B", "westbekaa_rachaya": "#FFEB3B",
    "zahrany": "#FFEB3B", "sour": "#FFEB3B", "nabatiyeh": "#FFEB3B",
    "marjayoun_hasbaya": "#FFEB3B", "bent_jbayl": "#FFEB3B",
}
DEFAULT_COLOR = "#bdc3c7"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- POSTGRESQL SETUP ---
# Every runtime feature reads from and writes to PostgreSQL.
# File generation is used only by the Excel/PDF export endpoints.
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:lims@localhost:5432/election_db"
)
DB_ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)


# =========================================================
#  DATABASE + HELPER FUNCTIONS
# =========================================================
def election_id(year: str, district_code: str) -> str:
    # Kept as the public identifier used by the frontend and export URLs.
    # It no longer points to a file or an in-memory cache.
    return f"{int(year)}:{str(district_code).strip()}"


def parse_file_id(file_id: str):
    if not file_id or ":" not in str(file_id):
        return None, None
    year_text, district_code = str(file_id).split(":", 1)
    try:
        year = int(year_text)
    except (TypeError, ValueError):
        return None, None
    district_code = district_code.strip()
    return (year, district_code) if district_code else (None, None)


def _request_dict(name: str):
    """Small request-local cache for immutable quota metadata only."""
    try:
        value = getattr(g, name, None)
        if value is None:
            value = {}
            setattr(g, name, value)
        return value
    except RuntimeError:
        # Allows helper functions to be called from a Flask shell/test without
        # an active request context. PostgreSQL remains the source of truth.
        return None


def _std_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def _pick(df: pd.DataFrame, names):
    for name in names:
        if name in df.columns:
            return name
    return None


def _norm_name(value) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def load_tables_from_database(year: str, district_code: str, baseline: bool = False):
    """Load seats, candidates, and aggregated votes from PostgreSQL."""
    params = {"year": int(year), "district_code": district_code}
    group_expression = "COALESCE(original_group_name, group_name)" if baseline else "group_name"
    try:
        with DB_ENGINE.connect() as conn:
            seats_df = pd.read_sql(text("""
                SELECT district AS "DISTRICT", religion AS "RELIGION"
                FROM election_seats
                WHERE year = :year AND district_code = :district_code
                ORDER BY id
            """), conn, params=params)

            members_df = pd.read_sql(text(f"""
                SELECT
                    CAST(candidate_id AS TEXT) AS "CANDIDATE_ID",
                    member AS "MEMBER",
                    {group_expression} AS "GROUP",
                    religion AS "RELIGION",
                    district AS "DISTRICT"
                FROM election_members
                WHERE year = :year AND district_code = :district_code
                ORDER BY id
            """), conn, params=params)

            data_df = pd.read_sql(text("""
                SELECT
                    CAST(candidate_id AS TEXT) AS "CANDIDATE_ID",
                    member AS "MEMBER",
                    SUM(votes)::int AS "VOTES"
                FROM election_votes
                WHERE year = :year AND district_code = :district_code
                GROUP BY candidate_id, member
            """), conn, params=params)
        return seats_df, members_df, data_df
    except Exception as exc:
        print(f"Database load failed for {year}:{district_code}: {exc}")
        return None, None, None


def _quota_from_seats(seats_df: pd.DataFrame) -> dict:
    rel_limits = {}
    dist_totals = {}
    if seats_df is None or seats_df.empty:
        return {"rel_limits": rel_limits, "dist_totals": dist_totals}

    normalized = _std_cols(seats_df).rename(columns={
        "REGION": "DISTRICT", "QADA": "DISTRICT",
        "SECT": "RELIGION", "CONFESSION": "RELIGION"
    })
    if "DISTRICT" in normalized.columns and "RELIGION" in normalized.columns:
        for _, row in normalized.iterrows():
            district = normalize_arabic(row.get("DISTRICT", ""))
            religion = normalize_arabic(row.get("RELIGION", ""))
            if not district or not religion:
                continue
            rel_limits[(district, religion)] = rel_limits.get((district, religion), 0) + 1
            dist_totals[district] = dist_totals.get(district, 0) + 1
    return {"rel_limits": rel_limits, "dist_totals": dist_totals}


def _store_request_quota(file_id: str, quota: dict):
    request_cache = _request_dict("quota_cache")
    if request_cache is not None:
        request_cache[file_id] = quota


def get_quota(file_id: str) -> dict:
    request_cache = _request_dict("quota_cache")
    if request_cache is not None and file_id in request_cache:
        return request_cache[file_id]

    year, district_code = parse_file_id(file_id)
    if year is None:
        return {"rel_limits": {}, "dist_totals": {}}
    try:
        with DB_ENGINE.connect() as conn:
            seats_df = pd.read_sql(text("""
                SELECT district AS "DISTRICT", religion AS "RELIGION"
                FROM election_seats
                WHERE year = :year AND district_code = :district_code
                ORDER BY id
            """), conn, params={"year": year, "district_code": district_code})
        quota = _quota_from_seats(seats_df)
        _store_request_quota(file_id, quota)
        return quota
    except Exception as exc:
        print(f"Quota load failed for {file_id}: {exc}")
        return {"rel_limits": {}, "dist_totals": {}}


def get_list_only_votes(file_id: str) -> dict:
    """Votes cast for a list without naming a preferred candidate ("list
    only" ballots under Lebanese preferential-vote law) -- real votes that
    count toward a list's quota-seat total but were never attributed to any
    individual candidate, so they never appear in a per-candidate VOTES sum.
    """
    request_cache = _request_dict("list_only_cache")
    if request_cache is not None and file_id in request_cache:
        return request_cache[file_id]

    year, district_code = parse_file_id(file_id)
    if year is None:
        return {}
    try:
        with DB_ENGINE.connect() as conn:
            rows = conn.execute(text("""
                SELECT group_name, COALESCE(SUM(votes), 0)::int AS votes
                FROM election_votes
                WHERE year = :year AND district_code = :district_code
                  AND candidate_id IS NULL AND group_name IS NOT NULL
                GROUP BY group_name
            """), {"year": year, "district_code": district_code}).all()
        result = {str(g): int(v) for g, v in rows}
    except Exception as exc:
        print(f"List-only vote load failed for {file_id}: {exc}")
        result = {}
    if request_cache is not None:
        request_cache[file_id] = result
    return result


def _list_key(name) -> str:
    """Canonical key for comparing list names across source files."""
    text_ = normalize_arabic(str(name or ""))
    text_ = re.sub("[ً-ْـ]", "", text_)
    return re.sub(r"[\s\d()]+", "", text_)


def group_vote_totals(file_id: str, df: pd.DataFrame) -> pd.DataFrame:
    """Per-list (GROUP) vote totals, including list-only ballots -- the
    single source of truth every seat-quota calculation should sum from,
    instead of a bare df.groupby("GROUP")["VOTES"].sum() which silently
    excludes list-only ballots and therefore undercounts every list."""
    df_group = df.groupby("GROUP", as_index=False)["VOTES"].sum()
    list_only = get_list_only_votes(file_id)
    if not list_only:
        return df_group

    # The raw export sometimes files a list's list-only ballots under a
    # slightly different spelling of its name (a leading list number, an
    # extra space, a hamza/madda variant). Match on a canonical key so those
    # ballots are added to the real list instead of forming a phantom list.
    key_to_group = {}
    for g in df_group["GROUP"].astype(str):
        key_to_group.setdefault(_list_key(g), g)
    extra_votes, unmatched = {}, {}
    for name, votes in list_only.items():
        target = name if name in set(df_group["GROUP"].astype(str)) else key_to_group.get(_list_key(name))
        if target is not None:
            extra_votes[target] = extra_votes.get(target, 0) + votes
        else:
            unmatched[name] = unmatched.get(name, 0) + votes

    df_group["VOTES"] = df_group.apply(
        lambda row: row["VOTES"] + extra_votes.get(str(row["GROUP"]), 0), axis=1
    )
    # A list can have list-only ballots but no candidate in the current
    # scenario; keep it so its votes still count toward the quotient.
    if unmatched:
        extra = pd.DataFrame([{"GROUP": g, "VOTES": v} for g, v in unmatched.items()])
        df_group = pd.concat([df_group, extra], ignore_index=True)
    return df_group


def load_regions_from_database():
    """Load map geometry and year availability from PostgreSQL."""
    sql = text("""
        WITH members_available AS (
            SELECT DISTINCT year, district_code FROM election_members
        ), votes_available AS (
            SELECT DISTINCT year, district_code FROM election_votes
        ), seats_available AS (
            SELECT DISTINCT year, district_code FROM election_seats
        ), available AS (
            SELECT m.year, m.district_code
            FROM members_available m
            INNER JOIN votes_available v USING (year, district_code)
            INNER JOIN seats_available s USING (year, district_code)
        )
        SELECT
            r.slug, r.name, r.district_code, r.district_label,
            r.geometry, r.display_order, a.year
        FROM election_regions r
        LEFT JOIN available a ON a.district_code = r.district_code
        WHERE r.active = TRUE
        ORDER BY r.display_order, r.id, a.year
    """)
    try:
        with DB_ENGINE.connect() as conn:
            rows = conn.execute(sql).mappings().all()
    except Exception as exc:
        print(f"Region load failed: {exc}")
        return []

    by_slug = {}
    for row in rows:
        slug = str(row["slug"])
        region = by_slug.setdefault(slug, {
            "name": str(row["name"]),
            "slug": slug,
            "district_code": str(row["district_code"]),
            "district_label": str(row["district_label"] or row["district_code"]),
            "geometry": row["geometry"],
            "display_order": int(row["display_order"] or 0),
            "files": {},
        })
        if row["year"] is not None:
            year = str(int(row["year"]))
            region["files"][year] = election_id(year, region["district_code"])
    return list(by_slug.values())


def get_region_from_database(slug: str):
    for region in load_regions_from_database():
        if region["slug"] == slug:
            return region
    return None


def deduplicated_display_regions(regions):
    """Filter out placeholder/ghost districts and collapse duplicate slugs
    that share a district_label, in stable display_label order.

    Shared by the map (index) and the national dashboard so a future change
    to the exclusion rules (e.g. another phantom district needs hiding) only
    has to be made in one place.
    """
    seen_labels, result = set(), []
    for region in sorted(regions, key=lambda item: (item["district_label"], item["name"])):
        label = region["district_label"]
        if label == "Unknown" or "Beirut 3" in label or region["name"] == "Beirut-three":
            continue
        if label in seen_labels:
            continue
        seen_labels.add(label)
        result.append(region)
    return result


def create_interactive_map(regions, selected_year):
    if not regions:
        return "{}"

    features = []
    for region in regions:
        geometry = region.get("geometry")
        if isinstance(geometry, str):
            try:
                geometry = json.loads(geometry)
            except json.JSONDecodeError:
                continue
        if not isinstance(geometry, dict):
            continue
        features.append({
            "type": "Feature",
            "id": region["slug"],
            "geometry": geometry,
            "properties": {
                "slug": region["slug"],
                "name": region["name"],
                "district_code": region["district_code"],
            },
        })

    if not features:
        return "{}"

    try:
        gdf = gpd.GeoDataFrame.from_features(features)
    except Exception as exc:
        print(f"Map geometry error: {exc}")
        return "{}"

    region_by_slug = {region["slug"]: region for region in regions}
    district_codes = sorted({region["district_code"] for region in regions})
    totals = {}
    if district_codes:
        stmt = text("""
            SELECT district_code, COALESCE(SUM(votes), 0)::bigint AS total_votes
            FROM election_votes
            WHERE year = :year AND district_code IN :district_codes
            GROUP BY district_code
        """).bindparams(bindparam("district_codes", expanding=True))
        try:
            with DB_ENGINE.connect() as conn:
                rows = conn.execute(stmt, {
                    "year": int(selected_year),
                    "district_codes": district_codes,
                }).mappings().all()
            totals = {str(row["district_code"]): int(row["total_votes"] or 0) for row in rows}
        except Exception as exc:
            print(f"Map vote totals failed: {exc}")

    gdf["District"] = [region_by_slug[str(slug)]["district_label"] for slug in gdf["slug"]]
    gdf["Data Available"] = [
        "Yes" if selected_year in region_by_slug[str(slug)]["files"] else "No"
        for slug in gdf["slug"]
    ]
    gdf["Total Voters"] = [
        f"{totals.get(region_by_slug[str(slug)]['district_code'], 0):,}"
        if selected_year in region_by_slug[str(slug)]["files"]
        else "No data"
        for slug in gdf["slug"]
    ]
    color_map = {str(slug): REGION_COLORS.get(str(slug), DEFAULT_COLOR) for slug in gdf["slug"]}

    geojson = json.loads(gdf.to_json())
    fig = px.choropleth(
        gdf,
        geojson=geojson,
        locations="slug",
        featureidkey="properties.slug",
        color="slug",
        color_discrete_map=color_map,
        hover_name="District",
        hover_data={
            "slug": False,
            "District": False,
            "Total Voters": True,
            "Data Available": True,
        },
    )
    fig.update_traces(marker_line_width=1, marker_line_color="black")
    fig.update_geos(fitbounds="locations", visible=False, bgcolor="rgba(0,0,0,0)")
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        dragmode=False,
    )
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


def persist_winner_flags(file_id: str, winners: set):
    # winners holds candidate_id values (falls back to a name string only if
    # a row somehow has no candidate_id) -- never match winners by name here:
    # two candidates can share the exact same name, and a name-based UPDATE
    # would flag both of them as winners even when only one of them won.
    year, district_code = parse_file_id(file_id)
    if year is None:
        return
    winner_ids = sorted({str(w) for w in winners})
    with DB_ENGINE.begin() as conn:
        conn.execute(text("""
            UPDATE election_members
            SET is_winner = FALSE, updated_at = NOW()
            WHERE year = :year AND district_code = :district_code
        """), {"year": year, "district_code": district_code})
        if winner_ids:
            stmt = text("""
                UPDATE election_members
                SET is_winner = TRUE, updated_at = NOW()
                WHERE year = :year AND district_code = :district_code
                  AND COALESCE(candidate_id, member) IN :winner_ids
            """).bindparams(bindparam("winner_ids", expanding=True))
            conn.execute(stmt, {
                "year": year,
                "district_code": district_code,
                "winner_ids": winner_ids,
            })


def update_candidate_group_db(file_id: str, candidate_name: str, new_group: str, candidate_id: str = None):
    # Two candidates can share the exact same name (confirmed in this
    # dataset), so the UPDATE always targets the specific candidate_id
    # resolved by the SELECT below -- never re-matches by name -- so it can
    # only ever touch the one row that was actually read, even when the
    # caller (e.g. the chat text interface) has no id to disambiguate with
    # up front and multiple candidates share that name.
    year, district_code = parse_file_id(file_id)
    if year is None or not candidate_name or not str(new_group).strip():
        raise ValueError("Invalid election, candidate, or list.")
    with DB_ENGINE.begin() as conn:
        row = conn.execute(text("""
            SELECT candidate_id, group_name
            FROM election_members
            WHERE year = :year AND district_code = :district_code
              AND (candidate_id = :candidate_id OR (:candidate_id IS NULL AND member = :member))
            ORDER BY id
            LIMIT 1
            FOR UPDATE
        """), {
            "year": year, "district_code": district_code,
            "member": candidate_name, "candidate_id": candidate_id,
        }).mappings().first()
        if row is None:
            raise LookupError(f"Candidate {candidate_name} not found.")
        old_group = str(row["group_name"])
        conn.execute(text("""
            UPDATE election_members
            SET group_name = :new_group, updated_at = NOW()
            WHERE year = :year AND district_code = :district_code AND candidate_id = :resolved_id
        """), {
            "new_group": str(new_group).strip(), "year": year,
            "district_code": district_code, "resolved_id": row["candidate_id"],
        })
    return old_group


def swap_candidate_groups_db(
    file_id: str, source_name: str, target_name: str,
    source_id: str = None, target_id: str = None,
):
    # Same principle as update_candidate_group_db: each UPDATE below targets
    # the specific candidate_id resolved by its own SELECT, never a name --
    # so two same-named candidates (confirmed to exist in this dataset) can
    # never both be mutated by one swap, with or without an explicit id.
    year, district_code = parse_file_id(file_id)
    if year is None or not source_name or not target_name or source_name == target_name:
        raise ValueError("Choose two different candidates.")

    row_sql = text("""
        SELECT candidate_id, member, group_name, religion, district
        FROM election_members
        WHERE year = :year AND district_code = :district_code
          AND (candidate_id = :cid OR (:cid IS NULL AND member = :name))
        ORDER BY id
        LIMIT 1
        FOR UPDATE
    """)

    with DB_ENGINE.begin() as conn:
        source = conn.execute(row_sql, {
            "year": year, "district_code": district_code, "name": source_name, "cid": source_id,
        }).mappings().first()
        target = conn.execute(row_sql, {
            "year": year, "district_code": district_code, "name": target_name, "cid": target_id,
        }).mappings().first()
        if source is None or target is None:
            raise LookupError("One or both candidates were not found.")
        if str(source["religion"]) != str(target["religion"]) or str(source["district"]) != str(target["district"]):
            raise ValueError("Candidates must have the same religion and district.")

        source_group = str(source["group_name"])
        target_group = str(target["group_name"])
        update_sql = text("""
            UPDATE election_members
            SET group_name = :new_group, updated_at = NOW()
            WHERE year = :year AND district_code = :district_code AND candidate_id = :resolved_id
        """)
        conn.execute(update_sql, {
            "new_group": target_group, "year": year, "district_code": district_code,
            "resolved_id": source["candidate_id"],
        })
        conn.execute(update_sql, {
            "new_group": source_group, "year": year, "district_code": district_code,
            "resolved_id": target["candidate_id"],
        })
    return {
        "source_group": source_group,
        "target_group": target_group,
        "religion": str(source["religion"]),
        "district": str(source["district"]),
    }


def multi_swap_candidate_groups_db(file_id: str, swaps):
    # NOTE: swap suggestions from build_atomic_swaps_for_group are name-only
    # (no candidate_id), so this function -- unlike update_candidate_group_db
    # and swap_candidate_groups_db -- still resolves candidates by name and
    # is not safe against the same-name-different-list collisions documented
    # there. Not hit by any of the 3 currently known duplicate pairs, but a
    # real residual gap if a future district has one of its weakest/strongest
    # candidates share a name with someone else in the same district.
    year, district_code = parse_file_id(file_id)
    if year is None or not swaps:
        raise ValueError("Invalid election identifier or empty swaps.")

    with DB_ENGINE.begin() as conn:
        rows = conn.execute(text("""
            SELECT member, group_name, religion, district
            FROM election_members
            WHERE year = :year AND district_code = :district_code
            ORDER BY id
            FOR UPDATE
        """), {"year": year, "district_code": district_code}).mappings().all()

        state = {
            str(row["member"]): {
                "group": str(row["group_name"]),
                "religion": str(row["religion"]),
                "district": str(row["district"]),
            }
            for row in rows
        }
        original_groups = {name: item["group"] for name, item in state.items()}

        for item in swaps:
            source_name = str(item.get("out") or "").strip()
            target_name = str(item.get("in") or "").strip()
            if not source_name or not target_name or source_name == target_name:
                raise ValueError("Every swap must contain two different candidates.")
            if source_name not in state or target_name not in state:
                raise LookupError("One or more candidates were not found.")
            source = state[source_name]
            target = state[target_name]
            if source["religion"] != target["religion"] or source["district"] != target["district"]:
                raise ValueError("Every swap must use candidates from the same religion and district.")
            source["group"], target["group"] = target["group"], source["group"]

        updates = [
            {
                "year": year,
                "district_code": district_code,
                "member": name,
                "new_group": item["group"],
            }
            for name, item in state.items()
            if original_groups.get(name) != item["group"]
        ]
        if updates:
            conn.execute(text("""
                UPDATE election_members
                SET group_name = :new_group, updated_at = NOW()
                WHERE year = :year AND district_code = :district_code AND member = :member
            """), updates)
    return len(updates)


def reset_groups_db(file_id: str):
    year, district_code = parse_file_id(file_id)
    if year is None:
        raise ValueError("Invalid election identifier.")

    with DB_ENGINE.begin() as conn:
        # Detect lists that exist only in the current scenario and were therefore
        # created by the user. Original lists are stored in original_group_name.
        custom_lists = conn.execute(text("""
            SELECT DISTINCT group_name
            FROM election_members
            WHERE year = :year
              AND district_code = :district_code
              AND group_name IS NOT NULL
              AND TRIM(group_name) <> ''
              AND group_name NOT IN (
                  SELECT DISTINCT original_group_name
                  FROM election_members
                  WHERE year = :year
                    AND district_code = :district_code
                    AND original_group_name IS NOT NULL
                    AND TRIM(original_group_name) <> ''
              )
            ORDER BY group_name
        """), {
            "year": year,
            "district_code": district_code,
        }).scalars().all()

        # Move every changed candidate back to the immutable/original list.
        # Once no candidate belongs to a user-created group_name, that custom
        # list disappears automatically because lists are derived from candidates.
        result = conn.execute(text("""
            UPDATE election_members
            SET group_name = original_group_name,
                updated_at = NOW()
            WHERE year = :year
              AND district_code = :district_code
              AND original_group_name IS NOT NULL
              AND group_name IS DISTINCT FROM original_group_name
        """), {
            "year": year,
            "district_code": district_code,
        })

    return {
        "updated_candidates": int(result.rowcount or 0),
        "removed_custom_lists": [str(name) for name in custom_lists],
    }

# =========================================================
#  CORE DATA LOGIC
# =========================================================
def get_candidates_df(file_id: str, baseline: bool = False):
    """Return the candidate DataFrame for one year/district from PostgreSQL.

    Important: the rest of the app still receives the same DataFrame columns as before:
    MEMBER, GROUP, RELIGION, DISTRICT, VOTES, IS_WINNER.
    This means the UI, reports, comparison page, chat, and simulations keep working.
    """
    year, district_code = parse_file_id(file_id)
    if year is None:
        return None

    try:
        seats_df, members_df, data_df = load_tables_from_database(year, district_code, baseline=baseline)
        if seats_df is None or members_df is None or data_df is None:
            return None
        if members_df.empty or seats_df.empty:
            return None

        seats_df = _std_cols(seats_df).rename(
            columns={"REGION": "DISTRICT", "QADA": "DISTRICT", "SECT": "RELIGION", "CONFESSION": "RELIGION"}
        )

        _store_request_quota(file_id, _quota_from_seats(seats_df))

        members_df = _std_cols(members_df)
        m_id = _pick(members_df, ["CANDIDATE_ID", "CANDIDATEID", "ID", "CID"])
        m_name = _pick(members_df, ["MEMBER", "CANDIDATE", "NAME", "HOLDER"])
        m_group = _pick(members_df, ["GROUP", "LIST", "LIST NAME", "LIST_NAME", "اللائحة"])
        m_rel = _pick(members_df, ["RELIGION", "SECT", "CONFESSION"])
        m_dist = _pick(members_df, ["DISTRICT", "REGION", "QADA"])

        ren_m = {}
        if m_id: ren_m[m_id] = "CANDIDATE_ID"
        if m_name: ren_m[m_name] = "MEMBER"
        if m_group: ren_m[m_group] = "GROUP"
        if m_rel: ren_m[m_rel] = "RELIGION"
        if m_dist: ren_m[m_dist] = "DISTRICT"
        members_df = members_df.rename(columns=ren_m)

        if "MEMBER" not in members_df.columns:
            return None
        if "GROUP" not in members_df.columns:
            members_df["GROUP"] = "Independent"
        if "RELIGION" not in members_df.columns:
            members_df["RELIGION"] = "Unknown"
        if "DISTRICT" not in members_df.columns:
            members_df["DISTRICT"] = "General"

        for col in ["MEMBER", "GROUP", "RELIGION", "DISTRICT"]:
            members_df[col] = members_df[col].astype(str).str.strip()
        members_df["_NAME_KEY"] = members_df["MEMBER"].map(_norm_name)

        data_df = _std_cols(data_df)
        d_id = _pick(data_df, ["CANDIDATE_ID", "CANDIDATEID", "ID", "CID"])
        d_name = _pick(data_df, ["MEMBER", "CANDIDATE", "NAME", "HOLDER"])
        d_votes = _pick(data_df, ["VOTES", "VOTE", "TOTAL VOTES", "TOTAL_VOTES", "VOTE_LIST", "PREFERENTIAL VOTES"])

        ren_d = {}
        if d_id: ren_d[d_id] = "CANDIDATE_ID"
        if d_name: ren_d[d_name] = "MEMBER"
        if d_votes: ren_d[d_votes] = "VOTES"
        data_df = data_df.rename(columns=ren_d)

        if "VOTES" not in data_df.columns:
            data_df["VOTES"] = 0
        data_df["VOTES"] = pd.to_numeric(data_df["VOTES"], errors="coerce").fillna(0).astype(int)

        if "MEMBER" in data_df.columns:
            data_df["_NAME_KEY"] = data_df["MEMBER"].map(_norm_name)
        else:
            data_df["_NAME_KEY"] = ""

        # IMPORTANT FIX for PostgreSQL import:
        # In many Excel files CANDIDATE_ID is empty/NULL. Pandas groupby drops NULL keys,
        # so grouping by CANDIDATE_ID made all votes disappear and the UI showed 0.
        # Use CANDIDATE_ID only when it contains real values in BOTH tables; otherwise match by candidate name.
        def _has_real_ids(frame, col="CANDIDATE_ID"):
            if col not in frame.columns:
                return False

            ids = frame[col].dropna().astype("string").str.strip().str.lower()
            ids = ids[~ids.isin(["", "none", "nan", "null", "nat", "<na>"])]
            return not ids.empty

        def _normalize_candidate_ids(frame, col="CANDIDATE_ID"):
            """Normalize candidate IDs so Pandas always merges identical dtypes.

            PostgreSQL/driver or Pandas version changes may return the same ID
            column as text in one DataFrame and float in another. Converting both
            sides to Pandas' nullable string dtype avoids object/float64 merge errors.
            """
            if col not in frame.columns:
                return

            ids = (
                frame[col]
                .astype("string")
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
            )

            invalid_ids = ids.str.lower().isin(
                ["", "none", "nan", "null", "nat", "<na>"]
            )
            frame[col] = ids.mask(invalid_ids, pd.NA)

        _normalize_candidate_ids(members_df)
        _normalize_candidate_ids(data_df)

        use_candidate_id = _has_real_ids(members_df) and _has_real_ids(data_df)

        if use_candidate_id:
            votes_agg = (
                data_df.groupby(
                    "CANDIDATE_ID",
                    as_index=False,
                    dropna=False,
                )["VOTES"]
                .sum()
            )
            merged = pd.merge(
                members_df,
                votes_agg[["CANDIDATE_ID", "VOTES"]],
                on="CANDIDATE_ID",
                how="left",
            )
        else:
            if "MEMBER" not in data_df.columns:
                data_df["MEMBER"] = ""
            data_df["_NAME_KEY"] = data_df["MEMBER"].map(_norm_name)
            votes_agg = data_df.groupby("_NAME_KEY", as_index=False, dropna=False)["VOTES"].sum()
            merged = pd.merge(
                members_df,
                votes_agg[["_NAME_KEY", "VOTES"]],
                on="_NAME_KEY",
                how="left"
            )

        merged["VOTES"] = pd.to_numeric(merged.get("VOTES", 0), errors="coerce").fillna(0).astype(int)
        if "CANDIDATE_ID" not in merged.columns:
            merged["CANDIDATE_ID"] = pd.NA
        df_final = merged[["CANDIDATE_ID", "MEMBER", "GROUP", "RELIGION", "DISTRICT", "VOTES"]].copy()

        # Two different candidates can legitimately share the exact same
        # name (confirmed in this dataset -- e.g. Mount4/2018), so winner
        # identity must be tracked by candidate_id, never by name: matching
        # by name here would mark every same-named row as a winner the
        # moment any one of them wins a seat.
        winners = _compute_winners_from_quota(file_id, df_final)
        df_final["IS_WINNER"] = df_final["CANDIDATE_ID"].isin(winners)

        return df_final

    except Exception as e:
        print(f"❌ System Error: {e}")
        return None

def _compute_winners_from_quota(file_id: str, df: pd.DataFrame) -> set:
    quota = get_quota(file_id)
    rel_limits = quota.get("rel_limits", {})

    number_of_seats = sum(rel_limits.values())
    if number_of_seats == 0: return set()

    seats_list = []
    for (dist, rel), count in rel_limits.items():
        for _ in range(int(count)):
            seats_list.append({"DISTRICT": dist, "RELIGION": rel, "HOLDER": "NA", "GROUP": "NA"})
    df_seats = pd.DataFrame(seats_list)

    df_group = group_vote_totals(file_id, df)
    valid_electoral = df_group["VOTES"].sum()
    if valid_electoral == 0: return set()

    electoral_quotient = valid_electoral / number_of_seats
    df_group["USED"] = 1
    df_group.loc[df_group["VOTES"] < electoral_quotient, "USED"] = 0

    unused_groups_votes = df_group.loc[df_group["USED"] == 0, "VOTES"].sum()
    adjusted_electoral = valid_electoral - unused_groups_votes
    if adjusted_electoral <= 0: return set()

    electoral_quotient_adjusted = adjusted_electoral / number_of_seats

    df_group_adjusted = df_group[df_group["USED"] == 1].copy()
    df_group_adjusted["QUOTIENT"] = df_group_adjusted["VOTES"] / electoral_quotient_adjusted
    df_group_adjusted["INITIAL_SEATS"] = df_group_adjusted["QUOTIENT"].apply(lambda x: math.floor(round(x, 9)))
    df_group_adjusted["ADVANTAGE"] = df_group_adjusted["QUOTIENT"] - df_group_adjusted["INITIAL_SEATS"]

    seats_left = number_of_seats - df_group_adjusted["INITIAL_SEATS"].sum()
    df_group_adjusted = df_group_adjusted.sort_values(by="ADVANTAGE", ascending=False).reset_index(drop=True)

    df_group_adjusted["EXTRA_SEATS"] = 0
    if seats_left > 0:
        for i in range(min(int(seats_left), len(df_group_adjusted))):
            df_group_adjusted.loc[i, "EXTRA_SEATS"] = 1

    df_group_adjusted["FINAL_SEATS"] = df_group_adjusted["INITIAL_SEATS"] + df_group_adjusted["EXTRA_SEATS"]
    group_seats_won = dict(zip(df_group_adjusted["GROUP"], df_group_adjusted["FINAL_SEATS"]))

    qualified_groups = df_group_adjusted["GROUP"].tolist()
    df_final_members = df[df["GROUP"].isin(qualified_groups)].copy()
    district_totals = df_final_members.groupby("DISTRICT")["VOTES"].sum().to_dict()

    def get_perc(row):
        dist, votes = str(row["DISTRICT"]), float(row["VOTES"])
        dist_total = float(district_totals.get(dist, 1))
        if dist_total == 0: return 0.0
        return (votes / dist_total) * 100

    df_final_members["VOTE_PERC"] = df_final_members.apply(get_perc, axis=1)
    # Stable sort with raw votes as the tie-breaker so equal percentages never
    # depend on row order.
    df_final_members = df_final_members.sort_values(
        by=["VOTE_PERC", "VOTES"], ascending=[False, False], kind="mergesort"
    ).reset_index(drop=True)

    winners = set()
    group_seats_filled = {g: 0 for g in group_seats_won}

    for _, row in df_final_members.iterrows():
        c_id, c_name, c_dist, c_rel, c_group = row["CANDIDATE_ID"], row["MEMBER"], row["DISTRICT"], row["RELIGION"], row["GROUP"]
        won, filled = group_seats_won.get(c_group, 0), group_seats_filled.get(c_group, 0)

        if won - filled > 0:
            mask = (
                (df_seats["HOLDER"] == "NA")
                & (df_seats["DISTRICT"] == normalize_arabic(c_dist))
                & (df_seats["RELIGION"] == normalize_arabic(c_rel))
            )
            if mask.any():
                seat_idx = df_seats[mask].index[0]
                df_seats.loc[seat_idx, "HOLDER"] = c_name
                df_seats.loc[seat_idx, "GROUP"] = c_group
                group_seats_filled[c_group] += 1
                # Two candidates can share the exact same name (confirmed in
                # this dataset), so identity must be the candidate_id, never
                # the name -- otherwise both same-named rows would end up
                # flagged as winners even though only one of them won a seat.
                winners.add(c_id if pd.notna(c_id) else str(c_name))

    # A seat can stay open after the pass above: the list still owed a seat
    # has no remaining candidate of that confession/district, while every
    # remaining candidate for it belongs to a list that already filled its
    # share. A parliamentary seat is never left vacant, so it goes to the
    # best-ranked remaining candidate for that seat among the qualified lists
    # (the same transfer the law applies when a list cannot fill a seat).
    # Never triggered by the official 2018/2022 data, only by simulations.
    for seat_idx in df_seats[df_seats["HOLDER"] == "NA"].index:
        seat_dist, seat_rel = df_seats.loc[seat_idx, "DISTRICT"], df_seats.loc[seat_idx, "RELIGION"]
        for _, row in df_final_members.iterrows():
            c_id = row["CANDIDATE_ID"] if pd.notna(row["CANDIDATE_ID"]) else str(row["MEMBER"])
            if c_id in winners:
                continue
            if normalize_arabic(row["DISTRICT"]) != seat_dist or normalize_arabic(row["RELIGION"]) != seat_rel:
                continue
            df_seats.loc[seat_idx, "HOLDER"] = row["MEMBER"]
            df_seats.loc[seat_idx, "GROUP"] = row["GROUP"]
            winners.add(c_id)
            break
        else:
            # No candidate left for this seat on any qualified list (possible
            # only when the qualified lists are incomplete): take the
            # highest-voted remaining candidate for the seat on any list.
            for _, row in df.sort_values("VOTES", ascending=False).iterrows():
                c_id = row["CANDIDATE_ID"] if pd.notna(row["CANDIDATE_ID"]) else str(row["MEMBER"])
                if c_id in winners:
                    continue
                if normalize_arabic(row["DISTRICT"]) != seat_dist or normalize_arabic(row["RELIGION"]) != seat_rel:
                    continue
                df_seats.loc[seat_idx, "HOLDER"] = row["MEMBER"]
                df_seats.loc[seat_idx, "GROUP"] = row["GROUP"]
                winners.add(c_id)
                break

    return winners

def recompute_winners(file_id: str) -> set:
    df = get_candidates_df(file_id)
    if df is None:
        return set()
    winners = _compute_winners_from_quota(file_id, df)
    persist_winner_flags(file_id, winners)
    return winners

def load_original_df(file_id: str):
    """Load the immutable baseline list assignments from PostgreSQL."""
    df = get_candidates_df(file_id, baseline=True)
    return df.copy() if df is not None else None

def group_seat_map(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {}
    return {str(k): int(v) for k, v in df[df["IS_WINNER"] == True].groupby("GROUP")["MEMBER"].count().to_dict().items()}

def summarize_groups_for_compare(original_df: pd.DataFrame, current_df: pd.DataFrame):
    groups = sorted(set(original_df["GROUP"].astype(str).unique()).union(set(current_df["GROUP"].astype(str).unique())))
    original_seats = group_seat_map(original_df)
    current_seats = group_seat_map(current_df)
    rows = []
    for grp in groups:
        ov = int(original_df[original_df["GROUP"].astype(str) == grp]["VOTES"].sum())
        cv = int(current_df[current_df["GROUP"].astype(str) == grp]["VOTES"].sum())
        original_seat_count = int(original_seats.get(grp, 0))
        current_seat_count = int(current_seats.get(grp, 0))
        rows.append({
            "group": grp, "original_votes": ov, "current_votes": cv,
            "vote_change": cv - ov, "original_seats": original_seat_count, "current_seats": current_seat_count,
            "seat_change": current_seat_count - original_seat_count
        })
    rows.sort(key=lambda r: (r["seat_change"], r["current_votes"]), reverse=True)
    return rows

# =========================================================
#  FAST ON-DEMAND SUGGESTION ENGINE
# =========================================================
def count_group_winners(df: pd.DataFrame, winners: set) -> dict:
    winner_df = df[df["CANDIDATE_ID"].isin(winners)].copy()
    if winner_df.empty: return {}
    return {str(k): int(v) for k, v in winner_df.groupby("GROUP")["MEMBER"].count().to_dict().items()}

def get_current_group_seats(file_id: str, df: pd.DataFrame) -> dict:     # return a dictionary: {group_name: number_of_seats_won}
    return count_group_winners(df, _compute_winners_from_quota(file_id, df))

def build_atomic_swaps_for_group(df: pd.DataFrame, target_group: str):
    atomic = []
    list_cands = df[df["GROUP"] == target_group].sort_values("VOTES", ascending=True).head(8)
    other_cands = df[df["GROUP"] != target_group].sort_values("VOTES", ascending=False).head(25)

    for _, c_src in list_cands.iterrows():
        for _, c_tgt in other_cands.iterrows():
            if str(c_src["RELIGION"]) == str(c_tgt["RELIGION"]) and str(c_src["DISTRICT"]) == str(c_tgt["DISTRICT"]):
                net_gain = int(c_tgt["VOTES"]) - int(c_src["VOTES"])        #check if the swap would result in a net gain of votes for the target group
                if net_gain > 0:                
                    atomic.append({
                        "out": str(c_src["MEMBER"]), "in": str(c_tgt["MEMBER"]),
                        "from_list": str(c_tgt["GROUP"]), "net_gain": int(net_gain),
                        "religion": str(c_src["RELIGION"]), "district": str(c_src["DISTRICT"]),     #only allow swaps within the same religion and district
                    })

    atomic = sorted(atomic, key=lambda x: x["net_gain"], reverse=True)      #sort by net gain descending
    seen, uniq = set(), []
    for a in atomic:
        if (a["out"], a["in"]) not in seen:
            seen.add((a["out"], a["in"]))
            uniq.append(a)                  #only keep unique swaps (no duplicates)
    return uniq[:12]

def is_valid_combo(combo):
    used_out, used_in = set(), set()
    for s in combo:
        if s["out"] in used_out or s["in"] in used_in: return False
        used_out.add(s["out"]); used_in.add(s["in"])
    return True             #check that no candidate is swapped more than once in the same combo

def apply_swap_combo_to_copy(df: pd.DataFrame, combo):
    sim_df = df.copy()
    for s in combo:
        src_mask, tgt_mask = sim_df["MEMBER"] == s["out"], sim_df["MEMBER"] == s["in"]
        if not src_mask.any() or not tgt_mask.any(): return None
        src_idx, tgt_idx = sim_df.index[src_mask][0], sim_df.index[tgt_mask][0]
        src_group, tgt_group = sim_df.at[src_idx, "GROUP"], sim_df.at[tgt_idx, "GROUP"]
        if sim_df.at[src_idx, "RELIGION"] != sim_df.at[tgt_idx, "RELIGION"] or sim_df.at[src_idx, "DISTRICT"] != sim_df.at[tgt_idx, "DISTRICT"]:
            return None
        sim_df.loc[sim_df["MEMBER"] == s["out"], "GROUP"] = tgt_group
        sim_df.loc[sim_df["MEMBER"] == s["in"], "GROUP"] = src_group
    return sim_df           #simulate the effect of a swap combo on the DataFrame and return the modified copy

def simulate_combo_gain(file_id: str, df: pd.DataFrame, target_group: str, combo, baseline_seats: int):
    sim_df = apply_swap_combo_to_copy(df, combo)
    if sim_df is None: return None
    seats_after_map = count_group_winners(sim_df, _compute_winners_from_quota(file_id, sim_df))
    seats_after = int(seats_after_map.get(target_group, 0))
    return {"seat_gain": int(seats_after - baseline_seats), "seats_after": seats_after}     #simulate the effect of a swap combo on the number of seats won by the target group and return the result

def calculate_votes_needed_for_one_group(file_id, df, target_group, target_k):
    quota = get_quota(file_id)
    num_seats = sum(quota.get("rel_limits", {}).values())
    if num_seats == 0: return None

    df_group = group_vote_totals(file_id, df)
    valid_electoral = df_group["VOTES"].sum()
    if valid_electoral == 0: return None            #check if there are any valid votes in the election data; if not, return None

    eq1 = valid_electoral / num_seats
    df_group["USED"] = (df_group["VOTES"] >= eq1).astype(int)               #mark groups that have passed the electoral threshold based on the electoral quotient (eq1)
    unused_votes = df_group.loc[df_group["USED"] == 0, "VOTES"].sum()
    adjusted_electoral = valid_electoral - unused_votes
    if adjusted_electoral <= 0: return None             #check if there are any votes left after excluding unused groups; if not, return None

    eq2 = adjusted_electoral / num_seats
    df_adj = df_group[df_group["USED"] == 1].copy()
    df_adj["QUOTIENT"] = df_adj["VOTES"] / eq2
    df_adj["INITIAL_SEATS"] = df_adj["QUOTIENT"].apply(lambda x: math.floor(round(x, 9)))
    df_adj["ADVANTAGE"] = df_adj["QUOTIENT"] - df_adj["INITIAL_SEATS"]
    seats_left = int(num_seats - df_adj["INITIAL_SEATS"].sum())
    df_adj = df_adj.sort_values(by="ADVANTAGE", ascending=False).reset_index(drop=True)             #sort the adjusted groups by their advantage to determine which groups are closest to gaining additional seats

    df_adj["EXTRA_SEATS"] = 0
    for i in range(min(seats_left, len(df_adj))): df_adj.loc[i, "EXTRA_SEATS"] = 1
    group_seats_won = dict(zip(df_adj["GROUP"], df_adj["INITIAL_SEATS"] + df_adj["EXTRA_SEATS"]))               #create a dictionary mapping each group to the total number of seats they have won based on their initial seats and any extra seats they may have gained

    row = df_group[df_group["GROUP"] == target_group]
    if row.empty: return None

    v, passed = row.iloc[0]["VOTES"], row.iloc[0]["USED"] == 1
    current_seats_actual = int(get_current_group_seats(file_id, df).get(target_group, 0))
    current_seats_formula = int(group_seats_won.get(target_group, 0)) if passed else 0                      #calculate the current number of seats won by the target group based on both the actual election results and the formula used to determine seat allocation

    k = int(target_k)
    if current_seats_formula + k > num_seats: return None

    if seats_left > 0 and seats_left <= len(df_adj):
        cutoff_remainder = df_adj.loc[seats_left - 1, "ADVANTAGE"]
    elif seats_left > len(df_adj):
        cutoff_remainder = 0.0
    else:
        cutoff_remainder = 1.0

    if not passed:
        needed = eq1 - v if k == 1 else (k * eq1) - v
    else:
        needed = ((current_seats_formula + k - 1 + cutoff_remainder) * eq2) - v
    needed = int(max(1, math.ceil(needed)))                                             #calculate the number of additional votes needed for the target group to gain the desired number of seats (k), taking into account whether the group has passed the electoral threshold and the current seat allocation

    suggestions = find_min_vote_swaps(file_id, df, target_group, k, current_seats_actual, needed)

    return {"passed": bool(passed), "current_seats": current_seats_actual, "votes": needed, "suggestions": suggestions}


def find_min_vote_swaps(file_id, df, target_group, k, baseline_seats, needed, top_n=5):
    """Cheapest ways for `target_group` to gain at least `k` seats by
    swapping in candidates from other lists.

    A swap replaces one of the list's own candidates with an outside
    candidate of the SAME religion and SAME district (the only swaps the
    system allows). "Cheapest" means the fewest total votes carried in by
    the incoming candidates. Every incoming candidate is considered, not
    just the strongest ones, and solutions of 1, 2 or 3 swaps are compared
    together.
    """
    own = df[df["GROUP"] == target_group]
    others = df[df["GROUP"] != target_group]
    if own.empty or others.empty:
        return []

    swaps = []
    for _, src in own.iterrows():
        pool = others[
            (others["RELIGION"].astype(str) == str(src["RELIGION"]))
            & (others["DISTRICT"].astype(str) == str(src["DISTRICT"]))
            & (others["VOTES"] > src["VOTES"])
        ].sort_values("VOTES")
        if pool.empty:
            continue
        # Keep the cheapest few plus a spread up to the strongest, so both
        # cheap single swaps and bigger ones for multi-swap combos exist.
        keep = sorted({int(i) for i in np.linspace(0, len(pool) - 1, min(len(pool), 8))})
        for i in keep:
            tgt = pool.iloc[i]
            swaps.append({
                "out": str(src["MEMBER"]), "in": str(tgt["MEMBER"]),
                "out_id": src.get("CANDIDATE_ID"), "in_id": tgt.get("CANDIDATE_ID"),
                "from_list": strip_list_number(tgt["GROUP"]),
                "religion": str(src["RELIGION"]), "district": str(src["DISTRICT"]),
                "out_votes": int(src["VOTES"]), "in_votes": int(tgt["VOTES"]),
                "net_gain": int(tgt["VOTES"]) - int(src["VOTES"]),
            })
    if not swaps:
        return []

    def cost(combo):
        return sum(s["in_votes"] for s in combo)

    def key(s):
        return (s["out"], s["in"])

    found, valid_sets = [], []

    def try_combo(combo):
        sim = simulate_combo_gain(file_id, df, target_group, combo, baseline_seats)
        if sim and sim["seat_gain"] >= k:
            valid_sets.append({key(s) for s in combo})
            found.append({
                "type": f"{len(combo)}_swap", "seat_gain": sim["seat_gain"],
                "total_gain": sum(s["net_gain"] for s in combo),
                "total_votes": cost(combo), "swaps": list(combo),
            })

    def contains_valid_subset(combo):
        keys = {key(s) for s in combo}
        return any(v < keys for v in valid_sets)

    swaps.sort(key=lambda s: s["in_votes"])
    for s in swaps[:80]:
        try_combo([s])

    floor_gain = max(1, math.floor(needed * 0.5))
    for size, budget in ((2, 60), (3, 40)):
        combos = []
        for combo in combinations(swaps[:45], size):
            if not is_valid_combo(combo):
                continue
            if sum(s["net_gain"] for s in combo) < floor_gain:
                continue
            combos.append(combo)
        combos.sort(key=cost)
        tested = 0
        for combo in combos:
            if tested >= budget:
                break
            if len(found) >= top_n and cost(combo) >= sorted(f["total_votes"] for f in found)[top_n - 1]:
                break
            if contains_valid_subset(combo):
                continue
            tested += 1
            try_combo(combo)

    found.sort(key=lambda f: (f["total_votes"], len(f["swaps"])))
    unique, seen = [], set()
    for f in found:
        incoming = frozenset(s["in"] for s in f["swaps"])
        if incoming not in seen:      # same incoming candidates, just paired differently
            seen.add(incoming)
            unique.append(f)
    return unique[:top_n]


# =========================================================
#  ADDED FEATURE HELPERS: EXPORT + COMPARISON
# =========================================================
_LIST_NUMBER_RE = re.compile(r"^\(\s*\d+\s*\)\s*|\s*\(\s*\d+\s*\)\s*$")

def strip_list_number(name) -> str:
    """Ballot list names in the source data carry their official list-order
    number in parentheses (e.g. "طموح الشباب (3)", "( 4 ) إنقاذ وطن").
    Strip it for display -- the UI shows each list's rank instead."""
    text = str(name or "").strip()
    cleaned = _LIST_NUMBER_RE.sub("", text).strip()
    return cleaned or text

app.jinja_env.filters["clean_list_name"] = strip_list_number


def safe_sheet_name(name: str) -> str:
    cleaned = re.sub(r"[\\/*?:\[\]]", "_", str(name or "Sheet"))[:31]
    return cleaned or "Sheet"       #return a safe sheet name for Excel by replacing invalid characters and limiting the length to 31 characters

def safe_download_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", str(name or "election_report")).strip("_")           #return a safe download name for files by replacing invalid characters with underscores and stripping leading/trailing underscores

def get_region_by_file_id(file_id: str):
    for region in load_regions_from_database():
        if file_id in region.get("files", {}).values():
            return region["slug"], region
    return None, None

def build_report_tables(file_id: str):
    df = get_candidates_df(file_id)
    if df is None:
        return None

    working = df.copy()
    working["VOTES"] = pd.to_numeric(working["VOTES"], errors="coerce").fillna(0).astype(int)

    candidates = working.sort_values(["IS_WINNER", "VOTES"], ascending=[False, False])[
        ["MEMBER", "GROUP", "RELIGION", "DISTRICT", "VOTES", "IS_WINNER"]
    ]

    list_summary = working.groupby("GROUP", as_index=False).agg(
        TOTAL_VOTES=("VOTES", "sum"),
        CANDIDATES=("MEMBER", "count"),
        WINNERS=("IS_WINNER", "sum")
    ).sort_values("TOTAL_VOTES", ascending=False)

    district_summary = working.groupby(["DISTRICT", "RELIGION"], as_index=False).agg(
        TOTAL_VOTES=("VOTES", "sum"),
        CANDIDATES=("MEMBER", "count"),
        WINNERS=("IS_WINNER", "sum")
    ).sort_values(["DISTRICT", "RELIGION"])

    winners = candidates[candidates["IS_WINNER"] == True].copy()

    return {
        "candidates": candidates,
        "list_summary": list_summary,
        "district_summary": district_summary,
        "winners": winners,
    }                                               #return a dictionary containing DataFrames for candidates, list summary, district summary, and winners for a given file_id; return None if the candidates DataFrame is not available

def build_excel_report(file_id: str, district_label: str):
    """Build the multi-sheet Excel report for one district/year (current scenario).

    Returns a BytesIO, or None when the data is not available."""
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    df = get_candidates_df(file_id)
    if df is None:
        return None
    year, district_code = parse_file_id(file_id)
    df = df.copy()
    df["VOTES"] = pd.to_numeric(df["VOTES"], errors="coerce").fillna(0).astype(int)
    df["IS_WINNER"] = df["IS_WINNER"].astype(bool)
    df["LIST"] = df["GROUP"].map(strip_list_number)

    quota = get_quota(file_id)
    rel_limits = quota.get("rel_limits", {})
    num_seats = int(sum(rel_limits.values()))

    # ---- list-level allocation (same rules as _compute_winners_from_quota)
    totals = group_vote_totals(file_id, df)
    cand_votes = df.groupby("GROUP")["VOTES"].sum().to_dict()
    total_valid = int(totals["VOTES"].sum())
    q1 = total_valid / num_seats if num_seats else 0
    totals["QUALIFIED"] = totals["VOTES"] >= q1
    qualified_votes = int(totals.loc[totals["QUALIFIED"], "VOTES"].sum())
    q2 = qualified_votes / num_seats if num_seats else 0
    seats_by_list = df[df["IS_WINNER"]].groupby("GROUP").size().to_dict()

    list_rows = []
    for _, r in totals.sort_values("VOTES", ascending=False).iterrows():
        g, v, ok = r["GROUP"], int(r["VOTES"]), bool(r["QUALIFIED"])
        quotient = (v / q2) if (ok and q2) else None
        whole = math.floor(round(quotient, 9)) if quotient is not None else None
        won = int(seats_by_list.get(g, 0))
        list_rows.append({
            "Rank": len(list_rows) + 1,
            "List": strip_list_number(g),
            "Candidates": int((df["GROUP"] == g).sum()),
            "Candidate votes": int(cand_votes.get(g, 0)),
            "List-only votes": v - int(cand_votes.get(g, 0)),
            "Total votes": v,
            "% of valid votes": v / total_valid if total_valid else 0,
            "Passed threshold": "Yes" if ok else "No",
            "Quotient (votes / Q2)": round(quotient, 4) if quotient is not None else None,
            "Whole seats": whole,
            "Remainder": round(quotient - whole, 4) if quotient is not None else None,
            "Seats won": won,
            "Votes per seat": round(v / won) if won else None,
        })
    lists_df = pd.DataFrame(list_rows)

    # ---- candidates
    list_total = df.groupby("GROUP")["VOTES"].transform("sum")
    seat_total = df.groupby(["DISTRICT", "RELIGION"])["VOTES"].transform("sum")
    qualified_groups = set(totals.loc[totals["QUALIFIED"], "GROUP"])
    qual_mask = df["GROUP"].isin(qualified_groups)
    qada_total = df[qual_mask].groupby("DISTRICT")["VOTES"].sum().to_dict()
    df["PCT_LIST"] = (df["VOTES"] / list_total.where(list_total > 0)).fillna(0)
    df["PCT_SEAT"] = (df["VOTES"] / seat_total.where(seat_total > 0)).fillna(0)
    df["PCT_ALLOC"] = [
        (v / qada_total[d]) if (q and qada_total.get(d)) else None
        for v, d, q in zip(df["VOTES"], df["DISTRICT"], qual_mask)
    ]
    df["RANK_SEAT"] = df.groupby(["DISTRICT", "RELIGION"])["VOTES"].rank(method="min", ascending=False).astype(int)

    original = load_original_df(file_id)
    orig_group = dict(zip(original["CANDIDATE_ID"], original["GROUP"])) if original is not None else {}
    df["ORIG_LIST"] = [strip_list_number(orig_group.get(cid, g)) for cid, g in zip(df["CANDIDATE_ID"], df["GROUP"])]
    df["MOVED"] = df["ORIG_LIST"] != df["LIST"]
    party_map = get_party_map(file_id)
    df["PARTY"] = [party_map.get(str(cid), "") for cid in df["CANDIDATE_ID"]]

    def cand_table(frame):
        return pd.DataFrame({
            "Candidate": frame["MEMBER"],
            "Party": frame["PARTY"],
            "List": frame["LIST"],
            "Sect": frame["RELIGION"],
            "District": frame["DISTRICT"],
            "Votes": frame["VOTES"],
            "% of list": frame["PCT_LIST"],
            "% of seat votes": frame["PCT_SEAT"],
            "Allocation % (district)": frame["PCT_ALLOC"],
            "Rank for seat": frame["RANK_SEAT"],
            "Result": ["Winner" if w else "Not elected" for w in frame["IS_WINNER"]],
            "Original list": frame["ORIG_LIST"],
            "Moved": ["Yes" if m else "" for m in frame["MOVED"]],
        })

    candidates_df = cand_table(df.sort_values(["IS_WINNER", "VOTES"], ascending=[False, False]))
    winners_df = cand_table(df[df["IS_WINNER"]].sort_values(["DISTRICT", "RELIGION", "VOTES"], ascending=[True, True, False]))

    # ---- seats (sect x district)
    seat_rows = []
    for (dist, rel), g in df.groupby(["DISTRICT", "RELIGION"]):
        w = g[g["IS_WINNER"]].sort_values("VOTES", ascending=False)
        losers = g[~g["IS_WINNER"]].sort_values("VOTES", ascending=False)
        margin = None
        if not w.empty and not losers.empty:
            margin = int(w["VOTES"].min()) - int(losers.iloc[0]["VOTES"])
        seat_rows.append({
            "District": dist, "Sect": rel,
            "Seats": int(rel_limits.get((normalize_arabic(dist), normalize_arabic(rel)), 0)),
            "Candidates": len(g), "Total votes": int(g["VOTES"].sum()),
            "Winner(s)": "، ".join(f"{n} ({l})" for n, l in zip(w["MEMBER"], w["LIST"])),
            "Lowest winner votes": int(w["VOTES"].min()) if not w.empty else None,
            "Best non-elected": losers.iloc[0]["MEMBER"] if not losers.empty else None,
            "Best non-elected votes": int(losers.iloc[0]["VOTES"]) if not losers.empty else None,
            "Vote gap (winner - best non-elected)": margin,
        })
    seats_df = pd.DataFrame(seat_rows)

    # ---- scenario changes against the original lists
    changes_df = pd.DataFrame({
        "Candidate": df.loc[df["MOVED"], "MEMBER"], "Sect": df.loc[df["MOVED"], "RELIGION"],
        "District": df.loc[df["MOVED"], "DISTRICT"], "Votes": df.loc[df["MOVED"], "VOTES"],
        "Original list": df.loc[df["MOVED"], "ORIG_LIST"], "Current list": df.loc[df["MOVED"], "LIST"],
        "Result now": ["Winner" if w else "Not elected" for w in df.loc[df["MOVED"], "IS_WINNER"]],
    })
    seat_change_df = pd.DataFrame()
    if original is not None:
        orig_winners = _compute_winners_from_quota(file_id, original)
        orig_seats = original[original["CANDIDATE_ID"].isin(orig_winners)].groupby("GROUP").size().to_dict()
        orig_votes = group_vote_totals(file_id, original)
        orig_votes = dict(zip(orig_votes["GROUP"], orig_votes["VOTES"].astype(int)))
        cur_votes = dict(zip(totals["GROUP"], totals["VOTES"].astype(int)))
        rows = []
        for g in sorted(set(orig_votes) | set(cur_votes), key=lambda x: -cur_votes.get(x, 0)):
            rows.append({
                "List": strip_list_number(g),
                "Original votes": orig_votes.get(g, 0), "Current votes": cur_votes.get(g, 0),
                "Vote change": cur_votes.get(g, 0) - orig_votes.get(g, 0),
                "Original seats": int(orig_seats.get(g, 0)), "Current seats": int(seats_by_list.get(g, 0)),
                "Seat change": int(seats_by_list.get(g, 0)) - int(orig_seats.get(g, 0)),
            })
        seat_change_df = pd.DataFrame(rows)

    # ---- areas (from the ballot-pen data; original lists)
    areas_df = pd.DataFrame()
    try:
        with DB_ENGINE.connect() as conn:
            pen = pd.read_sql(text("""
                SELECT v.neighborhood AS area, v.ballot_pen AS pen, v.candidate_id AS cid, v.votes
                FROM election_pen_votes v
                WHERE v.year = :year AND v.district_code = :d
                  AND v.neighborhood IS NOT NULL AND v.neighborhood NOT IN ('', 'nan')
            """), conn, params={"year": year, "d": district_code})
        if not pen.empty:
            pen["list"] = pen["cid"].map(dict(zip(df["CANDIDATE_ID"].astype(str), df["ORIG_LIST"])))
            pen["name"] = pen["cid"].map(dict(zip(df["CANDIDATE_ID"].astype(str), df["MEMBER"])))
            rows = []
            for area, g in pen.groupby("area"):
                by_list = g.groupby("list")["votes"].sum().sort_values(ascending=False)
                by_cand = g.groupby("name")["votes"].sum().sort_values(ascending=False)
                tot = int(g["votes"].sum())
                rows.append({
                    "City / area": area, "Ballot pens": g["pen"].nunique(), "Candidate votes": tot,
                    "Leading list": by_list.index[0] if len(by_list) else None,
                    "Leading list votes": int(by_list.iloc[0]) if len(by_list) else None,
                    "Leading list share": (by_list.iloc[0] / tot) if tot and len(by_list) else None,
                    "Second list": by_list.index[1] if len(by_list) > 1 else None,
                    "Second list votes": int(by_list.iloc[1]) if len(by_list) > 1 else None,
                    "Top candidate": by_cand.index[0] if len(by_cand) else None,
                    "Top candidate votes": int(by_cand.iloc[0]) if len(by_cand) else None,
                })
            areas_df = pd.DataFrame(rows).sort_values("Candidate votes", ascending=False)
    except Exception as exc:
        print(f"Excel export: area sheet skipped ({exc})")

    moved_count = int(df["MOVED"].sum())
    summary_rows = [
        ("District", district_label), ("Election year", year),
        ("Scenario", "Original lists" if moved_count == 0 else f"Modified — {moved_count} candidate(s) moved"),
        ("Generated", datetime.now().strftime("%Y-%m-%d %H:%M")),
        ("", ""),
        ("Seats", num_seats), ("Lists", int(len(totals))), ("Candidates", int(len(df))),
        ("Candidate (preferential) votes", int(df["VOTES"].sum())),
        ("List-only votes", total_valid - int(df["VOTES"].sum())),
        ("Total valid list votes", total_valid),
        ("", ""),
        ("First electoral quotient (Q1 = votes / seats)", round(q1, 2)),
        ("Lists that passed Q1", int(totals["QUALIFIED"].sum())),
        ("Votes of qualified lists", qualified_votes),
        ("Second quotient (Q2 = qualified votes / seats)", round(q2, 2)),
        ("", ""),
        ("Note", "Blank and invalid ballots are not included in these totals."),
        ("Note", "Allocation % = candidate votes / votes of qualified lists' candidates in the same district."),
    ]
    summary_df = pd.DataFrame(summary_rows, columns=["Item", "Value"])

    sheets = [("Summary", summary_df), ("Lists", lists_df), ("Winners", winners_df),
              ("Candidates", candidates_df), ("Seats", seats_df)]
    if not seat_change_df.empty:
        sheets.append(("Scenario - lists", seat_change_df))
    if not changes_df.empty:
        sheets.append(("Scenario - moves", changes_df))
    if not areas_df.empty:
        sheets.append(("Areas", areas_df))
    if party_map:
        known = df[df["PARTY"] != ""]
        party_rows = []
        for party, g in known.groupby("PARTY"):
            top = g.sort_values("VOTES", ascending=False).iloc[0]
            party_rows.append({
                "Party": party, "Candidates": len(g), "Votes": int(g["VOTES"].sum()),
                "% of candidate votes": g["VOTES"].sum() / max(1, int(df["VOTES"].sum())),
                "Seats won": int(g["IS_WINNER"].sum()),
                "Winners": "، ".join(g[g["IS_WINNER"]].sort_values("VOTES", ascending=False)["MEMBER"]),
                "Top candidate": top["MEMBER"], "Top candidate votes": int(top["VOTES"]),
            })
        unknown = df[df["PARTY"] == ""]
        if len(unknown):
            party_rows.append({
                "Party": "(unknown)", "Candidates": len(unknown), "Votes": int(unknown["VOTES"].sum()),
                "% of candidate votes": unknown["VOTES"].sum() / max(1, int(df["VOTES"].sum())),
                "Seats won": int(unknown["IS_WINNER"].sum()),
                "Winners": "، ".join(unknown[unknown["IS_WINNER"]]["MEMBER"]),
                "Top candidate": None, "Top candidate votes": None,
            })
        parties_df = pd.DataFrame(party_rows).sort_values(["Seats won", "Votes"], ascending=False)
        sheets.insert(5, ("Parties", parties_df))

    head_fill = PatternFill("solid", fgColor="1F4E78")
    win_fill = PatternFill("solid", fgColor="E2F0D9")
    bad_fill = PatternFill("solid", fgColor="FCE4D6")
    thin = Side(style="thin", color="D9D9D9")
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, frame in sheets:
            frame.to_excel(writer, index=False, sheet_name=name)
            ws = writer.sheets[name]
            headers = [c.value for c in ws[1]]
            for cell in ws[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = head_fill
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            ws.row_dimensions[1].height = 32
            ws.freeze_panes = "A2"
            if name != "Summary" and ws.max_row > 1:
                ws.auto_filter.ref = ws.dimensions
            for idx, header in enumerate(headers, 1):
                letter = get_column_letter(idx)
                h = str(header or "")
                is_pct = h.startswith("%") or "share" in h.lower() or h.startswith("Allocation %")
                longest = len(h)
                for (cell,) in ws.iter_rows(min_row=2, min_col=idx, max_col=idx):
                    cell.border = Border(bottom=thin)
                    if cell.value is None:
                        continue
                    if is_pct and isinstance(cell.value, (int, float)):
                        cell.number_format = "0.00%"
                    elif isinstance(cell.value, int) and not isinstance(cell.value, bool):
                        cell.number_format = "#,##0"
                    longest = max(longest, len(str(cell.value)))
                ws.column_dimensions[letter].width = max(10, min(longest + 3, 48))
            # row highlights
            if "Result" in headers or "Result now" in headers:
                col = headers.index("Result" if "Result" in headers else "Result now") + 1
                for row in ws.iter_rows(min_row=2):
                    if row[col - 1].value == "Winner":
                        for cell in row: cell.fill = win_fill
            if "Passed threshold" in headers:
                col = headers.index("Passed threshold") + 1
                for row in ws.iter_rows(min_row=2):
                    fill = win_fill if row[col - 1].value == "Yes" else bad_fill
                    row[col - 1].fill = fill
            if "Seat change" in headers:
                col = headers.index("Seat change") + 1
                for row in ws.iter_rows(min_row=2):
                    v = row[col - 1].value or 0
                    if v > 0: row[col - 1].fill = win_fill
                    elif v < 0: row[col - 1].fill = bad_fill
            if name == "Summary":
                ws.column_dimensions["A"].width = 46
                ws.column_dimensions["B"].width = 60
                for row in ws.iter_rows(min_row=2):
                    row[0].font = Font(bold=True)
                    row[1].alignment = Alignment(horizontal="left", wrap_text=True)
    output.seek(0)
    return output


def make_compare_payload(slug: str):
    info = get_region_from_database(slug)
    if info is None:
        return None

    payload = {"region": info, "slug": slug, "years": {}, "groups": [], "winners": [], "chart_json": "{}"}

    for year in ["2018", "2022"]:
        file_id = info.get("files", {}).get(year)
        df = get_candidates_df(file_id) if file_id else None
        if df is None:
            payload["years"][year] = {"available": False, "file_id": file_id}
            continue

        total_votes = int(df["VOTES"].sum())
        total_candidates = int(len(df))
        total_lists = int(df["GROUP"].nunique())
        total_winners = int(df["IS_WINNER"].sum())
        top_candidate_row = df.sort_values("VOTES", ascending=False).head(1)
        top_candidate = None
        if not top_candidate_row.empty:
            r = top_candidate_row.iloc[0]
            top_candidate = {"name": str(r["MEMBER"]), "votes": int(r["VOTES"]), "group": str(r["GROUP"])}

        group_summary = df.groupby("GROUP", as_index=False).agg(
            TOTAL_VOTES=("VOTES", "sum"), WINNERS=("IS_WINNER", "sum"), CANDIDATES=("MEMBER", "count")
        )
        winner_names = df[df["IS_WINNER"] == True][["MEMBER", "GROUP", "VOTES"]].sort_values("VOTES", ascending=False)

        payload["years"][year] = {
            "available": True,
            "file_id": file_id,
            "total_votes": total_votes,
            "total_candidates": total_candidates,
            "total_lists": total_lists,
            "total_winners": total_winners,
            "top_candidate": top_candidate,
            "groups_df": group_summary,
            "winners_df": winner_names,
        }

    y18 = payload["years"].get("2018", {})
    y22 = payload["years"].get("2022", {})
    if y18.get("available") or y22.get("available"):
        g18 = y18.get("groups_df", pd.DataFrame(columns=["GROUP", "TOTAL_VOTES", "WINNERS", "CANDIDATES"])).rename(
            columns={"TOTAL_VOTES": "votes_2018", "WINNERS": "winners_2018", "CANDIDATES": "candidates_2018"}
        )
        g22 = y22.get("groups_df", pd.DataFrame(columns=["GROUP", "TOTAL_VOTES", "WINNERS", "CANDIDATES"])).rename(
            columns={"TOTAL_VOTES": "votes_2022", "WINNERS": "winners_2022", "CANDIDATES": "candidates_2022"}
        )
        merged = pd.merge(g18, g22, on="GROUP", how="outer").fillna(0)
        for col in ["votes_2018", "winners_2018", "candidates_2018", "votes_2022", "winners_2022", "candidates_2022"]:
            merged[col] = merged[col].astype(int)
        merged["vote_change"] = merged["votes_2022"] - merged["votes_2018"]
        merged["winner_change"] = merged["winners_2022"] - merged["winners_2018"]
        merged = merged.sort_values("votes_2022", ascending=False)
        payload["groups"] = merged.to_dict(orient="records")

        chart_labels = [strip_list_number(g) for g in merged["GROUP"].tolist()]
        fig = go.Figure()
        fig.add_bar(name="2018", x=chart_labels, y=merged["votes_2018"].tolist())
        fig.add_bar(name="2022", x=chart_labels, y=merged["votes_2022"].tolist())
        fig.update_layout(
            barmode="group", height=420, margin=dict(l=30, r=20, t=30, b=110),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            yaxis_title="Votes", xaxis_tickangle=-35, legend_title="Year"
        )
        payload["chart_json"] = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

        # The same person is often spelled slightly differently between the
        # two years' source files (e.g. كرامى vs كرامي), so match winners on a
        # normalized key rather than the exact text.
        def winners_by_key(year_info):
            if not year_info.get("available"):
                return {}
            names = year_info.get("winners_df", pd.DataFrame(columns=["MEMBER"]))["MEMBER"].astype(str).tolist()
            return {normalize_arabic(n).replace(" ", ""): n for n in names}

        w18, w22 = winners_by_key(y18), winners_by_key(y22)
        payload["winner_changes"] = {
            "kept": sorted(w22[k] for k in w18.keys() & w22.keys()),
            "new": sorted(w22[k] for k in w22.keys() - w18.keys()),
            "lost": sorted(w18[k] for k in w18.keys() - w22.keys()),
        }

    return payload

def build_national_payload(year: str):
    """Aggregate seats/votes per list across every district for one year.

    Winners are computed live by get_candidates_df(), so this reflects
    whatever what-if edits are currently applied per district, not just
    the original imported results.
    """
    district_rows = []
    list_totals = {}
    winners_rows = []
    total_seats = 0
    total_votes = 0
    districts_included = 0

    for region in deduplicated_display_regions(load_regions_from_database()):
        label = region["district_label"]
        file_id = region["files"].get(year)
        df = get_candidates_df(file_id) if file_id else None
        if df is None or df.empty:
            district_rows.append({"slug": region["slug"], "label": label, "available": False})
            continue

        districts_included += 1
        district_seats = int(df["IS_WINNER"].sum())
        district_votes = int(df["VOTES"].sum())
        total_seats += district_seats
        total_votes += district_votes

        group_agg = df.groupby("GROUP", as_index=False).agg(
            VOTES=("VOTES", "sum"), SEATS=("IS_WINNER", "sum"), CANDIDATES=("MEMBER", "count")
        )

        top_list = None
        winning_rows = group_agg[group_agg["SEATS"] > 0].sort_values(["SEATS", "VOTES"], ascending=False)
        if not winning_rows.empty:
            r = winning_rows.iloc[0]
            top_list = {"name": str(r["GROUP"]), "seats": int(r["SEATS"])}

        district_rows.append({
            "slug": region["slug"], "label": label, "available": True,
            "seats": district_seats, "votes": district_votes, "top_list": top_list,
        })

        winner_df = df[df["IS_WINNER"] == True].sort_values("VOTES", ascending=False)
        for _, r in winner_df.iterrows():
            winners_rows.append({
                "name": str(r["MEMBER"]),
                "group": str(r["GROUP"]),
                "votes": int(r["VOTES"]),
                "religion": str(r["RELIGION"]),
                "qada": str(r["DISTRICT"]),
                "region_label": label,
                "region_slug": region["slug"],
            })

        for _, row in group_agg.iterrows():
            name = str(row["GROUP"])
            entry = list_totals.setdefault(name, {"votes": 0, "seats": 0, "candidates": 0, "districts": set()})
            entry["votes"] += int(row["VOTES"])
            entry["seats"] += int(row["SEATS"])
            entry["candidates"] += int(row["CANDIDATES"])
            entry["districts"].add(label)

    list_rows = []
    for name, data in list_totals.items():
        list_rows.append({
            "name": name,
            "votes": data["votes"],
            "seats": data["seats"],
            "candidates": data["candidates"],
            "district_count": len(data["districts"]),
            "vote_share": round((data["votes"] / total_votes) * 100, 2) if total_votes else 0.0,
        })
    list_rows.sort(key=lambda r: (r["seats"], r["votes"]), reverse=True)
    winners_rows.sort(key=lambda r: (r["region_label"], -r["votes"]))

    chart_json = "{}"
    seat_winners = [r for r in list_rows if r["seats"] > 0][:20]
    if seat_winners:
        ordered = list(reversed(seat_winners))
        fig = go.Figure(go.Bar(
            x=[r["seats"] for r in ordered],
            y=[strip_list_number(r["name"]) for r in ordered],
            orientation="h",
            text=[r["seats"] for r in ordered],
            textposition="outside",
            marker_color="#0d6efd",
            cliponaxis=False,
        ))
        fig.update_layout(
            height=max(320, len(ordered) * 32 + 60),
            margin=dict(l=10, r=40, t=10, b=10),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(automargin=True, tickfont=dict(size=11, color="#212529")),
        )
        chart_json = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    return {
        "year": year,
        "district_rows": district_rows,
        "list_rows": list_rows,
        "winners_rows": winners_rows,
        "total_seats": total_seats,
        "total_votes": total_votes,
        "districts_included": districts_included,
        "districts_total": len(district_rows),
        "chart_json": chart_json,
    }

# =========================================================
#  ROUTES
# =========================================================
@app.route("/")
def index():
    selected_year = request.args.get("year", "2022")
    if selected_year not in {"2018", "2022"}:
        selected_year = "2022"

    all_regions = load_regions_from_database()
    if not all_regions:
        return (
            "Map data is missing from PostgreSQL. Run migrations/import_from_excel.py "
            "to populate election_seats/election_members/election_votes from the "
            "source Excel files (election_regions -- the map geometry table -- must "
            "already be seeded separately).",
            500,
        )

    # Always render every active map polygon so Lebanon keeps its complete shape.
    # Only the sidebar/navigation is filtered to districts that have complete
    # election data for the selected year.
    map_regions = all_regions
    available_regions = [
        region for region in all_regions
        if selected_year in region["files"]
    ]

    unique_districts = [
        {**region, "active_year": selected_year}
        for region in deduplicated_display_regions(available_regions)
    ]

    return render_template(
        "index.html",
        map_json=create_interactive_map(map_regions, selected_year),
        regions=unique_districts,
        available_slugs=[region["slug"] for region in available_regions],
        current_year=selected_year,
    )

@app.route("/region/<slug>")
def region_detail(slug):
    info = get_region_from_database(slug)
    if info is None:
        return abort(404)
    selected_year = request.args.get("year", "2022")
    file_id = info["files"].get(selected_year)
    if not file_id:
        return "Election data not found in PostgreSQL.", 404
    df = get_candidates_df(file_id)
    if df is None:
        return "Error loading election data from PostgreSQL.", 500

    total_seats = sum(get_quota(file_id).get("rel_limits", {}).values())

    # Group by candidate_id, not member name: two candidates can share the
    # exact same name (confirmed in this dataset), and grouping by name
    # would silently merge them into a single card, hiding one entirely.
    df_agg = df.groupby("CANDIDATE_ID", as_index=False).agg({"MEMBER": "first", "GROUP": "first", "RELIGION": "first", "DISTRICT": "first", "IS_WINNER": "max", "VOTES": "sum"})

    party_map = get_party_map(file_id)
    candidates = [{"id": str(r["CANDIDATE_ID"]), "name": str(r["MEMBER"]), "group": str(r["GROUP"]), "votes": int(r["VOTES"]), "religion": str(r["RELIGION"]), "district": str(r["DISTRICT"]), "is_winner": bool(r["IS_WINNER"]), "party": party_map.get(str(r["CANDIDATE_ID"]), "")} for _, r in df_agg.iterrows()]

    grouped = {}
    for c in candidates: grouped.setdefault(c["group"], []).append(c)
    for grp in grouped: grouped[grp].sort(key=lambda x: x["votes"], reverse=True)
    grouped = dict(sorted(grouped.items(), key=lambda i: sum(x["votes"] for x in i[1]), reverse=True))

    charts_by_district = {}
    districts = df_agg["DISTRICT"].unique() if len(df_agg["DISTRICT"].unique()) > 0 else ["General"]
    chart_global_counter = 0

    for district_name in districts:
        d_df = df_agg[df_agg["DISTRICT"] == district_name]
        district_charts = []
        for religion in d_df["RELIGION"].unique():
            r_df_all = d_df[d_df["RELIGION"] == religion].copy().sort_values("VOTES", ascending=False)
            if r_df_all.empty: continue
            threshold = r_df_all.iloc[0]["VOTES"] * 0.20
            r_df_all = r_df_all.reset_index(drop=True)
            visible_mask = (r_df_all.index < 5) | (r_df_all["VOTES"] >= threshold) | (r_df_all["IS_WINNER"] == True)
            r_df_visible = r_df_all[visible_mask].copy().sort_values("VOTES", ascending=True)
            r_df_all_sorted = r_df_all.sort_values("VOTES", ascending=True)

            def create_fig(dataframe):
                chart_height = max(160, (len(dataframe) * 35) + 40)
                fig = go.Figure(go.Bar(
                    x=dataframe["VOTES"].tolist(), y=dataframe["MEMBER"].tolist(), orientation="h",
                    marker_color=["#1b5e20" if w else "#cfd8dc" for w in dataframe["IS_WINNER"]],
                    text=[f"{int(v):,}" for v in dataframe["VOTES"]], textposition="outside", cliponaxis=False
                ))
                fig.update_layout(
                    height=chart_height, title="", showlegend=False, margin=dict(l=150, r=50, t=10, b=10),
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", bargap=0.3,
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(automargin=False, tickfont=dict(size=12, color="#212529"))
                )
                return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

            chart_global_counter += 1
            district_charts.append({
                "id": f"chart_{chart_global_counter}", "title": religion,
                "json_initial": create_fig(r_df_visible), "json_full": create_fig(r_df_all_sorted),
                "has_more": len(r_df_all) > len(r_df_visible)
            })
        charts_by_district[district_name] = district_charts

    all_candidates_sorted = sorted(df_agg["MEMBER"].astype(str).tolist())

    return render_template(
        "detail.html", region_name=info["name"], sub_region_name=info["district_label"],
        groups=grouped, file_id=file_id, current_year=selected_year,
        charts_by_district=charts_by_district, total_seats=total_seats,
        region_slug=slug, all_candidates=all_candidates_sorted
    )

@app.route("/compare/<slug>")
def compare_years(slug):
    payload = make_compare_payload(slug)
    if payload is None:
        return abort(404)
    return render_template("compare.html", **payload)

@app.route("/candidate")
def candidate_pens():
    """Top ballot pens (أقلام الاقتراع) that voted for one candidate."""
    file_id = request.args.get("file_id", "")
    candidate_id = request.args.get("candidate_id", "")
    year, district_code = parse_file_id(file_id)
    df = get_candidates_df(file_id)
    if df is None or year is None:
        return "Data not found.", 404
    match = df[df["CANDIDATE_ID"].astype(str) == candidate_id]
    if match.empty:
        return "Candidate not found.", 404
    cand = match.iloc[0]

    stmt = text("""
        WITH totals AS (
            SELECT ballot_pen, SUM(votes)::bigint AS pen_total
            FROM election_pen_votes
            WHERE year = :year AND district_code = :d
            GROUP BY ballot_pen
        )
        SELECT v.ballot_pen, MAX(v.neighborhood) AS neighborhood, MAX(v.voting_place) AS voting_place,
               SUM(v.votes)::bigint AS votes, MAX(t.pen_total) AS pen_total
        FROM election_pen_votes v JOIN totals t USING (ballot_pen)
        WHERE v.year = :year AND v.district_code = :d AND v.candidate_id = :cid
        GROUP BY v.ballot_pen
        ORDER BY votes DESC, v.ballot_pen
    """)
    try:
        with DB_ENGINE.connect() as conn:
            rows = conn.execute(stmt, {"year": year, "d": district_code, "cid": candidate_id}).mappings().all()
    except Exception as exc:
        print(f"Pen ranking failed: {exc}")
        rows = []

    pens_total_votes = sum(int(r["votes"]) for r in rows)
    top = [{
        "rank": i,
        "pen": r["ballot_pen"],
        "place": " - ".join(x for x in (r["neighborhood"], r["voting_place"]) if x and x != "nan"),
        "votes": int(r["votes"]),
        "share": round(int(r["votes"]) / max(1, int(r["pen_total"])) * 100, 1),
        "of_candidate": round(int(r["votes"]) / max(1, pens_total_votes) * 100, 1),
    } for i, r in enumerate(rows[:10], 1)]

    areas_stmt = text("""
        WITH totals AS (
            SELECT neighborhood, SUM(votes)::bigint AS area_total
            FROM election_pen_votes
            WHERE year = :year AND district_code = :d
            GROUP BY neighborhood
        )
        SELECT v.neighborhood, SUM(v.votes)::bigint AS votes, MAX(t.area_total) AS area_total,
               COUNT(DISTINCT v.ballot_pen) AS pens
        FROM election_pen_votes v JOIN totals t USING (neighborhood)
        WHERE v.year = :year AND v.district_code = :d AND v.candidate_id = :cid
          AND v.neighborhood IS NOT NULL AND v.neighborhood NOT IN ('', 'nan')
        GROUP BY v.neighborhood
        ORDER BY votes DESC, v.neighborhood
    """)
    try:
        with DB_ENGINE.connect() as conn:
            area_rows = conn.execute(areas_stmt, {"year": year, "d": district_code, "cid": candidate_id}).mappings().all()
    except Exception as exc:
        print(f"Area ranking failed: {exc}")
        area_rows = []
    cand_total = max(1, int(cand["VOTES"]))
    areas = [{
        "rank": i, "name": r["neighborhood"], "votes": int(r["votes"]), "pens": int(r["pens"]),
        "share": round(int(r["votes"]) / max(1, int(r["area_total"])) * 100, 1),
        "of_candidate": round(int(r["votes"]) / cand_total * 100, 1),
    } for i, r in enumerate(area_rows[:10], 1)]

    slug, region = get_region_by_file_id(file_id)
    return render_template(
        "candidate.html", areas=areas, candidate_id=candidate_id, year=year, slug=slug, file_id=file_id,
        candidate={
            "name": str(cand["MEMBER"]), "group": strip_list_number(cand["GROUP"]),
            "religion": str(cand["RELIGION"]), "district": str(cand["DISTRICT"]),
            "party": get_party_map(file_id).get(str(cand["CANDIDATE_ID"]), ""),
            "votes": int(cand["VOTES"]), "is_winner": bool(cand["IS_WINNER"]),
        },
        pens=top, pens_count=len(rows), pens_total_votes=pens_total_votes,
    )

def _pen_context(file_id):
    year, district_code = parse_file_id(file_id)
    if year is None:
        return None, None, None
    slug, _region = get_region_by_file_id(file_id)
    return year, district_code, slug


@app.route("/area")
def area_lists():
    """Lists (and ballot pens) for one city/area such as التبانة."""
    file_id = request.args.get("file_id", "")
    area = request.args.get("area", "")
    candidate_id = request.args.get("candidate_id", "")
    year, district_code, slug = _pen_context(file_id)
    if year is None or not area:
        return "Data not found.", 404
    params = {"year": year, "d": district_code, "area": area}
    lists_stmt = text("""
        SELECT COALESCE(NULLIF(m.original_group_name, ''), m.group_name) AS grp,
               SUM(v.votes)::bigint AS votes, COUNT(DISTINCT v.candidate_id) AS candidates
        FROM election_pen_votes v
        JOIN election_members m ON m.year = v.year AND m.district_code = v.district_code AND m.candidate_id = v.candidate_id
        WHERE v.year = :year AND v.district_code = :d AND v.neighborhood = :area
        GROUP BY grp ORDER BY votes DESC
    """)
    pens_stmt = text("""
        SELECT ballot_pen, MAX(voting_place) AS place, SUM(votes)::bigint AS votes
        FROM election_pen_votes
        WHERE year = :year AND district_code = :d AND neighborhood = :area
        GROUP BY ballot_pen ORDER BY votes DESC, ballot_pen
    """)
    with DB_ENGINE.connect() as conn:
        list_rows = conn.execute(lists_stmt, params).mappings().all()
        pen_rows = conn.execute(pens_stmt, params).mappings().all()
    total = sum(int(r["votes"]) for r in list_rows)
    lists = [{
        "rank": i, "name": strip_list_number(r["grp"]), "votes": int(r["votes"]),
        "candidates": int(r["candidates"]), "share": round(int(r["votes"]) / max(1, total) * 100, 1),
    } for i, r in enumerate(list_rows, 1)]
    pens = [{"pen": r["ballot_pen"], "place": r["place"] or "", "votes": int(r["votes"])} for r in pen_rows]
    return render_template("area.html", year=year, slug=slug, file_id=file_id, area=area,
                           candidate_id=candidate_id, lists=lists, pens=pens, total=total)


@app.route("/pen")
def pen_voters():
    """One ballot pen: results by list, and the registered voters of its town."""
    file_id = request.args.get("file_id", "")
    pen = request.args.get("pen", "")
    area = request.args.get("area", "")
    candidate_id = request.args.get("candidate_id", "")
    q = request.args.get("q", "").strip()
    sect = request.args.get("sect", "").strip()
    try:
        page = max(1, int(request.args.get("page", "1")))
    except ValueError:
        page = 1
    per_page = 50
    year, district_code, slug = _pen_context(file_id)
    if year is None or not pen:
        return "Data not found.", 404

    with DB_ENGINE.connect() as conn:
        lists = conn.execute(text("""
            SELECT COALESCE(NULLIF(m.original_group_name, ''), m.group_name) AS grp, SUM(v.votes)::bigint AS votes
            FROM election_pen_votes v
            JOIN election_members m ON m.year = v.year AND m.district_code = v.district_code AND m.candidate_id = v.candidate_id
            WHERE v.year = :year AND v.district_code = :d AND v.ballot_pen = :pen
            GROUP BY grp ORDER BY votes DESC
        """), {"year": year, "d": district_code, "pen": pen}).mappings().all()
        if not area:
            row = conn.execute(text("""
                SELECT MAX(neighborhood) AS n FROM election_pen_votes
                WHERE year = :year AND district_code = :d AND ballot_pen = :pen
            """), {"year": year, "d": district_code, "pen": pen}).mappings().first()
            area = (row or {}).get("n") or ""

        # election_pen_votes.neighborhood (2022 ballot export) and
        # voters_2026.town (2026 registry) disagree on presentation-form
        # ligatures/spacing, so match on the normalized column, not the raw text.
        area_norm = norm_place(area)
        where = "town_norm = :town_norm"
        vparams = {"town_norm": area_norm}
        if q:
            where += " AND (first_name || ' ' || family_name || ' ' || father_name) ILIKE :q"
            vparams["q"] = f"%{q}%"
        if sect:
            where += " AND personal_sect = :sect"
            vparams["sect"] = sect
        try:
            total_voters = conn.execute(text(f"SELECT COUNT(*) FROM voters_2026 WHERE {where}"), vparams).scalar() or 0
            # JSON extraction returns NULL before the phone migration is run,
            # keeping existing voter tables readable during the upgrade.
            voters = conn.execute(text(f"""
                SELECT first_name, family_name, father_name, mother_name, birth_date, personal_sect, gender, registry_number,
                       to_jsonb(v)->>'phone_number' AS phone_number
                FROM voters_2026 v WHERE {where}
                ORDER BY family_name, first_name, id LIMIT :lim OFFSET :off
            """), {**vparams, "lim": per_page, "off": (page - 1) * per_page}).mappings().all()
            sects = [r[0] for r in conn.execute(text(
                "SELECT DISTINCT personal_sect FROM voters_2026 WHERE town_norm = :town_norm ORDER BY 1"),
                {"town_norm": area_norm}).all() if r[0]]
        except Exception as exc:
            print(f"Voter lookup failed: {exc}")
            total_voters, voters, sects = 0, [], []

    pen_total = sum(int(r["votes"]) for r in lists)
    return render_template(
        "pen.html", year=year, slug=slug, file_id=file_id, pen=pen, area=area, candidate_id=candidate_id,
        lists=[{"name": strip_list_number(r["grp"]), "votes": int(r["votes"]),
                "share": round(int(r["votes"]) / max(1, pen_total) * 100, 1)} for r in lists],
        pen_total=pen_total, voters=voters, total_voters=int(total_voters), page=page,
        pages=max(1, -(-int(total_voters) // per_page)), q=q, sect=sect, sects=sects,
    )


def get_party_map(file_id: str) -> dict:
    """{candidate_id: party} for one district/year. Candidates whose party is
    unknown are simply absent. Party data exists for 2022 only."""
    request_cache = _request_dict("party_cache")
    if request_cache is not None and file_id in request_cache:
        return request_cache[file_id]
    year, district_code = parse_file_id(file_id)
    result = {}
    if year is not None:
        try:
            with DB_ENGINE.connect() as conn:
                rows = conn.execute(text("""
                    SELECT CAST(candidate_id AS TEXT), party FROM election_members
                    WHERE year = :year AND district_code = :d
                      AND party IS NOT NULL AND TRIM(party) <> ''
                """), {"year": year, "d": district_code}).all()
            result = {str(cid): str(party).strip() for cid, party in rows}
        except Exception as exc:
            print(f"Party load failed for {file_id}: {exc}")
    if request_cache is not None:
        request_cache[file_id] = result
    return result


def build_parties_payload(year: str):
    """Party analytics across every district for one year (current scenario)."""
    UNKNOWN = "Unknown"
    parties = {}
    district_labels = []
    totals = {"candidates": 0, "votes": 0, "seats": 0}

    for region in deduplicated_display_regions(load_regions_from_database()):
        file_id = region["files"].get(year)
        df = get_candidates_df(file_id) if file_id else None
        if df is None or df.empty:
            continue
        label = region["district_label"]
        district_labels.append(label)
        party_map = get_party_map(file_id)
        for _, r in df.iterrows():
            party = party_map.get(str(r["CANDIDATE_ID"])) or UNKNOWN
            votes, won = int(r["VOTES"]), bool(r["IS_WINNER"])
            entry = parties.setdefault(party, {
                "candidates": 0, "votes": 0, "seats": 0, "districts": {}, "sects": {}, "top": None, "winners": [],
            })
            entry["candidates"] += 1
            entry["votes"] += votes
            entry["seats"] += int(won)
            d = entry["districts"].setdefault(label, {"seats": 0, "votes": 0, "candidates": 0})
            d["seats"] += int(won); d["votes"] += votes; d["candidates"] += 1
            if won:
                entry["sects"][str(r["RELIGION"])] = entry["sects"].get(str(r["RELIGION"]), 0) + 1
                entry["winners"].append({"name": str(r["MEMBER"]), "votes": votes, "district": label,
                                         "sect": str(r["RELIGION"]), "list": strip_list_number(r["GROUP"])})
            if entry["top"] is None or votes > entry["top"]["votes"]:
                entry["top"] = {"name": str(r["MEMBER"]), "votes": votes, "district": label}
            totals["candidates"] += 1; totals["votes"] += votes; totals["seats"] += int(won)

    rows = []
    for name, e in parties.items():
        rows.append({
            "name": name, "is_unknown": name == UNKNOWN,
            "candidates": e["candidates"], "votes": e["votes"], "seats": e["seats"],
            "vote_share": round(e["votes"] / totals["votes"] * 100, 2) if totals["votes"] else 0.0,
            "seat_share": round(e["seats"] / totals["seats"] * 100, 1) if totals["seats"] else 0.0,
            "win_rate": round(e["seats"] / e["candidates"] * 100, 1) if e["candidates"] else 0.0,
            "avg_votes": round(e["votes"] / e["candidates"]) if e["candidates"] else 0,
            "votes_per_seat": round(e["votes"] / e["seats"]) if e["seats"] else None,
            "district_count": len(e["districts"]),
            "top": e["top"], "districts": e["districts"],
            "sects": sorted(e["sects"].items(), key=lambda kv: -kv[1]),
            "winners": sorted(e["winners"], key=lambda w: -w["votes"]),
        })
    rows.sort(key=lambda r: (r["is_unknown"], -r["seats"], -r["votes"]))

    known = [r for r in rows if not r["is_unknown"]]
    unknown = next((r for r in rows if r["is_unknown"]), None)
    coverage = {
        "candidates_known": sum(r["candidates"] for r in known), "candidates_total": totals["candidates"],
        "votes_known": sum(r["votes"] for r in known), "votes_total": totals["votes"],
        "seats_known": sum(r["seats"] for r in known), "seats_total": totals["seats"],
        "parties": len(known),
    }
    for key in ("candidates", "votes", "seats"):
        total = coverage[f"{key}_total"]
        coverage[f"{key}_pct"] = round(coverage[f"{key}_known"] / total * 100, 1) if total else 0.0

    def bar(items, value_key, title, color):
        if not items:
            return "{}"
        ordered = list(reversed(items))
        fig = go.Figure(go.Bar(
            x=[r[value_key] for r in ordered], y=[r["name"] for r in ordered], orientation="h",
            marker_color=color, text=[f"{r[value_key]:,}" for r in ordered],
            textposition="outside", cliponaxis=False,
            hovertemplate="%{y}<br>" + title + ": %{x:,}<extra></extra>",
        ))
        fig.update_layout(
            height=max(260, 30 * len(ordered) + 60), margin=dict(l=10, r=60, t=10, b=30),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", bargap=0.3, showlegend=False,
            xaxis=dict(title=title, showgrid=True, gridcolor="#eceff3", zeroline=False),
            yaxis=dict(automargin=True, tickfont=dict(size=12, color="#212529")),
        )
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    seat_parties = [r for r in known if r["seats"] > 0]
    vote_parties = sorted(known, key=lambda r: -r["votes"])[:15]
    matrix_districts = [d for d in district_labels if any(r["districts"].get(d, {}).get("seats") for r in seat_parties)]

    return {
        "has_data": bool(known), "party_rows": rows, "known_rows": known, "unknown_row": unknown,
        "coverage": coverage, "seat_parties": seat_parties, "matrix_districts": matrix_districts,
        "seats_chart_json": bar(seat_parties, "seats", "Seats", "#1b5e20"),
        "votes_chart_json": bar(vote_parties, "votes", "Votes", "#3f51b5"),
    }


@app.route("/parties")
def parties_dashboard():
    selected_year = request.args.get("year", "2022")
    if selected_year not in {"2018", "2022"}:
        selected_year = "2022"
    payload = build_parties_payload(selected_year)
    return render_template("parties.html", current_year=selected_year, **payload)


@app.route("/party")
def party_detail():
    """One political party: its strongest districts, cities/areas and ballot pens."""
    party = request.args.get("name", "").strip()
    year = request.args.get("year", "2022")
    if year not in {"2018", "2022"}:
        year = "2022"
    if not party:
        return "Party not found.", 404
    top_n = 15

    labels = {}
    for region in deduplicated_display_regions(load_regions_from_database()):
        file_id = region["files"].get(year)
        if file_id:
            labels[parse_file_id(file_id)[1]] = {"label": region["district_label"], "slug": region["slug"], "file_id": file_id}

    params = {"year": int(year), "party": party}
    with DB_ENGINE.connect() as conn:
        cand_rows = conn.execute(text("""
            SELECT m.district_code, CAST(m.candidate_id AS TEXT) AS cid, m.member, m.group_name, m.religion,
                   m.district, m.is_winner, COALESCE(SUM(v.votes), 0)::bigint AS votes
            FROM election_members m
            LEFT JOIN election_votes v
              ON v.year = m.year AND v.district_code = m.district_code AND v.candidate_id = m.candidate_id
            WHERE m.year = :year AND m.party = :party
            GROUP BY m.district_code, m.candidate_id, m.member, m.group_name, m.religion, m.district, m.is_winner
            ORDER BY votes DESC
        """), params).mappings().all()
        if not cand_rows:
            return "Party not found.", 404
        district_totals = dict(conn.execute(text("""
            SELECT district_code, COALESCE(SUM(votes), 0)::bigint
            FROM election_votes WHERE year = :year AND candidate_id IS NOT NULL GROUP BY district_code
        """), {"year": int(year)}).all())
        area_rows = conn.execute(text("""
            WITH party_votes AS (
                SELECT v.district_code, v.neighborhood, SUM(v.votes)::bigint AS votes,
                       COUNT(DISTINCT v.ballot_pen) AS pens
                FROM election_pen_votes v
                JOIN election_members m
                  ON m.year = v.year AND m.district_code = v.district_code AND m.candidate_id = v.candidate_id
                WHERE v.year = :year AND m.party = :party
                  AND v.neighborhood IS NOT NULL AND v.neighborhood NOT IN ('', 'nan')
                GROUP BY v.district_code, v.neighborhood
                ORDER BY votes DESC LIMIT :n
            )
            SELECT p.*, (SELECT SUM(t.votes) FROM election_pen_votes t
                         WHERE t.year = :year AND t.district_code = p.district_code
                           AND t.neighborhood = p.neighborhood)::bigint AS total
            FROM party_votes p ORDER BY p.votes DESC
        """), {**params, "n": top_n}).mappings().all()
        pen_rows = conn.execute(text("""
            WITH party_votes AS (
                SELECT v.district_code, v.ballot_pen, MAX(v.neighborhood) AS neighborhood,
                       SUM(v.votes)::bigint AS votes
                FROM election_pen_votes v
                JOIN election_members m
                  ON m.year = v.year AND m.district_code = v.district_code AND m.candidate_id = v.candidate_id
                WHERE v.year = :year AND m.party = :party
                GROUP BY v.district_code, v.ballot_pen
                ORDER BY votes DESC LIMIT :n
            )
            SELECT p.*, (SELECT SUM(t.votes) FROM election_pen_votes t
                         WHERE t.year = :year AND t.district_code = p.district_code
                           AND t.ballot_pen = p.ballot_pen)::bigint AS total
            FROM party_votes p ORDER BY p.votes DESC
        """), {**params, "n": top_n}).mappings().all()

    total_votes = sum(int(r["votes"]) for r in cand_rows)
    districts = {}
    for r in cand_rows:
        d = districts.setdefault(r["district_code"], {"votes": 0, "candidates": 0, "seats": 0})
        d["votes"] += int(r["votes"]); d["candidates"] += 1; d["seats"] += int(bool(r["is_winner"]))
    district_list = []
    for code, d in districts.items():
        info = labels.get(code, {"label": code, "slug": None, "file_id": f"{year}:{code}"})
        total = int(district_totals.get(code, 0))
        district_list.append({
            "label": info["label"], "slug": info["slug"], **d,
            "share": round(d["votes"] / total * 100, 1) if total else 0.0,
            "of_party": round(d["votes"] / total_votes * 100, 1) if total_votes else 0.0,
        })
    district_list.sort(key=lambda x: -x["votes"])

    def place(code):
        info = labels.get(code, {})
        return info.get("label", code), info.get("file_id", f"{year}:{code}")

    areas = []
    for r in area_rows:
        label, file_id = place(r["district_code"])
        total = int(r["total"] or 0)
        areas.append({"name": r["neighborhood"], "district": label, "file_id": file_id, "votes": int(r["votes"]),
                      "pens": int(r["pens"]), "share": round(int(r["votes"]) / total * 100, 1) if total else 0.0})
    pens = []
    for r in pen_rows:
        label, file_id = place(r["district_code"])
        total = int(r["total"] or 0)
        pens.append({"name": r["ballot_pen"], "area": r["neighborhood"] or "", "district": label, "file_id": file_id,
                     "votes": int(r["votes"]), "share": round(int(r["votes"]) / total * 100, 1) if total else 0.0})

    candidates = []
    for r in cand_rows:
        label, file_id = place(r["district_code"])
        candidates.append({"id": r["cid"], "name": r["member"], "list": strip_list_number(r["group_name"]),
                           "sect": r["religion"], "qada": r["district"], "district": label, "file_id": file_id,
                           "votes": int(r["votes"]), "is_winner": bool(r["is_winner"])})

    return render_template(
        "party.html", party=party, year=year, total_votes=total_votes,
        seats=sum(1 for c in candidates if c["is_winner"]), candidates=candidates,
        districts=district_list, areas=areas, pens=pens, top_n=top_n,
    )


@app.route("/national")
def national_dashboard():
    selected_year = request.args.get("year", "2022")
    if selected_year not in {"2018", "2022"}:
        selected_year = "2022"
    payload = build_national_payload(selected_year)
    return render_template("national.html", current_year=selected_year, **payload)

@app.route("/api/export_report/<file_id>/<fmt>")
def export_report(file_id, fmt):
    fmt = str(fmt).lower().strip()
    tables = build_report_tables(file_id)
    if tables is None:
        return "Data not found.", 404

    slug, info = get_region_by_file_id(file_id)
    year = file_id.split(":", 1)[0] if ":" in file_id else "year"
    district_label = info["district_label"] if info else file_id
    base_name = safe_download_name(f"{district_label}_{year}_election_report")

    if fmt in ["xlsx", "excel"]:
        output = build_excel_report(file_id, district_label)
        if output is None:
            return "Data not found.", 404
        return send_file(
            output,
            as_attachment=True,
            download_name=f"{base_name}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    if fmt == "pdf":
        try:
            from reportlab.lib import colors
            from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_LEFT
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.pdfbase import pdfmetrics
            from reportlab.pdfbase.ttfonts import TTFont
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
            import arabic_reshaper
            from bidi.algorithm import get_display
        except Exception as e:
            return (
                "PDF export needs reportlab + Arabic text packages. Install them with: "
                "pip install reportlab arabic-reshaper python-bidi. "
                f"Original error: {e}"
            ), 500

        def first_existing_path(paths):
            for path in paths:
                if os.path.exists(path):
                    return path
            return None

        # Arabic PDF fonts
        # Put font files here: <project>/static/fonts/
        # Supported pairs:
        #   DejaVuSans.ttf + DejaVuSans-Bold.ttf
        #   NotoSansArabic-Regular.ttf + NotoSansArabic-Bold.ttf
        font_dir = os.path.join(BASE_DIR, "static", "fonts")
        arabic_font_path = first_existing_path([
            os.path.join(font_dir, "DejaVuSans.ttf"),
            os.path.join(font_dir, "NotoSansArabic-Regular.ttf"),
            # Linux fallbacks, useful during development/deployment if installed globally.
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/noto/NotoSansArabic-Regular.ttf",
        ])
        arabic_bold_font_path = first_existing_path([
            os.path.join(font_dir, "DejaVuSans-Bold.ttf"),
            os.path.join(font_dir, "NotoSansArabic-Bold.ttf"),
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/noto/NotoSansArabic-Bold.ttf",
        ])

        if not arabic_font_path:
            return (
                "Arabic PDF font missing. Create static/fonts/ and add either: "
                "DejaVuSans.ttf + DejaVuSans-Bold.ttf OR "
                "NotoSansArabic-Regular.ttf + NotoSansArabic-Bold.ttf. "
                "Then restart Flask and export the PDF again."
            ), 500

        pdfmetrics.registerFont(TTFont("ArabicFont", arabic_font_path))
        if arabic_bold_font_path:
            pdfmetrics.registerFont(TTFont("ArabicFontBold", arabic_bold_font_path))
        else:
            pdfmetrics.registerFont(TTFont("ArabicFontBold", arabic_font_path))

        def has_arabic(value):
            return bool(re.search(r"[\u0600-\u06FF]", str(value or "")))

        def shape_text(value):
            text = "" if value is None else str(value)
            # ReportLab needs Arabic glyph shaping + bidirectional display order.
            if has_arabic(text):
                return get_display(arabic_reshaper.reshape(text))
            return text

        output = BytesIO()
        doc = SimpleDocTemplate(
            output, pagesize=landscape(A4),
            rightMargin=18, leftMargin=18, topMargin=18, bottomMargin=18
        )
        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(
            name="ArabicTitle", parent=styles["Title"], fontName="ArabicFontBold", fontSize=17,
            leading=22, alignment=TA_CENTER
        ))
        styles.add(ParagraphStyle(
            name="ArabicHeading", parent=styles["Heading2"], fontName="ArabicFontBold", fontSize=12,
            leading=15, alignment=TA_LEFT, spaceAfter=6
        ))
        styles.add(ParagraphStyle(
            name="ArabicCell", parent=styles["Normal"], fontName="ArabicFont", fontSize=7.2,
            leading=9, alignment=TA_RIGHT, wordWrap="CJK"
        ))
        styles.add(ParagraphStyle(
            name="ArabicHeaderCell", parent=styles["Normal"], fontName="ArabicFontBold", fontSize=7.5,
            leading=9.5, alignment=TA_CENTER, textColor=colors.white
        ))
        styles.add(ParagraphStyle(
            name="ArabicNormal", parent=styles["Normal"], fontName="ArabicFont", fontSize=9, leading=12
        ))
        styles.add(ParagraphStyle(
            name="ArabicItalic", parent=styles["Italic"], fontName="ArabicFont", fontSize=8, leading=10
        ))

        story = []
        story.append(Paragraph(shape_text(f"Election Report - {district_label} - {year}"), styles["ArabicTitle"]))
        story.append(Paragraph(shape_text(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"), styles["ArabicNormal"]))
        story.append(Spacer(1, 10))

        def cleaned_for_pdf(df):
            pdf_df = df.copy()
            if "IS_WINNER" in pdf_df.columns:
                pdf_df = pdf_df.drop(columns=["IS_WINNER"])
            return pdf_df

        def paragraph_cell(value, header=False):
            style = styles["ArabicHeaderCell"] if header else styles["ArabicCell"]
            return Paragraph(shape_text(value), style)

        def col_widths_for(df):
            cols = list(df.columns)
            widths = []
            for col in cols:
                if col == "MEMBER": widths.append(105)
                elif col == "GROUP": widths.append(145)
                elif col in ["RELIGION", "DISTRICT"]: widths.append(75)
                elif col in ["TOTAL_VOTES", "VOTES", "CANDIDATES", "WINNERS"]: widths.append(55)
                else: widths.append(65)
            total = sum(widths)
            max_width = landscape(A4)[0] - 36
            if total > max_width:
                ratio = max_width / total
                widths = [w * ratio for w in widths]
            return widths

        def add_table(title, df, max_rows=35):
            story.append(Paragraph(shape_text(title), styles["ArabicHeading"]))
            export_df = cleaned_for_pdf(df.head(max_rows)).copy()
            data = [[paragraph_cell(c, header=True) for c in export_df.columns.tolist()]]
            for row in export_df.astype(str).values.tolist():
                data.append([paragraph_cell(v) for v in row])

            table = Table(data, repeatRows=1, colWidths=col_widths_for(export_df), hAlign="LEFT")
            table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0d6efd")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "ArabicFontBold"),
                ("FONTNAME", (0, 1), (-1, -1), "ArabicFont"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5f7fb")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 3),
                ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]))
            story.append(table)
            if len(df) > max_rows:
                story.append(Paragraph(shape_text(f"Showing first {max_rows} rows out of {len(df)}."), styles["ArabicItalic"]))
            story.append(Spacer(1, 10))

        # Put the winners list first because it is the most important report output.
        add_table("Winning Candidates / المرشحون الفائزون", tables["winners"], 80)
        add_table("List Summary / ملخص اللوائح", tables["list_summary"], 40)
        story.append(PageBreak())
        add_table("All Candidates / جميع المرشحين", tables["candidates"], 100)
        add_table("District Summary / ملخص الأقضية والطوائف", tables["district_summary"], 60)

        doc.build(story)
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f"{base_name}.pdf", mimetype="application/pdf")

    return "Unsupported format. Use xlsx or pdf.", 400

@app.route("/api/reset_results", methods=["POST"])
def api_reset_results():
    data = request.json or {}
    file_id = data.get("filename")

    if not file_id:
        return jsonify({
            "success": False,
            "message": "Election identifier is required."
        }), 400

    try:
        reset_result = reset_groups_db(file_id)
        winners = recompute_winners(file_id)

        removed_lists = reset_result.get("removed_custom_lists", [])
        if removed_lists:
            message = (
                "Scenario reset successfully. "
                f"{len(removed_lists)} user-created list(s) removed."
            )
        else:
            message = "Scenario reset successfully to the original lists."

        return jsonify({
            "success": True,
            "message": message,
            "updated_candidates": reset_result["updated_candidates"],
            "removed_custom_lists": removed_lists,
            "winner_count": len(winners),
        })
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        print(f"Reset error: {exc}")
        return jsonify({"success": False, "message": f"Database reset failed: {exc}"}), 500

@app.route("/api/get_votes_needed", methods=["POST"])
def api_get_votes_needed():
    data = request.json or {}
    file_id = data.get("filename")
    target_group = data.get("group_name")
    target_k = str(data.get("target_k", "1"))
    df = get_candidates_df(file_id)
    if df is None or not target_group:
        return jsonify({"success": False, "message": "Data not available."}), 400
    return jsonify({
        "success": True,
        "result": calculate_votes_needed_for_one_group(file_id, df, target_group, target_k),
    })

@app.route("/api/scenario_comparison", methods=["POST"])
def api_scenario_comparison():
    data = request.json or {}
    file_id = data.get("filename")
    current_df = get_candidates_df(file_id)
    original_df = load_original_df(file_id)
    if current_df is None or original_df is None:
        return jsonify({"success": False, "message": "Data not available."}), 400

    # Make sure current winners reflect the latest state.
    current_winners = _compute_winners_from_quota(file_id, current_df)
    current_df = current_df.copy()
    current_df["IS_WINNER"] = current_df["CANDIDATE_ID"].isin(current_winners)

    original_winners = set(original_df[original_df["IS_WINNER"] == True]["MEMBER"].astype(str))
    current_winners = set(current_df[current_df["IS_WINNER"] == True]["MEMBER"].astype(str))

    moved_candidates = []
    original_groups = dict(zip(original_df["MEMBER"].astype(str), original_df["GROUP"].astype(str)))
    for _, row in current_df.iterrows():
        name = str(row["MEMBER"])
        old_group = original_groups.get(name)
        new_group = str(row["GROUP"])
        if old_group is not None and old_group != new_group:
            moved_candidates.append({"name": name, "from": old_group, "to": new_group, "votes": int(row["VOTES"])})

    rows = summarize_groups_for_compare(original_df, current_df)
    return jsonify({
        "success": True,
        "metrics": {
            "original_votes": int(original_df["VOTES"].sum()),
            "current_votes": int(current_df["VOTES"].sum()),
            "original_winners": len(original_winners),
            "current_winners": len(current_winners),
            "moved_candidates": len(moved_candidates),
            "new_winners": len(current_winners - original_winners),
            "lost_winners": len(original_winners - current_winners),
        },
        "groups": rows,
        "new_winners": sorted(list(current_winners - original_winners)),
        "lost_winners": sorted(list(original_winners - current_winners)),
        "unchanged_winners": sorted(list(current_winners & original_winners)),
        "moves": moved_candidates[:80]
    })

@app.route("/api/what_if_advisor", methods=["POST"])
def api_what_if_advisor():
    data = request.json or {}
    file_id = data.get("filename")
    target_group = data.get("group_name")
    df = get_candidates_df(file_id)
    if df is None or not target_group:
        return jsonify({"success": False, "message": "Choose a list first."}), 400

    current_winners = _compute_winners_from_quota(file_id, df)
    current_seats = count_group_winners(df, current_winners)
    baseline = int(current_seats.get(target_group, 0))

    votes_needed = calculate_votes_needed_for_one_group(file_id, df, target_group, "1") or {}

    # Simulate list vote swings: increase all candidates inside the selected list.
    swings = []
    for pct in [3, 5, 10, 15, 20]:
        sim_df = df.copy()
        mask = sim_df["GROUP"] == target_group
        sim_df.loc[mask, "VOTES"] = (sim_df.loc[mask, "VOTES"].astype(float) * (1 + pct / 100.0)).round().astype(int)
        sim_winners = _compute_winners_from_quota(file_id, sim_df)
        sim_seats = count_group_winners(sim_df, sim_winners)
        after = int(sim_seats.get(target_group, 0))
        swings.append({"percent": pct, "seats_after": after, "seat_gain": after - baseline})

    target_df = df[df["GROUP"] == target_group].copy().sort_values("VOTES", ascending=False)
    non_winners_df = target_df[~target_df["CANDIDATE_ID"].isin(current_winners)]

    best_candidate = None
    if not non_winners_df.empty:
        r = non_winners_df.iloc[0]
        best_candidate = {"name": str(r["MEMBER"]), "votes": int(r["VOTES"]), "religion": str(r["RELIGION"]), "district": str(r["DISTRICT"])}

    weakest_winner = None
    other_winners = df[(df["CANDIDATE_ID"].isin(current_winners)) & (df["GROUP"] != target_group)].copy().sort_values("VOTES", ascending=True)
    if not other_winners.empty:
        r = other_winners.iloc[0]
        weakest_winner = {"name": str(r["MEMBER"]), "group": str(r["GROUP"]), "votes": int(r["VOTES"]), "religion": str(r["RELIGION"]), "district": str(r["DISTRICT"])}

    suggestions = votes_needed.get("suggestions", []) if isinstance(votes_needed, dict) else []
    summary = []
    if baseline == 0:
        summary.append("The selected list currently has no seats. The first goal is passing the electoral threshold.")
    else:
        summary.append(f"The selected list currently has {baseline} seat(s).")
    if isinstance(votes_needed, dict) and votes_needed.get("votes"):
        summary.append(f"Approximate additional votes needed for one more seat: {int(votes_needed.get('votes')):,}.")
    if suggestions:
        summary.append("The engine found swap scenarios that may produce an extra seat.")
    else:
        summary.append("No safe direct swap was found, so try vote swing or coalition changes.")

    return jsonify({
        "success": True,
        "group": target_group,
        "baseline_seats": baseline,
        "current_votes": int(df[df["GROUP"] == target_group]["VOTES"].sum()),
        "votes_needed": votes_needed,
        "swings": swings,
        "best_non_winner": best_candidate,
        "weakest_external_winner": weakest_winner,
        "summary": summary
    })

@app.route("/api/change_candidate_group", methods=["POST"])
def change_candidate_group():
    data = request.json or {}
    file_id = data.get("filename")
    candidate_name = data.get("candidate_name")
    new_group = data.get("new_group")
    candidate_id = data.get("candidate_id")
    try:
        old_group = update_candidate_group_db(file_id, candidate_name, new_group, candidate_id)
        winners = recompute_winners(file_id)
        df = get_candidates_df(file_id)
        return jsonify({
            "success": True,
            "winner_ids": sorted(str(w) for w in winners),
            "candidate": candidate_name,
            "old_group": old_group,
            "new_group": new_group,
            "old_total": int(df[df["GROUP"] == old_group]["VOTES"].sum()),
            "new_total": int(df[df["GROUP"] == new_group]["VOTES"].sum()),
        })
    except LookupError as exc:
        return jsonify({"success": False, "message": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": f"Database update failed: {exc}"}), 500

@app.route("/api/swap_candidates", methods=["POST"])
def swap_candidates():
    data = request.json or {}
    file_id = data.get("filename")
    source_name = data.get("candidate_name")
    target_name = data.get("target_name")
    source_id = data.get("candidate_id")
    target_id = data.get("target_id")
    try:
        swap = swap_candidate_groups_db(file_id, source_name, target_name, source_id, target_id)
        winners = recompute_winners(file_id)
        df = get_candidates_df(file_id)
        source_group = swap["source_group"]
        target_group = swap["target_group"]
        return jsonify({
            "success": True,
            "winner_ids": sorted(str(w) for w in winners),
            "src_name": source_name,
            "target_name": target_name,
            "src_group": source_group,
            "target_group": target_group,
            "src_total": int(df[df["GROUP"] == source_group]["VOTES"].sum()),
            "target_total": int(df[df["GROUP"] == target_group]["VOTES"].sum()),
            "src_group_id": source_group.replace(" ", "_").replace(".", ""),
            "target_group_id": target_group.replace(" ", "_").replace(".", ""),
        })
    except LookupError as exc:
        return jsonify({"success": False, "message": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": f"Database swap failed: {exc}"}), 500

@app.route("/api/multi_swap_candidates", methods=["POST"])
def multi_swap_candidates():
    data = request.json or {}
    file_id = data.get("filename")
    swaps = data.get("swaps", [])
    try:
        updated = multi_swap_candidate_groups_db(file_id, swaps)
        winners = recompute_winners(file_id)
        return jsonify({"success": True, "updated_candidates": updated,
                        "winner_ids": sorted(str(w) for w in winners)})
    except LookupError as exc:
        return jsonify({"success": False, "message": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": f"Database multi-swap failed: {exc}"}), 500
@app.route("/api/recompute_results", methods=["POST"])
def api_recompute_results():
    file_id = (request.json or {}).get("filename")
    try:
        winners = recompute_winners(file_id)
        # recompute_winners() returns candidate_id values, not names.
        return jsonify({
            "success": True,
            "winner_ids": sorted(str(w) for w in winners),
            "winner_count": len(winners),
        })
    except Exception as exc:
        return jsonify({"success": False, "message": f"Recalculation failed: {exc}"}), 500

@app.route("/api/find_winning_list", methods=["POST"])
def api_find_winning_list():
    data = request.json or {}
    fid = data.get("filename")
    candidate_name = data.get("candidate_name")
    df = get_candidates_df(fid)
    
    if df is None or not candidate_name:
        return jsonify({"success": False, "message": "بيانات غير متوفرة."}), 400

    mask = df["MEMBER"] == candidate_name
    if not mask.any():
        return jsonify({"success": False, "message": f"المرشح {candidate_name} غير موجود."}), 404

    candidate_row = df[mask].iloc[0]
    current_group = str(candidate_row["GROUP"])
    candidate_religion = str(candidate_row["RELIGION"])
    candidate_district = str(candidate_row["DISTRICT"])
    candidate_votes = int(candidate_row["VOTES"])
    is_currently_winner = bool(candidate_row["IS_WINNER"])

    all_groups = sorted(df["GROUP"].unique().tolist())
    results = []

    for target_group in all_groups:
        if target_group == current_group:
            # Check current state
            results.append({
                "group": strip_list_number(target_group),
                "wins": is_currently_winner,
                "is_current": True,
                "possible": True,
                "total_votes": int(df[df["GROUP"] == target_group]["VOTES"].sum())
            })
            continue

        sim_df = df.copy()

        # Find candidates in the target group with the same religion and district
        target_group_mask = (
            (sim_df["GROUP"] == target_group) &
            (sim_df["RELIGION"] == candidate_religion) &
            (sim_df["DISTRICT"] == candidate_district)
        )
        potential_swaps = sim_df[target_group_mask]

        # If no one matches the religion and district in the target list, a swap isn't possible
        if potential_swaps.empty:
            results.append({
                "group": strip_list_number(target_group),
                "wins": False,
                "is_current": False,
                "possible": False,
                "message": "لا يوجد مرشح بنفس الطائفة والدائرة للتبديل",
                "total_votes": int(sim_df[sim_df["GROUP"] == target_group]["VOTES"].sum())
            })
            continue

        # Find the candidate with the minimum votes to swap out
        swap_out_idx = potential_swaps["VOTES"].idxmin()
        swap_out_name = sim_df.loc[swap_out_idx, "MEMBER"]

        # Perform the swap: move our candidate to the target group, and the weakest candidate to our current group
        sim_df.loc[sim_df["MEMBER"] == candidate_name, "GROUP"] = target_group
        sim_df.loc[sim_df["MEMBER"] == swap_out_name, "GROUP"] = current_group

        # Recompute winners with simulated data
        sim_winners = _compute_winners_from_quota(fid, sim_df)
        would_win = candidate_row["CANDIDATE_ID"] in sim_winners

        results.append({
            "group": strip_list_number(target_group),
            "wins": would_win,
            "is_current": False,
            "possible": True,
            "swapped_with": swap_out_name,
            "total_votes": int(sim_df[sim_df["GROUP"] == target_group]["VOTES"].sum())
        })

    return jsonify({
        "success": True,
        "candidate": candidate_name,
        "religion": candidate_religion,
        "district": candidate_district,
        "votes": candidate_votes,
        "current_group": strip_list_number(current_group),
        "currently_winner": is_currently_winner,
        "results": results
    })

# ---------------------------------------------------------------------------
# Election assistant (rule-based, Arabic-normalized, with follow-up context)
# ---------------------------------------------------------------------------
_CHAT_STOP = {"كم", "صوت", "اصوات", "لائحه", "قائمه", "مرشح", "من", "ما", "هو", "هي", "في", "عن",
              "الى", "علي", "على", "ماذا", "هل", "لو", "اذا", "ال", "votes", "list", "of", "the", "for"}


def _nrm(text) -> str:
    """Normalize Arabic text for matching: hamza/ya/kaf variants, ta-marbuta, diacritics, spacing."""
    s = normalize_arabic(str(text or "")).lower()
    s = re.sub(r"[ً-ْـ]", "", s)
    return s.replace("ة", "ه").strip()


def _chat_tokens(text) -> list:
    return [t for t in re.split(r"[^\w]+", _nrm(text)) if t and t not in _CHAT_STOP and len(t) > 1]


def _chat_rank_matches(query: str, names: list, limit: int = 5) -> list:
    """Rank names against a query by token overlap, falling back to difflib similarity."""
    q_tokens = _chat_tokens(query)
    if not q_tokens:
        return []
    q_norm = _nrm(query)
    scored = []
    for name in names:
        n_tokens = set(_chat_tokens(name))
        hit = sum(1 for t in q_tokens
                  if t in n_tokens or any(len(nt) > 2 and (t in nt or nt in t) for nt in n_tokens))
        if hit == 0:
            ratio = difflib.SequenceMatcher(None, q_norm, _nrm(name)).ratio()
            if ratio >= 0.6:
                scored.append((ratio, name))
            continue
        coverage = hit / len(q_tokens)
        exact_bonus = 0.5 if (q_norm == _nrm(name) or set(q_tokens) == n_tokens) else 0
        scored.append((coverage + exact_bonus + 0.01 * hit - 0.001 * len(n_tokens), name))
    scored.sort(key=lambda x: -x[0])
    return scored[:limit]


def _chat_html_card(title: str, body: str) -> str:
    return f"<div style='font-size:0.82rem;'><b style='color:#667eea;'>{title}</b><br><br>{body}</div>"


def _chat_reply(reply, action="none", suggestions=None, context=None):
    return jsonify({"reply": reply, "action": action, "suggestions": suggestions or [], "context": context or {}})


def _chat_help_html(prefix="يمكنني مساعدتك بـ:"):
    return ("<div style='font-size:0.82rem;'>" + prefix + "<br><br>"
            "🔍 <b>اسم مرشح</b> — تفاصيله وترتيبه (ثم اسأل «وقائمته؟»)<br>"
            "📋 <b>لائحة [اسم]</b> — مرشحو اللائحة وأصواتها<br>"
            "🏆 <b>من فاز</b> / <b>من فاز [لائحة]</b> / <b>فائزو [طائفة]</b><br>"
            "🧮 <b>كم يلزم [لائحة] مقعدين</b> — الأصوات وأرخص تبديل<br>"
            "🔎 <b>بأي لائحة يفوز [مرشح]</b><br>"
            "🎯 <b>اقرب الخاسرين</b> — من كاد يفوز<br>"
            "⚖️ <b>قارن X مع Y</b> — 🔄 <b>بدل X مع Y</b><br>"
            "📊 <b>ملخص</b> — <b>حاصل</b> — <b>اعلى 5</b></div>")


@app.route("/api/chat", methods=["POST"])
def api_chat():
    data = request.json or {}
    msg, fid = str(data.get("message", "")).strip(), data.get("filename")
    ctx_in = data.get("context") or {}
    df = get_candidates_df(fid)
    if df is None:
        return _chat_reply("عذراً، البيانات غير متوفرة حالياً.")

    m_norm = _nrm(msg)
    num_seats = sum(get_quota(fid).get("rel_limits", {}).values())
    total_votes = int(df["VOTES"].sum())
    groups_all = df["GROUP"].unique().tolist()
    group_labels = {g: strip_list_number(g) for g in groups_all}
    ctx = {"candidate": ctx_in.get("candidate"), "group": ctx_in.get("group")}
    member_set = set(df["MEMBER"].astype(str))

    def has(*words):
        return any(_nrm(w) in m_norm for w in words)

    def row_by_name(name):
        return df[df["MEMBER"] == name].iloc[0]

    def resolve_candidate(text, allow_context=True):
        """Return (row, None) | (None, ambiguity_reply) | (None, None)."""
        text = text.strip()
        if allow_context and ctx["candidate"] in member_set and (
                not text or re.fullmatch(r"(هو|هي|له|لها|عنه|عنها|نفسه|نفسها)", _nrm(text))):
            return row_by_name(ctx["candidate"]), None
        ranked = _chat_rank_matches(text, df["MEMBER"].astype(str).tolist(), 6)
        if not ranked:
            return None, None
        best = ranked[0][0]
        uniq = list(dict.fromkeys(n for s, n in ranked if s >= best - 0.05))
        if len(uniq) > 1 and best < 1.4:
            return None, _chat_reply("🤔 وجدت أكثر من مرشح بهذا الاسم، أيهم تقصد؟",
                                     suggestions=[f"أصوات {n}" for n in uniq[:4]], context=ctx)
        return row_by_name(ranked[0][1]), None

    def resolve_group(text, allow_context=True):
        text = re.sub(r"\(\s*\d+\s*\)", " ", text).strip()
        if not text:
            if allow_context and ctx["group"] in groups_all:
                return ctx["group"], None
            return None, None
        ranked = _chat_rank_matches(text, [group_labels[g] for g in groups_all], 5)
        if not ranked:
            return None, None
        label_to_group = {group_labels[g]: g for g in groups_all}
        best = ranked[0][0]
        close = list(dict.fromkeys(n for s, n in ranked if s >= best - 0.05))
        if len(close) > 1 and best < 1.4:
            return None, _chat_reply("🤔 أي لائحة تقصد؟", suggestions=[f"لائحة {n}" for n in close[:4]], context=ctx)
        return label_to_group[ranked[0][1]], None

    def strip_keywords(text, words):
        out = text
        for w in words:
            out = re.sub(re.escape(w), " ", out, flags=re.IGNORECASE)
        return re.sub(r"\s+", " ", out).strip()

    def group_seats(g):
        return int(df[(df["GROUP"] == g) & (df["IS_WINNER"] == True)].shape[0])

    def candidate_card(row):
        votes = int(row["VOTES"])
        grp_total = int(df[df["GROUP"] == row["GROUP"]]["VOTES"].sum())
        pct = round(votes / max(1, grp_total) * 100, 1)
        is_win = bool(row["IS_WINNER"])
        color = "#43a047" if is_win else "#e53935"
        peers = df[(df["DISTRICT"] == row["DISTRICT"]) & (df["RELIGION"] == row["RELIGION"])]
        rank_in_seat = int((peers["VOTES"] > votes).sum()) + 1
        html = "<div style='font-size:0.82rem;'>"
        html += f"<div style='background:#f8f9fc;padding:10px;border-radius:10px;border-right:3px solid {color};'>"
        html += f"<b style='font-size:0.9rem;'>{row['MEMBER']}</b> <span style='color:{color};'>{'🏆 فائز' if is_win else '❌ خاسر'}</span><br>"
        html += f"<span style='color:#6c757d;font-size:0.75rem;'>{row['RELIGION']} • {row['DISTRICT']}</span><br><br>"
        html += f"📋 اللائحة: <b>{group_labels[row['GROUP']]}</b><br>"
        html += f"🗳️ الأصوات: <b>{votes:,}</b> ({pct}% من لائحته)<br>"
        html += f"🥇 ترتيبه بين مرشحي مقعده: <b>{rank_in_seat}</b> من {len(peers)}<br>"
        if not is_win:
            w_peers = peers[peers["IS_WINNER"] == True]
            if not w_peers.empty:
                gap = int(w_peers["VOTES"].min()) - votes
                if gap > 0:
                    html += f"📉 يفصله عن أقل فائز بنفس المقعد: <b>{gap:,}</b> صوت<br>"
        html += (f"<div style='background:#e0e3e8;border-radius:4px;height:5px;margin-top:4px;'><div style='background:linear-gradient(90deg,#667eea,#764ba2);"
                 f"width:{min(100, pct)}%;height:100%;border-radius:4px;'></div></div></div></div>")
        return html

    def list_reply(grp):
        ctx["group"] = grp
        gdf = df[df["GROUP"] == grp].sort_values("VOTES", ascending=False)
        total = int(gdf["VOTES"].sum())
        html = f"المجموع: <b>{total:,}</b> صوت ({round(total / max(1, total_votes) * 100, 1)}%) | الفائزون: <b>{group_seats(grp)}</b><br><br>"
        for i, (_, r) in enumerate(gdf.iterrows(), 1):
            html += (f"{'✅' if r['IS_WINNER'] else '•'} <b>{i}.</b> {r['MEMBER']} — <b>{int(r['VOTES']):,}</b> "
                     f"<span style='color:#6c757d;font-size:0.7rem;'>{r['RELIGION']}</span><br>")
        return _chat_reply(_chat_html_card(f"📋 {group_labels[grp]}", html),
                           suggestions=[f"كم يلزم {group_labels[grp]} مقعد", f"من فاز {group_labels[grp]}"], context=ctx)

    def remember_candidate(row):
        ctx["candidate"] = str(row["MEMBER"])
        ctx["group"] = str(row["GROUP"])

    # --- help ---
    if re.fullmatch(r"\s*(مساعده|help|\?|؟)\s*", m_norm):
        return _chat_reply(_chat_help_html(), suggestions=["ملخص", "من فاز", "اقرب الخاسرين", "اعلى المرشحين"], context=ctx)

    # --- follow-ups about the remembered candidate/list ("وقائمته؟", "ولائحتها") ---
    if ctx["group"] in groups_all and re.fullmatch(r"\s*و?\s*(لائحته|لائحتها|قائمته|قائمتها|لايحته)\s*", m_norm.replace("؟", "").replace("?", "")):
        return list_reply(ctx["group"])

    # --- 1) swap ---
    for pattern in (r"(?:بدل|بدّل|تبديل|تبادل)\s+(.+?)\s+(?:مع|و)\s+(.+)", r"swap\s+(.+?)\s+with\s+(.+)"):
        m = re.search(pattern, msg, re.IGNORECASE)
        if m:
            a_row, amb = resolve_candidate(m.group(1), False)
            if amb: return amb
            b_row, amb = resolve_candidate(m.group(2), False)
            if amb: return amb
            if a_row is None: return _chat_reply(f"❌ لم أجد مرشحاً باسم <b>{m.group(1).strip()}</b>.", context=ctx)
            if b_row is None: return _chat_reply(f"❌ لم أجد مرشحاً باسم <b>{m.group(2).strip()}</b>.", context=ctx)
            if str(a_row["RELIGION"]) != str(b_row["RELIGION"]) or str(a_row["DISTRICT"]) != str(b_row["DISTRICT"]):
                return _chat_reply(
                    f"❌ لا يمكن التبادل بين <b>{a_row['MEMBER']}</b> ({a_row['RELIGION']} - {a_row['DISTRICT']}) و <b>{b_row['MEMBER']}</b> ({b_row['RELIGION']} - {b_row['DISTRICT']}).<br>يجب أن يكونا من نفس الطائفة والدائرة.",
                    context=ctx)
            if a_row["GROUP"] == b_row["GROUP"]:
                return _chat_reply("ℹ️ المرشحان في اللائحة نفسها، لا حاجة للتبادل.", context=ctx)
            sg, tg = str(a_row["GROUP"]), str(b_row["GROUP"])
            try:
                swap_candidate_groups_db(fid, a_row["MEMBER"], b_row["MEMBER"],
                                         source_id=str(a_row["CANDIDATE_ID"]), target_id=str(b_row["CANDIDATE_ID"]))
                recompute_winners(fid)
            except Exception as exc:
                return _chat_reply(f"❌ فشل تحديث قاعدة البيانات: {exc}", context=ctx)
            return _chat_reply(
                f"✅ تم التبادل وحفظه!<br><b>{a_row['MEMBER']}</b> ← {group_labels[tg]}<br><b>{b_row['MEMBER']}</b> ← {group_labels[sg]}",
                action="swap_done", context=ctx)

    # --- 2) compare two candidates ---
    for pattern in (r"(?:قارن|مقارنة|مقارنه)\s+(.+?)\s+(?:مع|و)\s+(.+)", r"compare\s+(.+?)\s+with\s+(.+)"):
        m = re.search(pattern, msg, re.IGNORECASE)
        if m:
            a_row, amb = resolve_candidate(m.group(1), False)
            if amb: return amb
            b_row, amb = resolve_candidate(m.group(2), False)
            if amb: return amb
            if a_row is None: return _chat_reply(f"❌ لم أجد مرشحاً باسم <b>{m.group(1).strip()}</b>.", context=ctx)
            if b_row is None: return _chat_reply(f"❌ لم أجد مرشحاً باسم <b>{m.group(2).strip()}</b>.", context=ctx)
            va, vb = int(a_row["VOTES"]), int(b_row["VOTES"])
            leader = a_row["MEMBER"] if va >= vb else b_row["MEMBER"]

            def side(r, v, color):
                return (f"<div style='flex:1;background:#f8f9fc;padding:8px;border-radius:8px;border-right:3px solid {color};'>"
                        f"<b>{r['MEMBER']}</b> {'🏆' if r['IS_WINNER'] else '❌'}<br>"
                        f"<span style='color:#6c757d;font-size:0.75rem;'>{r['RELIGION']} • {r['DISTRICT']}</span><br>"
                        f"اللائحة: {group_labels[r['GROUP']]}<br>الأصوات: <b>{v:,}</b></div>")
            html = "<div style='font-size:0.82rem;'><b style='color:#667eea;'>⚖️ مقارنة بين مرشحين</b><br><br><div style='display:flex;gap:8px;'>"
            html += side(a_row, va, "#667eea") + side(b_row, vb, "#764ba2") + "</div>"
            html += f"<br><b>الفرق:</b> {abs(va - vb):,} صوت لصالح <b>{leader}</b>"
            can = (str(a_row["RELIGION"]) == str(b_row["RELIGION"]) and str(a_row["DISTRICT"]) == str(b_row["DISTRICT"])
                   and a_row["GROUP"] != b_row["GROUP"])
            html += ("<br><span style='color:#43a047;'>✅ يمكن تبادلهما (نفس الطائفة والدائرة)</span>" if can
                     else "<br><span style='color:#e53935;'>❌ لا يمكن تبادلهما</span>") + "</div>"
            return _chat_reply(html, suggestions=[f"بدل {a_row['MEMBER']} مع {b_row['MEMBER']}"] if can else [], context=ctx)

    # --- 3) votes needed / cheapest swaps for a list ---
    if has("كم يلزم", "كم يحتاج", "شو يلزم", "ماذا يلزم", "احتاج", "يحتاج", "لتربح", "لتفوز", "مقعد اضافي", "مقعدا اضافيا", "يلزم"):
        k = 1
        km = re.search(r"([123])\s*(?:مقعد|مقاعد|seat)", m_norm)
        if km:
            k = int(km.group(1))
        elif has("مقعدين"):
            k = 2
        elif has("ثلاثه مقاعد", "ثلاث مقاعد"):
            k = 3
        text = strip_keywords(msg, ["كم", "يلزم", "يحتاج", "احتاج", "لائحة", "لائحه", "قائمة", "مقعدين", "مقعد", "مقاعد",
                                     "اضافي", "إضافي", "لتربح", "لتفوز", "شو", "ماذا", "الى", "إلى"])
        text = re.sub(r"[123]", " ", text).strip()
        grp, amb = resolve_group(text)
        if amb: return amb
        if grp is None:
            return _chat_reply("أي لائحة تقصد؟ مثال: <code>كم يلزم لائحة الجبل مقعد</code>", context=ctx)
        ctx["group"] = grp
        res = calculate_votes_needed_for_one_group(fid, df, grp, k)
        if not res:
            return _chat_reply(f"لا يمكن حساب ذلك للائحة <b>{group_labels[grp]}</b> (يتجاوز عدد المقاعد الممكن).", context=ctx)
        html = f"<div style='font-size:0.82rem;'><b style='color:#667eea;'>🧮 {group_labels[grp]}</b><br>"
        html += f"المقاعد الحالية: <b>{res['current_seats']}</b><br>لكسب <b>{k}</b> مقعد إضافي يلزمها ≈ <b>{res['votes']:,}</b> صوت.<br>"
        sugg = res.get("suggestions") or []
        if sugg:
            best = sugg[0]
            html += f"<br><b style='color:#43a047;'>أرخص تبديل:</b> (مجموع {int(best['total_votes']):,} صوت)<br>"
            for s in best["swaps"]:
                html += (f"• <b>{s['in']}</b> ({int(s['in_votes']):,}) من {s['from_list']} ← يحل محل "
                         f"<b>{s['out']}</b> ({s['religion']} • {s['district']})<br>")
        else:
            html += "<br>لا يوجد تبديل ضمن قيود الطائفة والدائرة يحقق ذلك.<br>"
        html += "</div>"
        return _chat_reply(html, suggestions=[f"كم يلزم {group_labels[grp]} {n} مقعد" for n in (1, 2, 3) if n != k], context=ctx)

    # --- 4) which list would X win in ---
    if has("باي لائحه", "بأي لائحه", "في اي لائحه", "اي لائحه يفوز", "اي لائحه تفوز"):
        text = strip_keywords(msg, ["بأي", "باي", "في أي", "في اي", "أي", "اي", "لائحة", "لائحه", "يفوز", "تفوز", "سيفوز",
                                     "لو", "إذا", "اذا", "؟", "?"])
        row, amb = resolve_candidate(text)
        if amb: return amb
        if row is None:
            return _chat_reply("لم أفهم اسم المرشح. مثال: <code>بأي لائحة يفوز [اسم]</code>", context=ctx)
        remember_candidate(row)
        try:
            with app.test_request_context(json={"filename": fid, "candidate_name": row["MEMBER"]}):
                resp = api_find_winning_list()
            payload = resp.get_json()
        except Exception as exc:
            return _chat_reply(f"❌ تعذر الحساب: {exc}", context=ctx)
        wins = [r for r in payload.get("results", []) if r.get("wins")]
        html = (f"<div style='font-size:0.82rem;'><b style='color:#667eea;'>🔎 {row['MEMBER']}</b> "
                f"(حالياً: {group_labels[row['GROUP']]}، {'فائز' if row['IS_WINNER'] else 'خاسر'})<br><br>")
        if wins:
            html += "<b style='color:#43a047;'>يفوز إذا كان في:</b><br>" + "".join(
                f"✅ {w['group']}{' (الحالية)' if w.get('is_current') else ''}<br>" for w in wins)
        else:
            html += "لا يفوز في أي لائحة ضمن قيود التبديل.<br>"
        html += "</div>"
        return _chat_reply(html, suggestions=[f"أصوات {row['MEMBER']}"], context=ctx)

    # --- 5) closest losers ---
    if has("اقرب الخاسرين", "اقرب خاسر", "كاد يفوز", "كادوا", "الخاسرين", "closest"):
        rows = []
        for (dist, rel), peers in df.groupby(["DISTRICT", "RELIGION"]):
            w = peers[peers["IS_WINNER"] == True]
            lo = peers[peers["IS_WINNER"] != True]
            if w.empty or lo.empty:
                continue
            lowest_w = w.sort_values("VOTES").iloc[0]
            top_l = lo.sort_values("VOTES", ascending=False).iloc[0]
            rows.append((int(lowest_w["VOTES"]) - int(top_l["VOTES"]), top_l, lowest_w, dist, rel))
        rows.sort(key=lambda x: abs(x[0]))
        if not rows:
            return _chat_reply("لا توجد بيانات كافية.", context=ctx)
        html = ""
        for gap, lo, w, dist, rel in rows[:6]:
            html += f"❌ <b>{lo['MEMBER']}</b> ({group_labels[lo['GROUP']]}) — {rel} • {dist}<br>"
            html += f"&nbsp;&nbsp;&nbsp;&nbsp;أصواته أقل من <b>{w['MEMBER']}</b> بـ <b>{abs(gap):,}</b> صوت<br>"
        return _chat_reply(_chat_html_card("🎯 أقرب الخاسرين إلى مقعد", html), suggestions=["ملخص", "من فاز"], context=ctx)

    # --- 6) quotient ---
    if has("حاصل"):
        if num_seats == 0:
            return _chat_reply("لا توجد بيانات مقاعد.", context=ctx)
        return _chat_reply(f"📊 الحاصل الانتخابي ≈ <b>{int(total_votes / num_seats):,}</b> صوتاً ({total_votes:,} صوت ÷ {num_seats} مقعد).",
                           suggestions=["ملخص", "من فاز"], context=ctx)

    # --- 7) winners (all / by religion / by list) ---
    religions = sorted({str(r) for r in df["RELIGION"].unique()})
    rel_hit = next((r for r in religions if len(_nrm(r)) > 2 and _nrm(r) in m_norm), None)
    if has("فائز", "فاز", "winners", "من ربح", "نواب"):
        winners = df[df["IS_WINNER"] == True]
        if winners.empty:
            return _chat_reply("لا يوجد فائزون حالياً. اضغط Recalculate أولاً.", context=ctx)
        if rel_hit:
            sub = winners[winners["RELIGION"] == rel_hit].sort_values("VOTES", ascending=False)
            html = "".join(f"✅ <b>{r['MEMBER']}</b> — {int(r['VOTES']):,} ({group_labels[r['GROUP']]}) • {r['DISTRICT']}<br>"
                           for _, r in sub.iterrows()) or "لا يوجد."
            return _chat_reply(_chat_html_card(f"🏆 فائزو {rel_hit} ({len(sub)})", html), suggestions=["ملخص"], context=ctx)
        text = strip_keywords(msg, ["من", "فاز", "فائزين", "الفائزين", "فائزو", "لائحة", "لائحه", "في", "winners", "?", "؟"])
        target_grp = None
        if _chat_tokens(text):
            target_grp, amb = resolve_group(text, False)
            if amb: return amb
        if target_grp is not None:
            ctx["group"] = target_grp
            sub = winners[winners["GROUP"] == target_grp].sort_values("VOTES", ascending=False)
            html = "".join(f"✅ {r['MEMBER']} — {int(r['VOTES']):,} • {r['RELIGION']}<br>" for _, r in sub.iterrows()) or "لا فائزين في هذه اللائحة."
            return _chat_reply(_chat_html_card(f"🏆 فائزو {group_labels[target_grp]} ({len(sub)})", html),
                               suggestions=[f"لائحة {group_labels[target_grp]}", f"كم يلزم {group_labels[target_grp]} مقعد"], context=ctx)
        html = ""
        by_group = winners.groupby("GROUP")["MEMBER"].apply(list).to_dict()
        for grp, names in sorted(by_group.items(), key=lambda x: len(x[1]), reverse=True):
            html += f"<b style='color:#0d6efd;'>{group_labels[grp]}</b> ({len(names)}):<br>" + "".join(f"&nbsp;&nbsp;✅ {n}<br>" for n in names) + "<br>"
        return _chat_reply(_chat_html_card(f"🏆 الفائزون ({len(winners)} مقعد)", html), suggestions=["ملخص", "اقرب الخاسرين"], context=ctx)

    # --- 8) summary ---
    if has("ملخص", "summary", "توزيع", "احصائيات", "المقاعد"):
        winners = df[df["IS_WINNER"] == True]
        html = (f"👥 المرشحون: <b>{len(df)}</b><br>🗳️ مجموع الأصوات: <b>{total_votes:,}</b><br>"
                f"💺 المقاعد: <b>{num_seats}</b><br>📋 اللوائح: <b>{len(groups_all)}</b><br>")
        if num_seats > 0:
            html += f"📐 الحاصل: <b>{int(total_votes / num_seats):,}</b><br>"
        html += "<br><b style='color:#43a047;'>🏆 توزيع المقاعد:</b><br>"
        if not winners.empty:
            for grp, cnt in winners.groupby("GROUP")["MEMBER"].count().sort_values(ascending=False).items():
                grp_total = int(df[df["GROUP"] == grp]["VOTES"].sum())
                bar = int(cnt / max(1, num_seats) * 100)
                html += (f"<div style='margin:3px 0;'><div style='display:flex;justify-content:space-between;font-size:0.78rem;'><span>{group_labels[grp]}</span>"
                         f"<span><b>{int(cnt)}</b> مقعد • {grp_total:,} صوت</span></div><div style='background:#e0e3e8;border-radius:4px;height:6px;margin-top:2px;'>"
                         f"<div style='background:linear-gradient(90deg,#667eea,#764ba2);width:{bar}%;height:100%;border-radius:4px;'></div></div></div>")
        else:
            html += "لم يتم حساب النتائج بعد."
        return _chat_reply(_chat_html_card("📊 ملخص الدائرة", html), suggestions=["من فاز", "اقرب الخاسرين", "اعلى المرشحين"], context=ctx)

    # --- 9) top candidates ---
    if has("اعلى", "top", "ترتيب", "الاكثر", "اكثر"):
        n = 10
        nm = re.search(r"(\d+)", m_norm)
        if nm:
            n = max(1, min(30, int(nm.group(1))))
        top_df = df.sort_values("VOTES", ascending=False).head(n)
        mx = max(1, int(top_df.iloc[0]["VOTES"]))
        html = ""
        for i, (_, r) in enumerate(top_df.iterrows(), 1):
            v = int(r["VOTES"])
            html += (f"<div style='margin-bottom:4px;'><b>{i}.</b> {r['MEMBER']} {'🏆' if r['IS_WINNER'] else ''} — <b>{v:,}</b>"
                     f" <span style='color:#6c757d;font-size:0.7rem;'>({group_labels[r['GROUP']]})</span>"
                     f"<div style='background:#e0e3e8;border-radius:3px;height:4px;margin-top:1px;'><div style='background:linear-gradient(90deg,#667eea,#764ba2);"
                     f"width:{int(v / mx * 100)}%;height:100%;border-radius:3px;'></div></div></div>")
        return _chat_reply(_chat_html_card(f"🏅 أعلى {n} مرشحين", html), suggestions=["ملخص"], context=ctx)

    # --- 10) list details ---
    if has("لائحه", "قائمه", "list"):
        text = strip_keywords(msg, ["لائحة", "لائحه", "قائمة", "list", "تفاصيل", "عن", "ما", "هي", "هل", "?", "؟"])
        grp, amb = resolve_group(text)
        if amb: return amb
        if grp is not None:
            return list_reply(grp)

    # --- 11) candidate details (keywords, follow-ups, or a bare name) ---
    text = strip_keywords(msg, ["أصوات", "اصوات", "صوت", "كم", "votes", "عن", "معلومات", "تفاصيل", "مرشح", "هل", "؟", "?"])
    row, amb = resolve_candidate(text)
    if amb: return amb
    if row is not None:
        remember_candidate(row)
        return _chat_reply(candidate_card(row),
                           suggestions=[f"بأي لائحة يفوز {row['MEMBER']}", f"لائحة {group_labels[row['GROUP']]}"], context=ctx)
    g, amb = resolve_group(text, False)
    if amb: return amb
    if g is not None:
        return _chat_reply(f"وجدت لائحة <b>{group_labels[g]}</b>.", suggestions=[f"لائحة {group_labels[g]}"], context=ctx)

    return _chat_reply(_chat_help_html(prefix="لم أفهم سؤالك تماماً 🤔 — جرّب أحد هذه الأمثلة:"),
                       suggestions=["ملخص", "من فاز", "اقرب الخاسرين", "اعلى المرشحين"], context=ctx)


if __name__ == "__main__":
    app.config['TEMPLATES_AUTO_RELOAD'] = True 
    app.run(debug=True, port=5000)
