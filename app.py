from flask import Flask, render_template, abort, request, jsonify, send_file, g
import pandas as pd
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
            district = str(row.get("DISTRICT", "")).strip()
            religion = str(row.get("RELIGION", "")).strip()
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
    year, district_code = parse_file_id(file_id)
    if year is None:
        return
    winner_names = sorted({str(name) for name in winners})
    with DB_ENGINE.begin() as conn:
        conn.execute(text("""
            UPDATE election_members
            SET is_winner = FALSE, updated_at = NOW()
            WHERE year = :year AND district_code = :district_code
        """), {"year": year, "district_code": district_code})
        if winner_names:
            stmt = text("""
                UPDATE election_members
                SET is_winner = TRUE, updated_at = NOW()
                WHERE year = :year AND district_code = :district_code
                  AND member IN :winner_names
            """).bindparams(bindparam("winner_names", expanding=True))
            conn.execute(stmt, {
                "year": year,
                "district_code": district_code,
                "winner_names": winner_names,
            })


def update_candidate_group_db(file_id: str, candidate_name: str, new_group: str):
    year, district_code = parse_file_id(file_id)
    if year is None or not candidate_name or not str(new_group).strip():
        raise ValueError("Invalid election, candidate, or list.")
    with DB_ENGINE.begin() as conn:
        row = conn.execute(text("""
            SELECT group_name
            FROM election_members
            WHERE year = :year AND district_code = :district_code AND member = :member
            ORDER BY id
            LIMIT 1
            FOR UPDATE
        """), {"year": year, "district_code": district_code, "member": candidate_name}).mappings().first()
        if row is None:
            raise LookupError(f"Candidate {candidate_name} not found.")
        old_group = str(row["group_name"])
        conn.execute(text("""
            UPDATE election_members
            SET group_name = :new_group, updated_at = NOW()
            WHERE year = :year AND district_code = :district_code AND member = :member
        """), {
            "new_group": str(new_group).strip(), "year": year,
            "district_code": district_code, "member": candidate_name,
        })
    return old_group


def swap_candidate_groups_db(file_id: str, source_name: str, target_name: str):
    year, district_code = parse_file_id(file_id)
    if year is None or not source_name or not target_name or source_name == target_name:
        raise ValueError("Choose two different candidates.")

    stmt = text("""
        SELECT member, group_name, religion, district
        FROM election_members
        WHERE year = :year AND district_code = :district_code
          AND member IN :names
        ORDER BY id
        FOR UPDATE
    """).bindparams(bindparam("names", expanding=True))

    with DB_ENGINE.begin() as conn:
        rows = conn.execute(stmt, {
            "year": year, "district_code": district_code,
            "names": [source_name, target_name],
        }).mappings().all()
        by_name = {str(row["member"]): row for row in rows}
        if source_name not in by_name or target_name not in by_name:
            raise LookupError("One or both candidates were not found.")
        source = by_name[source_name]
        target = by_name[target_name]
        if str(source["religion"]) != str(target["religion"]) or str(source["district"]) != str(target["district"]):
            raise ValueError("Candidates must have the same religion and district.")

        source_group = str(source["group_name"])
        target_group = str(target["group_name"])
        conn.execute(text("""
            UPDATE election_members
            SET group_name = CASE
                WHEN member = :source_name THEN :target_group
                WHEN member = :target_name THEN :source_group
                ELSE group_name
            END,
            updated_at = NOW()
            WHERE year = :year AND district_code = :district_code
              AND member IN (:source_name, :target_name)
        """), {
            "source_name": source_name, "target_name": target_name,
            "source_group": source_group, "target_group": target_group,
            "year": year, "district_code": district_code,
        })
    return {
        "source_group": source_group,
        "target_group": target_group,
        "religion": str(source["religion"]),
        "district": str(source["district"]),
    }


def multi_swap_candidate_groups_db(file_id: str, swaps):
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

        rel_limits = {}
        dist_totals = {}
        if "DISTRICT" in seats_df.columns and "RELIGION" in seats_df.columns:
            for _, row in seats_df.iterrows():
                d = str(row.get("DISTRICT", "")).strip()
                r = str(row.get("RELIGION", "")).strip()
                if not d or not r:
                    continue
                rel_limits[(d, r)] = rel_limits.get((d, r), 0) + 1
                dist_totals[d] = dist_totals.get(d, 0) + 1

        _store_request_quota(file_id, {"rel_limits": rel_limits, "dist_totals": dist_totals})

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
        df_final = merged[["MEMBER", "GROUP", "RELIGION", "DISTRICT", "VOTES"]].copy()

        winners = _compute_winners_from_quota(file_id, df_final)
        df_final["IS_WINNER"] = df_final["MEMBER"].astype(str).isin(winners)

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

    df_group = df.groupby("GROUP", as_index=False)["VOTES"].sum()
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
    df_group_adjusted["INITIAL_SEATS"] = df_group_adjusted["QUOTIENT"].apply(lambda x: math.floor(round(x, 3)))
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
        return round((votes / dist_total) * 100, 3)

    df_final_members["VOTE_PERC"] = df_final_members.apply(get_perc, axis=1)
    df_final_members = df_final_members.sort_values(by="VOTE_PERC", ascending=False).reset_index(drop=True)

    winners = set()
    group_seats_filled = {g: 0 for g in group_seats_won}

    for _, row in df_final_members.iterrows():
        c_name, c_dist, c_rel, c_group = row["MEMBER"], row["DISTRICT"], row["RELIGION"], row["GROUP"]
        won, filled = group_seats_won.get(c_group, 0), group_seats_filled.get(c_group, 0)
        
        if won - filled > 0:
            mask = (df_seats["HOLDER"] == "NA") & (df_seats["DISTRICT"] == c_dist) & (df_seats["RELIGION"] == c_rel)
            if mask.any():
                seat_idx = df_seats[mask].index[0]
                df_seats.loc[seat_idx, "HOLDER"] = c_name
                df_seats.loc[seat_idx, "GROUP"] = c_group
                group_seats_filled[c_group] += 1
                winners.add(str(c_name))

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
    current_winners = _compute_winners_from_quota("__tmp__", current_df) if False else set(current_df[current_df["IS_WINNER"] == True]["MEMBER"].astype(str))
    current_seats = group_seat_map(current_df)
    rows = []
    for g in groups:
        ov = int(original_df[original_df["GROUP"].astype(str) == g]["VOTES"].sum())
        cv = int(current_df[current_df["GROUP"].astype(str) == g]["VOTES"].sum())
        os = int(original_seats.get(g, 0))
        cs = int(current_seats.get(g, 0))
        rows.append({
            "group": g, "original_votes": ov, "current_votes": cv,
            "vote_change": cv - ov, "original_seats": os, "current_seats": cs,
            "seat_change": cs - os
        })
    rows.sort(key=lambda r: (r["seat_change"], r["current_votes"]), reverse=True)
    return rows

# =========================================================
#  FAST ON-DEMAND SUGGESTION ENGINE
# =========================================================
def count_group_winners(df: pd.DataFrame, winners: set) -> dict:
    winner_df = df[df["MEMBER"].astype(str).isin(winners)].copy()
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

    cache_id = (file_id, target_group, str(target_k), get_df_version(df))
    if cache_id in SUGGESTION_CACHE: return SUGGESTION_CACHE[cache_id]

    df_group = df.groupby("GROUP", as_index=False)["VOTES"].sum()
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
    df_adj["INITIAL_SEATS"] = df_adj["QUOTIENT"].apply(lambda x: math.floor(round(x, 3)))
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

    atomic_swaps = build_atomic_swaps_for_group(df, target_group)
    suggestions = []

    if k == 1:
        for s in atomic_swaps[:5]:
            sim = simulate_combo_gain(file_id, df, target_group, [s], current_seats_actual)
            if sim and sim["seat_gain"] >= 1:
                suggestions.append({"type": "1_swap", "seat_gain": sim["seat_gain"], "total_gain": s["net_gain"], "swaps": [s]})        #simulate the effect of each atomic swap on the number of seats won by the target group and add valid suggestions to the list
    else:
        combo_size = min(k, 3)
        tested = 0
        for combo in combinations(atomic_swaps, combo_size):
            if tested >= (300 if k == 2 else 150): break
            tested += 1
            if not is_valid_combo(combo): continue
            total_gain = sum(x["net_gain"] for x in combo)
            if total_gain < max(1, math.floor(needed * 0.75)): continue
            sim = simulate_combo_gain(file_id, df, target_group, combo, current_seats_actual)
            if sim and sim["seat_gain"] >= k:
                suggestions.append({"type": f"{combo_size}_swap", "seat_gain": sim["seat_gain"], "total_gain": total_gain, "swaps": list(combo)})
        suggestions = sorted(suggestions, key=lambda x: (x["total_gain"], len(x["swaps"])))[:5]         #sort the suggestions by total gain and number of swaps, and keep only the top 5 suggestions

    result = {"passed": bool(passed), "current_seats": current_seats_actual, "votes": needed, "suggestions": suggestions}
    SUGGESTION_CACHE[cache_id] = result
    return result


# =========================================================
#  ADDED FEATURE HELPERS: EXPORT + COMPARISON
# =========================================================
def safe_sheet_name(name: str) -> str:
    cleaned = re.sub(r"[\\/*?:\[\]]", "_", str(name or "Sheet"))[:31]
    return cleaned or "Sheet"       #return a safe sheet name for Excel by replacing invalid characters and limiting the length to 31 characters

def safe_download_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", str(name or "election_report")).strip("_")           #return a safe download name for files by replacing invalid characters with underscores and stripping leading/trailing underscores

def get_region_by_file_id(file_id: str):
    for slug, info in REGION_MAP.items():
        if file_id in info.get("files", {}).values():
            return slug, info
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

        fig = go.Figure()
        fig.add_bar(name="2018", x=merged["GROUP"].tolist(), y=merged["votes_2018"].tolist())
        fig.add_bar(name="2022", x=merged["GROUP"].tolist(), y=merged["votes_2022"].tolist())
        fig.update_layout(
            barmode="group", height=420, margin=dict(l=30, r=20, t=30, b=110),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            yaxis_title="Votes", xaxis_tickangle=-35, legend_title="Year"
        )
        payload["chart_json"] = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

        w18 = set(y18.get("winners_df", pd.DataFrame(columns=["MEMBER"]))["MEMBER"].astype(str).tolist()) if y18.get("available") else set()
        w22 = set(y22.get("winners_df", pd.DataFrame(columns=["MEMBER"]))["MEMBER"].astype(str).tolist()) if y22.get("available") else set()
        payload["winner_changes"] = {
            "kept": sorted(list(w18 & w22)),
            "new": sorted(list(w22 - w18)),
            "lost": sorted(list(w18 - w22)),
        }

    return payload

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
            "Map data is missing from PostgreSQL. Run schema_postgres.sql and "
            "the one-time migrations/import_map_to_postgres.py script.",
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

    unique_districts, seen_labels = [], set()
    for region in sorted(available_regions, key=lambda item: (item["district_label"], item["name"])):
        label = region["district_label"]
        if label == "Unknown" or "Beirut 3" in label or region["name"] == "Beirut-three":
            continue
        if label not in seen_labels:
            seen_labels.add(label)
            unique_districts.append({**region, "active_year": selected_year})

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

    df_agg = df.groupby("MEMBER", as_index=False).agg({"GROUP": "first", "RELIGION": "first", "DISTRICT": "first", "IS_WINNER": "max", "VOTES": "sum"})
    
    candidates = [{"name": str(r["MEMBER"]), "group": str(r["GROUP"]), "votes": int(r["VOTES"]), "religion": str(r["RELIGION"]), "district": str(r["DISTRICT"]), "is_winner": bool(r["IS_WINNER"])} for _, r in df_agg.iterrows()]

    grouped = {}
    for c in candidates: grouped.setdefault(c["group"], []).append(c)
    for g in grouped: grouped[g].sort(key=lambda x: x["votes"], reverse=True)
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
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            tables["list_summary"].to_excel(writer, index=False, sheet_name="List Summary")
            tables["winners"].to_excel(writer, index=False, sheet_name="Winners")
            tables["candidates"].to_excel(writer, index=False, sheet_name="Candidates")
            tables["district_summary"].to_excel(writer, index=False, sheet_name="District Summary")

            for sheet_name in writer.sheets:
                ws = writer.sheets[sheet_name]
                ws.freeze_panes = "A2"
                for col_cells in ws.columns:
                    max_len = max(len(str(cell.value or "")) for cell in col_cells)
                    ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 45)

        output.seek(0)
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

        # Suggestions can depend on the current list assignments, so invalidate
        # them after a reset when the cache exists.
        try:
            SUGGESTION_CACHE.clear()
        except Exception:
            pass

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
    current_df["IS_WINNER"] = current_df["MEMBER"].astype(str).isin(current_winners)

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
    winners_df = target_df[target_df["MEMBER"].astype(str).isin(current_winners)]
    non_winners_df = target_df[~target_df["MEMBER"].astype(str).isin(current_winners)]

    best_candidate = None
    if not non_winners_df.empty:
        r = non_winners_df.iloc[0]
        best_candidate = {"name": str(r["MEMBER"]), "votes": int(r["VOTES"]), "religion": str(r["RELIGION"]), "district": str(r["DISTRICT"])}

    weakest_winner = None
    other_winners = df[(df["MEMBER"].astype(str).isin(current_winners)) & (df["GROUP"] != target_group)].copy().sort_values("VOTES", ascending=True)
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

@app.route("/api/analyze_virtual_list", methods=["POST"])
def analyze_virtual_list():
    data = request.json or {}
    file_id = data.get("filename")
    selected = data.get("candidates", [])
    df = get_candidates_df(file_id)
    quota = get_quota(file_id)
    num_seats = sum(quota.get("rel_limits", {}).values())

    if df is None or df.empty or num_seats == 0:
        return jsonify({"success": False, "message": "Election data is unavailable."}), 400

    selected_names = []
    for candidate in selected:
        if isinstance(candidate, dict) and candidate.get("name"):
            selected_names.append(str(candidate["name"]))
        elif isinstance(candidate, str):
            selected_names.append(candidate)
    selected_names = list(dict.fromkeys(selected_names))
    if not selected_names:
        return jsonify({"success": False, "message": "Choose at least one candidate."}), 400

    selected_df = df[df["MEMBER"].astype(str).isin(selected_names)]
    total_votes = int(selected_df["VOTES"].sum())
    quotient = float(df["VOTES"].sum()) / num_seats

    return jsonify({
        "success": True,
        "total_votes": total_votes,
        "quotient": int(quotient),
        "reaches_quotient": total_votes >= quotient,
        "matched_candidates": int(selected_df["MEMBER"].nunique()),
    })
@app.route("/api/change_candidate_group", methods=["POST"])
def change_candidate_group():
    data = request.json or {}
    file_id = data.get("filename")
    candidate_name = data.get("candidate_name")
    new_group = data.get("new_group")
    try:
        old_group = update_candidate_group_db(file_id, candidate_name, new_group)
        recompute_winners(file_id)
        df = get_candidates_df(file_id)
        return jsonify({
            "success": True,
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
    try:
        swap = swap_candidate_groups_db(file_id, source_name, target_name)
        recompute_winners(file_id)
        df = get_candidates_df(file_id)
        source_group = swap["source_group"]
        target_group = swap["target_group"]
        return jsonify({
            "success": True,
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
        recompute_winners(file_id)
        return jsonify({"success": True, "updated_candidates": updated})
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
        return jsonify({
            "success": True,
            "winner_names": sorted(winners),
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
                "group": target_group,
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
                "group": target_group,
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
        would_win = candidate_name in sim_winners

        results.append({
            "group": target_group,
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
        "current_group": current_group,
        "currently_winner": is_currently_winner,
        "results": results
    })

@app.route("/api/chat", methods=["POST"])
def api_chat():
    data = request.json or {}
    msg, fid = str(data.get("message", "")).strip(), data.get("filename")
    df = get_candidates_df(fid)
    if df is None:
        return jsonify({"reply": "عذراً، البيانات غير متوفرة حالياً.", "action": "none"})

    msg_lower = msg.lower()
    all_names = df["MEMBER"].astype(str).tolist()

    def fuzzy_find(name_input):
        """Find closest candidate name using fuzzy matching."""
        name_input = name_input.strip()
        # Exact match first
        exact = [n for n in all_names if n == name_input]
        if exact: return exact[0]
        # Substring match
        substr = [n for n in all_names if name_input in n or n in name_input]
        if len(substr) == 1: return substr[0]
        # Fuzzy match
        matches = difflib.get_close_matches(name_input, all_names, n=1, cutoff=0.5)
        return matches[0] if matches else None

    # --- 1) SWAP COMMAND: "بدل X مع Y" ---
    swap_patterns = [
        r"بدل\s+(.+?)\s+مع\s+(.+)",
        r"بدّل\s+(.+?)\s+مع\s+(.+)",
        r"تبديل\s+(.+?)\s+مع\s+(.+)",
        r"swap\s+(.+?)\s+with\s+(.+)",
    ]
    for pattern in swap_patterns:
        m = re.search(pattern, msg, re.IGNORECASE)
        if m:
            src_input, tgt_input = m.group(1).strip(), m.group(2).strip()
            src_name = fuzzy_find(src_input)
            tgt_name = fuzzy_find(tgt_input)

            if not src_name:
                return jsonify({"reply": f"❌ لم أجد مرشحاً باسم <b>{src_input}</b>.", "action": "none"})
            if not tgt_name:
                return jsonify({"reply": f"❌ لم أجد مرشحاً باسم <b>{tgt_input}</b>.", "action": "none"})

            src_row = df[df["MEMBER"] == src_name].iloc[0]
            tgt_row = df[df["MEMBER"] == tgt_name].iloc[0]

            if str(src_row["RELIGION"]) != str(tgt_row["RELIGION"]) or str(src_row["DISTRICT"]) != str(tgt_row["DISTRICT"]):
                return jsonify({
                    "reply": f"❌ لا يمكن التبادل بين <b>{src_name}</b> ({src_row['RELIGION']} - {src_row['DISTRICT']}) و <b>{tgt_name}</b> ({tgt_row['RELIGION']} - {tgt_row['DISTRICT']}).<br>يجب أن يكونا من نفس الطائفة والدائرة.",
                    "action": "none"
                })

            # Persist the swap in PostgreSQL.
            sg, tg = str(src_row["GROUP"]), str(tgt_row["GROUP"])
            try:
                swap_candidate_groups_db(fid, src_name, tgt_name)
                recompute_winners(fid)
            except Exception as exc:
                return jsonify({"reply": f"❌ فشل تحديث قاعدة البيانات: {exc}", "action": "none"})

            return jsonify({
                "reply": f"✅ تم التبادل وحفظه في قاعدة البيانات!<br><b>{src_name}</b> ← {tg}<br><b>{tgt_name}</b> ← {sg}",
                "action": "swap_done"
            })

    # --- 2) QUOTIENT QUERY ---
    if "حاصل" in msg:
        num_seats = sum(get_quota(fid).get("rel_limits", {}).values())
        if num_seats == 0:
            return jsonify({"reply": "لا توجد بيانات مقاعد.", "action": "none"})
        eq1 = df["VOTES"].sum() / num_seats
        return jsonify({"reply": f"📊 الحاصل الانتخابي يبلغ حالياً <b>{int(eq1):,}</b> صوتاً.", "action": "none"})

    # --- 3) WINNER QUERY: "فائزين" or "من فاز" ---
    if any(w in msg for w in ["فائز", "فاز", "winners", "الفائز", "فائزين", "من ربح"]):
        winners = df[df["IS_WINNER"] == True]
        if winners.empty:
            return jsonify({"reply": "لا يوجد فائزون حالياً. اضغط Recalculate أولاً.", "action": "none"})

        by_group = winners.groupby("GROUP")["MEMBER"].apply(list).to_dict()
        html = f"🏆 <b>الفائزون ({len(winners)} مقعد):</b><br><br>"
        for grp, names in sorted(by_group.items(), key=lambda x: len(x[1]), reverse=True):
            html += f"<b style='color:#0d6efd;'>{grp}</b> ({len(names)}):<br>"
            for n in names:
                html += f"  ✅ {n}<br>"
            html += "<br>"
        return jsonify({"reply": html, "action": "none"})

    # --- 4) COMPARE COMMAND: "قارن X مع Y" ---
    compare_patterns = [
        r"قارن\s+(.+?)\s+مع\s+(.+)",
        r"compare\s+(.+?)\s+with\s+(.+)",
        r"مقارنة\s+(.+?)\s+و\s+(.+)",
    ]
    for pattern in compare_patterns:
        m = re.search(pattern, msg, re.IGNORECASE)
        if m:
            name_a_input, name_b_input = m.group(1).strip(), m.group(2).strip()
            name_a = fuzzy_find(name_a_input)
            name_b = fuzzy_find(name_b_input)
            if not name_a:
                return jsonify({"reply": f"❌ لم أجد مرشحاً باسم <b>{name_a_input}</b>.", "action": "none"})
            if not name_b:
                return jsonify({"reply": f"❌ لم أجد مرشحاً باسم <b>{name_b_input}</b>.", "action": "none"})
            row_a = df[df["MEMBER"] == name_a].iloc[0]
            row_b = df[df["MEMBER"] == name_b].iloc[0]
            votes_a, votes_b = int(row_a["VOTES"]), int(row_b["VOTES"])
            diff = abs(votes_a - votes_b)
            leader = name_a if votes_a >= votes_b else name_b
            status_a = "🏆" if row_a["IS_WINNER"] else "❌"
            status_b = "🏆" if row_b["IS_WINNER"] else "❌"

            # Build comparison card
            html = f"<div style='font-size:0.82rem;'>"
            html += f"<b style='color:#667eea;'>⚖️ مقارنة بين مرشحين</b><br><br>"
            html += f"<div style='display:flex;gap:8px;'>"
            html += f"<div style='flex:1;background:#f8f9fc;padding:8px;border-radius:8px;border-right:3px solid #667eea;'>"
            html += f"<b>{name_a}</b> {status_a}<br>"
            html += f"<span style='color:#6c757d;font-size:0.75rem;'>{row_a['RELIGION']} • {row_a['DISTRICT']}</span><br>"
            html += f"اللائحة: {row_a['GROUP']}<br>"
            html += f"الأصوات: <b>{votes_a:,}</b>"
            html += f"</div>"
            html += f"<div style='flex:1;background:#f8f9fc;padding:8px;border-radius:8px;border-right:3px solid #764ba2;'>"
            html += f"<b>{name_b}</b> {status_b}<br>"
            html += f"<span style='color:#6c757d;font-size:0.75rem;'>{row_b['RELIGION']} • {row_b['DISTRICT']}</span><br>"
            html += f"اللائحة: {row_b['GROUP']}<br>"
            html += f"الأصوات: <b>{votes_b:,}</b>"
            html += f"</div></div>"
            html += f"<br><b>الفرق:</b> {diff:,} صوت لصالح <b>{leader}</b>"

            can_swap = str(row_a["RELIGION"]) == str(row_b["RELIGION"]) and str(row_a["DISTRICT"]) == str(row_b["DISTRICT"])
            if can_swap:
                html += f"<br><span style='color:#43a047;'>✅ يمكن تبادلهما (نفس الطائفة والدائرة)</span>"
            else:
                html += f"<br><span style='color:#e53935;'>❌ لا يمكن تبادلهما (طائفة أو دائرة مختلفة)</span>"
            html += "</div>"
            return jsonify({"reply": html, "action": "none"})

    # --- 5) SUMMARY COMMAND: "ملخص" ---
    if any(w in msg for w in ["ملخص", "summary", "توزيع", "إحصائيات"]):
        num_seats = sum(get_quota(fid).get("rel_limits", {}).values())
        total_votes = int(df["VOTES"].sum())
        total_candidates = len(df)
        groups = df["GROUP"].unique()
        winners = df[df["IS_WINNER"] == True]

        html = "<div style='font-size:0.82rem;'>"
        html += f"<b style='color:#667eea;'>📊 ملخص الدائرة</b><br><br>"
        html += f"👥 المرشحون: <b>{total_candidates}</b><br>"
        html += f"🗳️ مجموع الأصوات: <b>{total_votes:,}</b><br>"
        html += f"💺 المقاعد: <b>{num_seats}</b><br>"
        html += f"📋 عدد اللوائح: <b>{len(groups)}</b><br>"
        if num_seats > 0:
            eq = total_votes / num_seats
            html += f"📐 الحاصل الانتخابي: <b>{int(eq):,}</b><br>"
        html += f"<br><b style='color:#43a047;'>🏆 توزيع المقاعد:</b><br>"

        if not winners.empty:
            by_group = winners.groupby("GROUP")["MEMBER"].count().sort_values(ascending=False)
            for grp, cnt in by_group.items():
                grp_total = int(df[df["GROUP"] == grp]["VOTES"].sum())
                bar_width = int((cnt / max(1, num_seats)) * 100)
                html += f"<div style='margin:3px 0;'>"
                html += f"<div style='display:flex;justify-content:space-between;font-size:0.78rem;'><span>{grp}</span><span><b>{int(cnt)}</b> مقعد • {grp_total:,} صوت</span></div>"
                html += f"<div style='background:#e0e3e8;border-radius:4px;height:6px;margin-top:2px;'>"
                html += f"<div style='background:linear-gradient(90deg,#667eea,#764ba2);width:{bar_width}%;height:100%;border-radius:4px;'></div>"
                html += f"</div></div>"
        else:
            html += "لم يتم حساب النتائج بعد. اضغط <b>Recalculate</b>."
        html += "</div>"
        return jsonify({"reply": html, "action": "none"})

    if any(w in msg for w in ["أصوات", "صوت", "كم", "votes"]):
        best_match = None
        best_score = 0
        for name in all_names:
            if name in msg:
                if len(name) > best_score:
                    best_match = name
                    best_score = len(name)
        if not best_match:
            cleaned = re.sub(r"(أصوات|صوت|كم|votes)\s*", "", msg).strip()
            if cleaned:
                best_match = fuzzy_find(cleaned)

        if best_match:
            row = df[df["MEMBER"] == best_match].iloc[0]
            is_winner = bool(row["IS_WINNER"])
            status_icon = "🏆" if is_winner else "❌"
            status_text = "فائز" if is_winner else "خاسر"
            status_color = "#43a047" if is_winner else "#e53935"
            votes = int(row["VOTES"])
            total_in_list = int(df[df["GROUP"] == row["GROUP"]]["VOTES"].sum())
            pct = round((votes / max(1, total_in_list)) * 100, 1)

            html = f"<div style='font-size:0.82rem;'>"
            html += f"<div style='background:#f8f9fc;padding:10px;border-radius:10px;border-right:3px solid {status_color};'>"
            html += f"<b style='font-size:0.9rem;'>{best_match}</b> <span style='color:{status_color};'>{status_icon} {status_text}</span><br>"
            html += f"<span style='color:#6c757d;font-size:0.75rem;'>{row['RELIGION']} • {row['DISTRICT']}</span><br><br>"
            html += f"📋 اللائحة: <b>{row['GROUP']}</b><br>"
            html += f"🗳️ الأصوات: <b>{votes:,}</b> ({pct}% من لائحته)<br>"
            html += f"<div style='background:#e0e3e8;border-radius:4px;height:5px;margin-top:4px;'>"
            html += f"<div style='background:linear-gradient(90deg,#667eea,#764ba2);width:{min(100,pct)}%;height:100%;border-radius:4px;'></div>"
            html += f"</div>"
            html += "</div></div>"
            return jsonify({"reply": html, "action": "none"})

    # --- 7) LIST QUERY: "لائحة X" ---
    if any(w in msg for w in ["لائحة", "list", "قائمة"]):
        for grp in df["GROUP"].unique():
            if grp.lower() in msg_lower or msg_lower in grp.lower():
                grp_df = df[df["GROUP"] == grp].sort_values("VOTES", ascending=False)
                total = int(grp_df["VOTES"].sum())
                winners_count = int(grp_df["IS_WINNER"].sum())
                num_seats = sum(get_quota(fid).get("rel_limits", {}).values())
                eq = total / max(1, num_seats) if num_seats > 0 else 0

                html = f"<div style='font-size:0.82rem;'>"
                html += f"<b style='color:#667eea;'>📋 {grp}</b><br>"
                html += f"المجموع: <b>{total:,}</b> صوت | الفائزون: <b>{winners_count}</b><br>"
                if num_seats > 0:
                    pct = round((total / max(1, df["VOTES"].sum())) * 100, 1)
                    html += f"نسبة الأصوات: <b>{pct}%</b><br>"
                html += "<br>"
                for idx, (_, r) in enumerate(grp_df.iterrows()):
                    icon = "✅" if r["IS_WINNER"] else "•"
                    rank = idx + 1
                    html += f"{icon} <b>{rank}.</b> {r['MEMBER']} — <b>{int(r['VOTES']):,}</b><br>"
                html += "</div>"
                return jsonify({"reply": html, "action": "none"})

    # --- 8) TOP CANDIDATES: "أعلى" or "ترتيب" ---
    if any(w in msg for w in ["أعلى", "top", "ترتيب", "أكثر"]):
        top_n = 10
        top_df = df.sort_values("VOTES", ascending=False).head(top_n)
        max_votes = int(top_df.iloc[0]["VOTES"]) if not top_df.empty else 1
        html = f"<div style='font-size:0.82rem;'>"
        html += f"<b style='color:#667eea;'>🏅 أعلى {top_n} مرشحين</b><br><br>"
        for idx, (_, r) in enumerate(top_df.iterrows()):
            rank = idx + 1
            votes = int(r["VOTES"])
            bar_w = int((votes / max(1, max_votes)) * 100)
            icon = "🏆" if r["IS_WINNER"] else ""
            html += f"<div style='margin-bottom:4px;'>"
            html += f"<b>{rank}.</b> {r['MEMBER']} {icon} — <b>{votes:,}</b>"
            html += f" <span style='color:#6c757d;font-size:0.7rem;'>({r['GROUP']})</span>"
            html += f"<div style='background:#e0e3e8;border-radius:3px;height:4px;margin-top:1px;'>"
            html += f"<div style='background:linear-gradient(90deg,#667eea,#764ba2);width:{bar_w}%;height:100%;border-radius:3px;'></div>"
            html += f"</div></div>"
        html += "</div>"
        return jsonify({"reply": html, "action": "none"})

    # --- FALLBACK ---
    return jsonify({
        "reply": "<div style='font-size:0.82rem;'>يمكنني مساعدتك بـ:<br><br>"
                 " لتبديل مرشحين<br>"
                 " لمقارنة مرشحين<br>"
                 "الحاصل الانتخابي<br>"
                 "من فاز — عرض الفائزين<br>"
                 "أصوات [اسم] — تفاصيل مرشح<br>"
                 "لائحة [اسم] — تفاصيل لائحة<br>"
                 "ملخص — ملخص توزيع المقاعد<br>"
                 "أعلى — أعلى 10 مرشحين</div>",
        "action": "none"
    })

if __name__ == "__main__":
    app.config['TEMPLATES_AUTO_RELOAD'] = True 
    app.run(debug=True, port=5000)