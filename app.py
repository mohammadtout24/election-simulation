from flask import Flask, render_template, abort, request, jsonify
import pandas as pd
import json
import os
import re
import math
import difflib
import geopandas as gpd
import plotly.express as px
import plotly
import plotly.graph_objects as go
import plotly.utils
from itertools import combinations

app = Flask(__name__)

# --- GLOBAL CACHE ---
DATA_CACHE = {}   # {file_id: df_final}
QUOTA_CACHE = {}  # {file_id: {'rel_limits': {(dist, rel): count}, 'dist_totals': {dist: count}}}
SUGGESTION_CACHE = {}  # {(file_id, group_name, target_k, version): result}

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

# --- MAPPING ---
DISTRICT_MAPPING = {
    "Beirut-one": "Beirut1_result", "Beirut-two": "Beirut2_result", "Jbayl": "Mount1_result",
    "Kesrouan": "Mount1_result", "Matn": "Mount2_result", "Baabda": "Mount3_result",
    "Aley": "Mount4_result", "Chouf": "Mount4_result", "Akkar": "North1_result",
    "Tripoli": "North2_result", "Minieh-Dannieh": "North2_result", "Miniyeh-Danniyeh": "North2_result",
    "Zgharta": "North3_result", "Bcharreh": "North3_result", "Koura": "North3_result",
    "Batroun": "North3_result", "Zahleh": "Bekaa1_result", "WestBekaa-Rachaya": "Bekaa2_result",
    "Baalbek-Hermel": "Bekaa3_result", "Saida": "South1_result", "Jezzine": "South1_result",
    "Zahrany": "South2_result", "Sour": "South2_result", "Nabatiyeh": "South3_result",
    "Marjayoun-Hasbaya": "South3_result", "Bent Jbayl": "South3_result",
}

REGION_MAP = {}
GDF_GLOBAL = None

# --- PATH SETUP ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAP_DIR = os.path.join(BASE_DIR, "data")
YEAR_DIRS = {
    "2018": os.path.join(BASE_DIR, "2018"),
    "2022": os.path.join(BASE_DIR, "2022"),
}

# =========================================================
#  HELPER FUNCTIONS
# =========================================================
def clean_text(text):
    if not isinstance(text, str): return ""
    return re.sub(r"\s+", " ", text.strip()).lower()

def normalize_slug(text):
    text = str(text).lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")

def format_district_name(base_filename):
    if not base_filename: return "Unknown"
    base = base_filename.split("_")[0]
    return re.sub(r"([a-zA-Z])(\d)", r"\1 \2", base)

def district_code_from_base_key(base_key: str):
    if not base_key: return None
    return base_key.split("_")[0]

def cache_key(year: str, district_code: str) -> str:
    return f"{year}:{district_code}"

def district_files(year: str, district_code: str) -> dict:
    root = os.path.join(YEAR_DIRS[str(year)], str(district_code))
    return {
        "members": os.path.join(root, f"{district_code}_members.xlsx"),
        "seats": os.path.join(root, f"{district_code}_seats.xlsx"),
        "data": os.path.join(root, f"{district_code}_data.xlsx"),
    }

def _std_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df

def _pick(df: pd.DataFrame, names):
    for n in names:
        if n in df.columns: return n
    return None

def _norm_name(x) -> str:
    return re.sub(r"\s+", " ", str(x or "").strip()).lower()

def clear_all_suggestion_cache():
    SUGGESTION_CACHE.clear()

def get_df_version(df: pd.DataFrame) -> int:
    return hash(tuple(df["GROUP"].astype(str).tolist()))

# =========================================================
#  MAP LOAD
# =========================================================
def load_and_prepare_map():
    global REGION_MAP, GDF_GLOBAL
    REGION_MAP = {}

    map_file_path = os.path.join(MAP_DIR, "lebanonmap.xlsx")
    if not os.path.exists(map_file_path):
        print(f"❌ Missing map file: {map_file_path}")
        GDF_GLOBAL = None
        return

    df_map = pd.read_excel(map_file_path)
    features = []

    for _, row in df_map.iterrows():
        try:
            map_name = str(row["DISTRICT"]).strip()
            slug = normalize_slug(map_name)

            if slug == "beirut_three":
                slug = "beirut_two"
                map_name = "Beirut-two"

            base_file_key = DISTRICT_MAPPING.get(map_name)
            district_label = "Unknown"
            files_available = {}

            if base_file_key:
                district_label = format_district_name(base_file_key)
                dcode = district_code_from_base_key(base_file_key)

                if dcode:
                    for y in ["2018", "2022"]:
                        paths = district_files(y, dcode)
                        if all(os.path.exists(p) for p in paths.values()):
                            files_available[y] = cache_key(y, dcode)

            REGION_MAP[slug] = {
                "name": map_name,
                "slug": slug,
                "files": files_available,
                "district_label": district_label,
            }

            geometry = json.loads(row["geometry"])
            features.append({
                "type": "Feature",
                "id": slug,
                "geometry": geometry,
                "properties": {"slug": slug, "name": map_name}
            })
        except Exception:
            pass

    if not features:
        GDF_GLOBAL = None
        return

    geojson_obj = {"type": "FeatureCollection", "features": features}
    GDF_GLOBAL = gpd.GeoDataFrame.from_features(geojson_obj)
    GDF_GLOBAL["slug"] = [f["id"] for f in features]

load_and_prepare_map()

def create_interactive_map(target_slugs, selected_year):
    if GDF_GLOBAL is None or GDF_GLOBAL.empty:
        return "{}"

    gdf = GDF_GLOBAL[GDF_GLOBAL["slug"].isin(target_slugs)].copy()

    color_map = {slug: REGION_COLORS.get(slug, DEFAULT_COLOR) for slug in gdf["slug"]}
    gdf["color_code"] = gdf["slug"].map(color_map)

    voters_data = []
    district_names = []

    for slug in gdf["slug"]:
        d_name = REGION_MAP[slug]["district_label"] if slug in REGION_MAP else slug
        district_names.append(d_name)

        total = 0
        if slug in REGION_MAP:
            file_id = REGION_MAP[slug]["files"].get(selected_year)
            if file_id:
                df = get_candidates_df(file_id)
                if df is not None and "VOTES" in df.columns:
                    total = df["VOTES"].sum()

        voters_data.append(f"{int(total):,}")

    gdf["District"] = district_names
    gdf["Total Voters"] = voters_data
    geojson = json.loads(gdf.to_json())

    fig = px.choropleth(
        gdf, geojson=geojson, locations="slug", featureidkey="properties.slug",
        color="slug", color_discrete_map=color_map, hover_name="District",
        hover_data={"slug": False, "color_code": False, "District": False, "Total Voters": True}
    )

    fig.update_traces(marker_line_width=1, marker_line_color="black")
    fig.update_geos(fitbounds="locations", visible=False, bgcolor="rgba(0,0,0,0)")
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0}, showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", dragmode=False
    )

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

# =========================================================
#  CORE DATA LOGIC
# =========================================================
def get_candidates_df(file_id: str):
    if file_id in DATA_CACHE: return DATA_CACHE[file_id]
    if ":" not in str(file_id): return None
    year, district_code = file_id.split(":", 1)
    if year not in YEAR_DIRS: return None
    paths = district_files(year, district_code)
    if not all(os.path.exists(p) for p in paths.values()): return None

    try:
        seats_df = _std_cols(pd.read_excel(paths["seats"], engine="openpyxl")).rename(
            columns={"REGION": "DISTRICT", "QADA": "DISTRICT", "SECT": "RELIGION", "CONFESSION": "RELIGION"}
        )

        rel_limits = {}
        dist_totals = {}
        if "DISTRICT" in seats_df.columns and "RELIGION" in seats_df.columns:
            for _, row in seats_df.iterrows():
                d = str(row.get("DISTRICT", "")).strip()
                r = str(row.get("RELIGION", "")).strip()
                if not d or not r: continue
                rel_limits[(d, r)] = rel_limits.get((d, r), 0) + 1
                dist_totals[d] = dist_totals.get(d, 0) + 1

        QUOTA_CACHE[file_id] = {"rel_limits": rel_limits, "dist_totals": dist_totals}

        members_df = _std_cols(pd.read_excel(paths["members"], engine="openpyxl"))
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

        if "MEMBER" not in members_df.columns: return None
        if "GROUP" not in members_df.columns: members_df["GROUP"] = "Independent"
        if "RELIGION" not in members_df.columns: members_df["RELIGION"] = "Unknown"
        if "DISTRICT" not in members_df.columns: members_df["DISTRICT"] = "General"

        for col in ["MEMBER", "GROUP", "RELIGION", "DISTRICT"]:
            members_df[col] = members_df[col].astype(str).str.strip()
        members_df["_NAME_KEY"] = members_df["MEMBER"].map(_norm_name)

        data_df = _std_cols(pd.read_excel(paths["data"], engine="openpyxl"))
        d_id = _pick(data_df, ["CANDIDATE_ID", "CANDIDATEID", "ID", "CID"])
        d_name = _pick(data_df, ["MEMBER", "CANDIDATE", "NAME", "HOLDER"])
        d_votes = _pick(data_df, ["VOTES", "VOTE", "TOTAL VOTES", "TOTAL_VOTES", "VOTE_LIST", "PREFERENTIAL VOTES"])

        ren_d = {}
        if d_id: ren_d[d_id] = "CANDIDATE_ID"
        if d_name: ren_d[d_name] = "MEMBER"
        if d_votes: ren_d[d_votes] = "VOTES"
        data_df = data_df.rename(columns=ren_d)

        if "VOTES" not in data_df.columns: data_df["VOTES"] = 0
        data_df["VOTES"] = pd.to_numeric(data_df["VOTES"], errors="coerce").fillna(0).astype(int)

        if "MEMBER" in data_df.columns:
            data_df["_NAME_KEY"] = data_df["MEMBER"].map(_norm_name)
        else:
            data_df["_NAME_KEY"] = ""

        group_cols = [col for col in ["CANDIDATE_ID", "MEMBER"] if col in data_df.columns]
        if not group_cols: return None

        votes_agg = data_df.groupby(group_cols, as_index=False)["VOTES"].sum()

        if "CANDIDATE_ID" in members_df.columns and "CANDIDATE_ID" in votes_agg.columns:
            merged = pd.merge(members_df, votes_agg[["CANDIDATE_ID", "VOTES"]], on="CANDIDATE_ID", how="left")
        else:
            if "_NAME_KEY" not in votes_agg.columns:
                votes_agg["_NAME_KEY"] = votes_agg["MEMBER"].map(_norm_name) if "MEMBER" in votes_agg.columns else ""
            merged = pd.merge(members_df, votes_agg[["_NAME_KEY", "VOTES"]], on="_NAME_KEY", how="left")

        merged["VOTES"] = pd.to_numeric(merged.get("VOTES", 0), errors="coerce").fillna(0).astype(int)
        df_final = merged[["MEMBER", "GROUP", "RELIGION", "DISTRICT", "VOTES"]].copy()

        winners = _compute_winners_from_quota(file_id, df_final)
        df_final["IS_WINNER"] = df_final["MEMBER"].astype(str).isin(winners)

        DATA_CACHE[file_id] = df_final
        return df_final

    except Exception as e:
        print(f"❌ System Error: {e}")
        return None

def _compute_winners_from_quota(file_id: str, df: pd.DataFrame) -> set:
    quota = QUOTA_CACHE.get(file_id, {})
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
    if file_id not in DATA_CACHE: return set()
    df = DATA_CACHE[file_id].copy()
    winners = _compute_winners_from_quota(file_id, df)
    DATA_CACHE[file_id]["IS_WINNER"] = DATA_CACHE[file_id]["MEMBER"].astype(str).isin(winners)
    return winners

# =========================================================
#  FAST ON-DEMAND SUGGESTION ENGINE
# =========================================================
def count_group_winners(df: pd.DataFrame, winners: set) -> dict:
    winner_df = df[df["MEMBER"].astype(str).isin(winners)].copy()
    if winner_df.empty: return {}
    return {str(k): int(v) for k, v in winner_df.groupby("GROUP")["MEMBER"].count().to_dict().items()}

def get_current_group_seats(file_id: str, df: pd.DataFrame) -> dict:
    return count_group_winners(df, _compute_winners_from_quota(file_id, df))

def build_atomic_swaps_for_group(df: pd.DataFrame, target_group: str):
    atomic = []
    list_cands = df[df["GROUP"] == target_group].sort_values("VOTES", ascending=True).head(8)
    other_cands = df[df["GROUP"] != target_group].sort_values("VOTES", ascending=False).head(25)

    for _, c_src in list_cands.iterrows():
        for _, c_tgt in other_cands.iterrows():
            if str(c_src["RELIGION"]) == str(c_tgt["RELIGION"]) and str(c_src["DISTRICT"]) == str(c_tgt["DISTRICT"]):
                net_gain = int(c_tgt["VOTES"]) - int(c_src["VOTES"])
                if net_gain > 0:
                    atomic.append({
                        "out": str(c_src["MEMBER"]), "in": str(c_tgt["MEMBER"]),
                        "from_list": str(c_tgt["GROUP"]), "net_gain": int(net_gain),
                        "religion": str(c_src["RELIGION"]), "district": str(c_src["DISTRICT"]),
                    })

    atomic = sorted(atomic, key=lambda x: x["net_gain"], reverse=True)
    seen, uniq = set(), []
    for a in atomic:
        if (a["out"], a["in"]) not in seen:
            seen.add((a["out"], a["in"]))
            uniq.append(a)
    return uniq[:12]

def is_valid_combo(combo):
    used_out, used_in = set(), set()
    for s in combo:
        if s["out"] in used_out or s["in"] in used_in: return False
        used_out.add(s["out"]); used_in.add(s["in"])
    return True

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
    return sim_df

def simulate_combo_gain(file_id: str, df: pd.DataFrame, target_group: str, combo, baseline_seats: int):
    sim_df = apply_swap_combo_to_copy(df, combo)
    if sim_df is None: return None
    seats_after_map = count_group_winners(sim_df, _compute_winners_from_quota(file_id, sim_df))
    seats_after = int(seats_after_map.get(target_group, 0))
    return {"seat_gain": int(seats_after - baseline_seats), "seats_after": seats_after}

def calculate_votes_needed_for_one_group(file_id, df, target_group, target_k):
    quota = QUOTA_CACHE.get(file_id, {})
    num_seats = sum(quota.get("rel_limits", {}).values())
    if num_seats == 0: return None

    cache_id = (file_id, target_group, str(target_k), get_df_version(df))
    if cache_id in SUGGESTION_CACHE: return SUGGESTION_CACHE[cache_id]

    df_group = df.groupby("GROUP", as_index=False)["VOTES"].sum()
    valid_electoral = df_group["VOTES"].sum()
    if valid_electoral == 0: return None

    eq1 = valid_electoral / num_seats
    df_group["USED"] = (df_group["VOTES"] >= eq1).astype(int)
    unused_votes = df_group.loc[df_group["USED"] == 0, "VOTES"].sum()
    adjusted_electoral = valid_electoral - unused_votes
    if adjusted_electoral <= 0: return None

    eq2 = adjusted_electoral / num_seats
    df_adj = df_group[df_group["USED"] == 1].copy()
    df_adj["QUOTIENT"] = df_adj["VOTES"] / eq2
    df_adj["INITIAL_SEATS"] = df_adj["QUOTIENT"].apply(lambda x: math.floor(round(x, 3)))
    df_adj["ADVANTAGE"] = df_adj["QUOTIENT"] - df_adj["INITIAL_SEATS"]
    seats_left = int(num_seats - df_adj["INITIAL_SEATS"].sum())
    df_adj = df_adj.sort_values(by="ADVANTAGE", ascending=False).reset_index(drop=True)

    df_adj["EXTRA_SEATS"] = 0
    for i in range(min(seats_left, len(df_adj))): df_adj.loc[i, "EXTRA_SEATS"] = 1
    group_seats_won = dict(zip(df_adj["GROUP"], df_adj["INITIAL_SEATS"] + df_adj["EXTRA_SEATS"]))

    row = df_group[df_group["GROUP"] == target_group]
    if row.empty: return None

    v, passed = row.iloc[0]["VOTES"], row.iloc[0]["USED"] == 1
    current_seats_actual = int(get_current_group_seats(file_id, df).get(target_group, 0))
    current_seats_formula = int(group_seats_won.get(target_group, 0)) if passed else 0

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
    needed = int(max(1, math.ceil(needed)))

    atomic_swaps = build_atomic_swaps_for_group(df, target_group)
    suggestions = []

    if k == 1:
        for s in atomic_swaps[:5]:
            sim = simulate_combo_gain(file_id, df, target_group, [s], current_seats_actual)
            if sim and sim["seat_gain"] >= 1:
                suggestions.append({"type": "1_swap", "seat_gain": sim["seat_gain"], "total_gain": s["net_gain"], "swaps": [s]})
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
        suggestions = sorted(suggestions, key=lambda x: (x["total_gain"], len(x["swaps"])))[:5]

    result = {"passed": bool(passed), "current_seats": current_seats_actual, "votes": needed, "suggestions": suggestions}
    SUGGESTION_CACHE[cache_id] = result
    return result

# =========================================================
#  ROUTES
# =========================================================
@app.route("/")
def index():
    global GDF_GLOBAL
    if GDF_GLOBAL is None: load_and_prepare_map()
    if GDF_GLOBAL is None or GDF_GLOBAL.empty: return "Map file not loaded. Put lebanonmap.xlsx in ./data/lebanonmap.xlsx", 500

    selected_year = request.args.get("year", "2022")
    unique_districts, seen_labels = [], set()

    for r in sorted(REGION_MAP.values(), key=lambda x: (x["district_label"], x["name"])):
        label = r["district_label"]
        if label == "Unknown" or "Beirut 3" in label or r["name"] == "Beirut-three": continue
        if selected_year in r["files"]:
            if label not in seen_labels:
                seen_labels.add(label)
                unique_districts.append({**r, "active_year": selected_year})

    return render_template(
        "index.html",
        map_json=create_interactive_map(GDF_GLOBAL["slug"].tolist(), selected_year),
        regions=unique_districts,
        current_year=selected_year,
    )

@app.route("/region/<slug>")
def region_detail(slug):
    if slug not in REGION_MAP: return abort(404)
    selected_year = request.args.get("year", "2022")
    info = REGION_MAP[slug]
    file_id = info["files"].get(selected_year)
    if not file_id: return "Data file not found."
    df = get_candidates_df(file_id)
    if df is None: return "Error loading Excel."

    total_seats = sum(QUOTA_CACHE.get(file_id, {}).get("rel_limits", {}).values())

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

@app.route("/api/reset_results", methods=["POST"])
def api_reset_results():
    file_id = (request.json or {}).get("filename")
    if file_id in DATA_CACHE: del DATA_CACHE[file_id]
    keys_to_del = [k for k in SUGGESTION_CACHE.keys() if k[0] == file_id]
    for k in keys_to_del: del SUGGESTION_CACHE[k]
    return jsonify({"success": True, "message": "Cache cleared."})

@app.route("/api/get_votes_needed", methods=["POST"])
def api_get_votes_needed():
    data = request.json or {}
    file_id, target_group, target_k = data.get("filename"), data.get("group_name"), str(data.get("target_k", "1"))
    if not file_id or file_id not in DATA_CACHE: return jsonify({"success": False}), 400
    return jsonify({"success": True, "result": calculate_votes_needed_for_one_group(file_id, DATA_CACHE[file_id], target_group, target_k)})

@app.route("/api/analyze_virtual_list", methods=["POST"])
def analyze_virtual_list():
    data = request.json or {}
    file_id, selected = data.get("filename"), data.get("candidates", [])
    df = DATA_CACHE.get(file_id)
    quota = QUOTA_CACHE.get(file_id, {})
    
    if not df or not quota: return jsonify({"success": False})

    total_votes = sum(int(c['votes']) for c in selected)
    num_seats = sum(quota.get("rel_limits", {}).values())
    quotient = df["VOTES"].sum() / num_seats if num_seats > 0 else 0
    
    return jsonify({
        "success": True, "total_votes": total_votes,
        "quotient": int(quotient), "reaches_quotient": total_votes >= quotient
    })

@app.route("/api/change_candidate_group", methods=["POST"])
def change_candidate_group():
    data = request.json or {}
    fid, src_name, new_group = data.get("filename"), data.get("candidate_name"), data.get("new_group")
    df = DATA_CACHE.get(fid)
    if df is None: return jsonify({"success": False, "message": "Data not found."}), 400

    mask = df["MEMBER"] == src_name
    if not mask.any(): return jsonify({"success": False, "message": f"Candidate {src_name} not found."}), 404

    old_group = df.loc[mask, "GROUP"].values[0]
    df.loc[mask, "GROUP"] = new_group
    clear_all_suggestion_cache()

    return jsonify({
        "success": True, "candidate": src_name, "old_group": old_group, "new_group": new_group,
        "old_total": int(df[df["GROUP"] == old_group]["VOTES"].sum()),
        "new_total": int(df[df["GROUP"] == new_group]["VOTES"].sum())
    })

@app.route("/api/swap_candidates", methods=["POST"])
def swap_candidates():
    # Left intact for multi-swap backwards compatibility if needed
    data = request.json or {}
    fid, src_name, target_name = data.get("filename"), data.get("candidate_name"), data.get("target_name")
    df = DATA_CACHE.get(fid)
    src_idx, target_idx = df.index[df["MEMBER"] == src_name][0], df.index[df["MEMBER"] == target_name][0]
    
    if df.at[src_idx, "RELIGION"] != df.at[target_idx, "RELIGION"] or df.at[src_idx, "DISTRICT"] != df.at[target_idx, "DISTRICT"]:
        return jsonify({"success": False, "message": "لا يمكن التبادل لاختلاف الطائفة أو الدائرة."}), 400

    sg, tg = df.at[src_idx, "GROUP"], df.at[target_idx, "GROUP"]
    df.loc[df["MEMBER"] == src_name, "GROUP"], df.loc[df["MEMBER"] == target_name, "GROUP"] = tg, sg
    clear_all_suggestion_cache()

    return jsonify({
        "success": True, "src_name": src_name, "target_name": target_name, "src_group": sg, "target_group": tg,
        "src_total": int(df[df["GROUP"] == sg]["VOTES"].sum()), "target_total": int(df[df["GROUP"] == tg]["VOTES"].sum()),
        "src_group_id": sg.replace(" ", "_").replace(".", ""), "target_group_id": tg.replace(" ", "_").replace(".", ""),
    })

@app.route("/api/multi_swap_candidates", methods=["POST"])
def multi_swap_candidates():
    data = request.json or {}
    file_id, swaps = data.get("filename"), data.get("swaps", [])
    df = DATA_CACHE.get(file_id)
    if not df is None and swaps:
        for s in swaps:
            s_rows, t_rows = df.index[df["MEMBER"] == s["out"]], df.index[df["MEMBER"] == s["in"]]
            sg, tg = df.loc[s_rows[0], "GROUP"], df.loc[t_rows[0], "GROUP"]
            df.loc[s_rows, "GROUP"], df.loc[t_rows, "GROUP"] = tg, sg
        clear_all_suggestion_cache()
        return jsonify({"success": True})
    return jsonify({"success": False}), 400

@app.route("/api/recompute_results", methods=["POST"])
def api_recompute_results():
    file_id = (request.json or {}).get("filename")
    winners = recompute_winners(file_id)
    return jsonify({"success": True, "winner_names": sorted(list(winners)), "winner_count": len(winners)})

@app.route("/api/find_winning_list", methods=["POST"])
def api_find_winning_list():
    data = request.json or {}
    fid = data.get("filename")
    candidate_name = data.get("candidate_name")
    df = DATA_CACHE.get(fid)
    
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
    df = DATA_CACHE.get(fid)
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

            # Perform the swap
            sg, tg = str(src_row["GROUP"]), str(tgt_row["GROUP"])
            df.loc[df["MEMBER"] == src_name, "GROUP"] = tg
            df.loc[df["MEMBER"] == tgt_name, "GROUP"] = sg
            clear_all_suggestion_cache()

            return jsonify({
                "reply": f"✅ تم التبادل بنجاح!<br><b>{src_name}</b> ← {tg}<br><b>{tgt_name}</b> ← {sg}<br><br><i>اضغط \"Recalculate\" لتحديث النتائج.</i>",
                "action": "swap_done"
            })

    # --- 2) QUOTIENT QUERY ---
    if "حاصل" in msg:
        num_seats = sum(QUOTA_CACHE.get(fid, {}).get("rel_limits", {}).values())
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
        num_seats = sum(QUOTA_CACHE.get(fid, {}).get("rel_limits", {}).values())
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

    # --- 6) CANDIDATE LOOKUP: "أصوات X" or "كم صوت X" ---
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
                num_seats = sum(QUOTA_CACHE.get(fid, {}).get("rel_limits", {}).values())
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