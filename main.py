# ===============================
# Imports
# ===============================
from rapidfuzz import process, fuzz
import os
from dotenv import load_dotenv
import requests
import pandas as pd
from datetime import datetime
import re

# ===============================
# Constants
# ===============================
load_dotenv()
print("Loaded SLEEPER_USERNAME:", os.getenv("SLEEPER_USERNAME"))

CSV_FILE = "nfl_players.csv"
DATE_FILE = "last_retrieval.txt"

NFL_TEAMS = [
    "Arizona Cardinals","Atlanta Falcons","Baltimore Ravens","Buffalo Bills","Carolina Panthers",
    "Chicago Bears","Cincinnati Bengals","Cleveland Browns","Dallas Cowboys","Denver Broncos",
    "Detroit Lions","Green Bay Packers","Houston Texans","Indianapolis Colts","Jacksonville Jaguars",
    "Kansas City Chiefs","Las Vegas Raiders","Los Angeles Chargers","Los Angeles Rams","Miami Dolphins",
    "Minnesota Vikings","New England Patriots","New Orleans Saints","New York Giants","New York Jets",
    "Philadelphia Eagles","Pittsburgh Steelers","San Francisco 49ers","Seattle Seahawks","Tampa Bay Buccaneers",
    "Tennessee Titans","Washington Commanders"
]

# ===============================
# Utility Functions
# ===============================
def strip_suffix(name):
    """Remove Jr., Sr., II, III from player names for consistent matching"""
    if pd.isna(name):
        return ""
    return re.sub(r'\s+(Jr\.|Sr\.|II|III)$', '', name).strip()

def fuzzy_match_names(name, choices, limit=1, score_cutoff=80):
    """Return best fuzzy match from a list of choices"""
    results = process.extract(name, choices, scorer=fuzz.token_sort_ratio, limit=limit, score_cutoff=score_cutoff)
    return results[0][0] if results else None

# ===============================
# Data Retrieval
# ===============================
def get_account_information():
    username = os.getenv("SLEEPER_USERNAME")
    if not username:
        raise Exception("SLEEPER_USERNAME not set in .env")
    
    url = f"https://api.sleeper.app/v1/user/{username}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Account loaded: {data.get('display_name')} ({data.get('user_id')})")
        return data
    else:
        raise Exception(f"Error retrieving account: {response.status_code}")

def get_updated_player_data():
    """Fetch NFL player data from Sleeper API, caching daily to CSV"""
    if os.path.exists(CSV_FILE) and os.path.exists(DATE_FILE):
        with open(DATE_FILE, "r") as f:
            last_date = f.read().strip()
        today = datetime.today().strftime("%Y-%m-%d")
        if last_date == today:
            print("📂 Loading player data from saved CSV...")
            return pd.read_csv(CSV_FILE)

    print("🌐 Fetching player data from Sleeper API...")
    url = "https://api.sleeper.app/v1/players/nfl"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API request failed: {response.status_code}")
    
    players = response.json()
    player_list = []
    for player_id, info in players.items():
        info['player_id'] = player_id
        player_list.append(info)
    
    df = pd.DataFrame(player_list)
    df.to_csv(CSV_FILE, index=False)

    with open(DATE_FILE, "w") as f:
        f.write(datetime.today().strftime("%Y-%m-%d"))

    return df

# ===============================
# Data Cleaning
# ===============================
def clean_df(df, include_defenses=True):
    """Reorder, deduplicate, clean, and optionally filter defenses"""
    main_columns = [
        "player_id", "full_name", "first_name", "last_name", "position", 
        "team", "team_abbr", "fantasy_positions", "active", "status",
        "age", "height", "weight", "college", "years_exp",
        "injury_status", "injury_notes", "injury_body_part",
        "practice_description", "practice_participation",
        "birth_city", "birth_state", "birth_country", "birth_date",
        "team_changed_at"
    ]

    final_columns = main_columns + [col for col in df.columns if col not in main_columns]
    df_ordered = df[final_columns].copy()

    if not include_defenses:
        df_ordered = df_ordered[df_ordered["position"] != "DEF"]

    df_ordered["team_changed_at"] = pd.to_datetime(df_ordered["team_changed_at"], unit='s', errors='coerce')

    # Sort by full_name + most recent update and drop duplicates
    df_sorted = df_ordered.sort_values(["full_name", "team_changed_at"], ascending=[True, False])
    df_unique = df_sorted.drop_duplicates(subset="full_name", keep="first")

    # Add cleaned name for merging (normalized)
    df_unique["full_name_clean"] = df_unique["full_name"].apply(strip_suffix).str.lower().str.strip()

    df_unique.to_csv("nfl_players_ordered.csv", index=False)
    print("💾 Saved nfl_players_ordered.csv (deduplicated & cleaned)")

    return df_unique

# ===============================
# Merging / Matching
# ===============================
def join_to_get_ranked_order(df_left, df_right):
    # Clean and normalize names
    df_left["PLAYER NAME_CLEAN"] = df_left["PLAYER NAME"].apply(strip_suffix).str.lower().str.strip()
    df_right_names = df_right["full_name_clean"].tolist()

    matched_names = [fuzzy_match_names(name, df_right_names) for name in df_left["PLAYER NAME_CLEAN"]]
    df_left["Matched Name"] = matched_names

    df_merged = pd.merge(
        df_left,
        df_right,
        how="left",
        left_on="Matched Name",
        right_on="full_name_clean"
    )

    total_rows = len(df_left)
    matched_rows = df_left["Matched Name"].notna().sum()
    success_rate = matched_rows / total_rows * 100
    print(f"🎯 Matched {matched_rows}/{total_rows} rows ({success_rate:.2f}% success rate)")

    return df_merged

def join_check(df_left, df_right, use_fuzzy=True):
    """
    Checks for unmatched names after merging.

    If use_fuzzy=True, it relies on the 'Matched Name' column
    produced by fuzzy matching. Otherwise, it does a direct merge.
    """
    if use_fuzzy:
        if "Matched Name" not in df_left.columns:
            raise ValueError("Column 'Matched Name' not found. Run join_to_get_ranked_order first.")
        
        unmatched_left = df_left[df_left["Matched Name"].isna()]
        if len(unmatched_left) == 0:
            print("🎉 All names matched (fuzzy match).")
        else:
            print("Unmatched names (fuzzy match):", unmatched_left["PLAYER NAME"].tolist())
    else:
        # Direct merge check
        df_left["PLAYER NAME_CLEAN"] = df_left["PLAYER NAME"].apply(strip_suffix)
        df_merged = pd.merge(
            df_left,
            df_right,
            how="left",
            left_on="PLAYER NAME_CLEAN",
            right_on="full_name_clean",
            indicator=True
        )
        unmatched_left = df_merged[df_merged["_merge"] == "left_only"]
        if len(unmatched_left) == 0:
            print("🎉 All names matched (exact merge).")
        else:
            print("Unmatched names (exact merge):", unmatched_left["PLAYER NAME"].tolist())


# ===============================
# Main Execution
# ===============================
if __name__ == "__main__":
    # Load account and player data
    get_account_information()
    df_sleeper_data = get_updated_player_data()
    df_cleaned = clean_df(df_sleeper_data, include_defenses=False)

    # Check duplicates
    duplicates = df_cleaned[df_cleaned.duplicated(subset="full_name", keep=False)]
    print(f"Found {len(duplicates)} duplicate rows:")
    print(duplicates[["full_name", "team", "position"]])

    # Load FantasyPros rankings and filter out NFL team defenses
    fantasy_pros_rankings_ppr = pd.read_csv("FantasyPros_2025_Draft_ALL_Rankings.csv")
    fantasy_pros_rankings_ppr["PLAYER NAME_CLEAN"] = fantasy_pros_rankings_ppr["PLAYER NAME"].apply(strip_suffix).str.lower().str.strip()
    fantasy_pros_rankings_ppr = fantasy_pros_rankings_ppr[~fantasy_pros_rankings_ppr["PLAYER NAME"].isin(NFL_TEAMS)]

    # Perform fuzzy merge and check unmatched
    merged = join_to_get_ranked_order(fantasy_pros_rankings_ppr, df_cleaned)
    join_check(fantasy_pros_rankings_ppr, df_cleaned)
