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

CSV_FILE = "Input_data/nfl_players.csv"
DATE_FILE = "Input_data/last_retrieval.txt"

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
    df_ordered = df[main_columns].copy()

    if not include_defenses:
        df_ordered = df_ordered[df_ordered["position"] != "DEF"]

    df_ordered["team_changed_at"] = pd.to_datetime(df_ordered["team_changed_at"], unit='s', errors='coerce')

    # Sort by full_name + most recent update and drop duplicates
    df_sorted = df_ordered.sort_values(["full_name", "team_changed_at"], ascending=[True, False])
    df_unique = df_sorted.drop_duplicates(subset="full_name", keep="first").copy()

    # Add cleaned name for merging (normalized)
    df_unique.loc[:, "full_name_clean"] = df_unique["full_name"].apply(strip_suffix).str.lower().str.strip()

    df_unique.to_csv("Input_data/nfl_players_ordered.csv", index=False)
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
# Add Information
# ===============================
def add_is_rookie_col(df):
    """
    Adds a boolean column 'is_rookie' to the dataframe.
    A player is considered a rookie if years_exp == 0.
    """
    if "years_exp" not in df.columns:
        raise ValueError("Column 'years_exp' not found in dataframe. Did you merge correctly?")
    
    df = df.copy()
    df["is_rookie"] = df["years_exp"].fillna(-1).astype(int) == 0
    return df

def add_is_lottery_ticket_col(df, lottery_ticket_names):
    """
    Adds a boolean column 'is_lottery_ticket' to the dataframe.
    A player is considered a lottery ticket if their full_name 
    appears in the provided list `lottery_ticket_names`.
    """
    if "full_name" not in df.columns:
        raise ValueError("Column 'full_name' not found in dataframe.")
    
    df = df.copy()
    # Normalize both df names and provided names for consistency
    normalized_names = [strip_suffix(name).lower().strip() for name in lottery_ticket_names]
    df["is_lottery_ticket"] = df["full_name_clean"].isin(normalized_names)
    return df

def add_handcuff_col(df, handcuff_pairs):
    """
    Adds a 'handcuff' column to the dataframe.

    handcuff_pairs: list of tuples
        Example: [("James Conner", "Trey Benson"), ("Bijan Robinson", "Tyler Allgeier")]

    - If a player is a starter in handcuff_pairs, the 'handcuff' column 
      will contain the backup's name.
    - All other players get NaN in the 'handcuff' column.
    """
    if "full_name" not in df.columns:
        raise ValueError("Column 'full_name' not found in dataframe.")

    df = df.copy()

    # Build a lookup map: starter_clean -> handcuff_name
    handcuff_map = {
        strip_suffix(starter).lower().strip(): handcuff
        for starter, handcuff in handcuff_pairs
    }

    # Assign only to starters
    df["handcuff"] = df["full_name_clean"].map(handcuff_map)

    return df

def add_fantasypros_sleeper_col(df, sleeper_list):
    """
    Adds a boolean column 'is_fantasypros_sleeper' to the dataframe.
    A player is considered a sleeper if their name appears in sleeper_list.
    """
    if "full_name_clean" not in df.columns:
        raise ValueError("Column 'full_name_clean' not found. Run clean_df() first.")

    df = df.copy()
    normalized_names = [strip_suffix(name).lower().strip() for name in sleeper_list]
    df["is_fantasypros_sleeper"] = df["full_name_clean"].isin(normalized_names)
    return df

def add_espn_rankings(df_1, df_2):
    df_merged = pd.merge(
            df_1,
            df_2,
            how="left",
            left_on="PLAYER NAME",
            right_on="player"
        )
    
    df_merged["RK_DIFF"] = df_merged["RK"] - df_merged["num"]

    return df_merged
    # Want to left join df_1[PLAYER NAME] with df[player]

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
    # print(f"Found {len(duplicates)} duplicate rows:")
    # print(duplicates[["full_name", "team", "position"]])

    # Load FantasyPros rankings and filter out NFL team defenses
    fantasy_pros_rankings_ppr = pd.read_csv("Input_data/FantasyPros_2025_Draft_ALL_Rankings.csv")
    fantasy_pros_rankings_ppr["PLAYER NAME_CLEAN"] = fantasy_pros_rankings_ppr["PLAYER NAME"].apply(strip_suffix).str.lower().str.strip()
    fantasy_pros_rankings_ppr = fantasy_pros_rankings_ppr[~fantasy_pros_rankings_ppr["PLAYER NAME"].isin(NFL_TEAMS)]

    # Load ESPN rankings
    espn_rankings = pd.read_csv("Input_data/ESPN players Order.csv")

    # Perform fuzzy merge and check unmatched
    lottery_list = [
        "Trevor Lawrence",
        "C.J. Stroud",
        "J.J. McCarthy",
        "Jacory Croskey-Merritt",
        "Rashid Shaheed",
        "Luther Burden III",
        "Cedric Tillman",
        "Jaydon Blue",
        "Marquise Brown",
        "DeMario Douglas",
        "Isaac Guerendo",
        "Chig Okonkwo",
        "Woody Marks",
        "Will Shipley",
        "Adonai Mitchell",
        "Dyami Brown",
        "Elijah Arroyo",
        "Isaac TeSlaa",
        "Kayshon Boutte",
        "Darren Waller",
        ]

    handcuffs = [
        ("James Conner", "Trey Benson"),
        ("Bijan Robinson", "Tyler Allgeier"),
        ("Derrick Henry", "Keaton Mitchell"),
        ("James Cook", "Ray Davis"),
        ("Chuba Hubbard", "Rico Dowdle"),
        ("D'Andre Swift", "Kyle Monangai"),
        ("Chase Brown", "Tahj Brooks"),
        ("Jerome Ford", "Dylan Sampson"),
        ("Javonte Williams", "Jaydon Blue"),
        ("J.K. Dobbins", "RJ Harvey"),
        ("Jahmyr Gibbs", "David Montgomery"),
        ("Josh Jacobs", "Chris Brooks"),
        ("Nick Chubb", "Dameon Pierce"),
        ("Jonathan Taylor", "DJ Giddens"),
        ("Travis Etienne Jr.", "Tank Bigsby"),
        ("Isiah Pacheco", "Kareem Hunt"),
        ("Omarion Hampton", "Najee Harris"),
        ("Kyren Williams", "Blake Corum"),
        ("Ashton Jeanty", "Zamir White"),
        ("De'Von Achane", "Ollie Gordon II"),
        ("Aaron Jones Sr.", "Jordan Mason"),
        ("TreVeyon Henderson", "Rhamondre Stevenson"),
        ("Alvin Kamara", "Kendre Miller"),
        ("Tyrone Tracy Jr.", "Cam Skattebo"),
        ("Breece Hall", "Braelon Allen"),
        ("Saquon Barkley", "Will Shipley"),
        ("Jaylen Warren", "Kaleb Johnson"),
        ("Kenneth Walker III", "Zach Charbonnet"),
        ("Christian McCaffrey", "Brian Robinson Jr."),
        ("Bucky Irving", "Rachaad White"),
        ("Tony Pollard", "Tyjae Spears"),
        ("Jacory Croskey-Merritt", "Austin Ekeler")
    ]
    
    sleepers = [
        "Jacory Croskey-Merritt",
        "Ollie Gordon II",
        "Dyami Brown",
        "Dont'e Thornton Jr.",
        "Tory Horton",
        "Sean Tucker",
        "Tyler Shough",
        "Isaiah Davis",
        "Shedeur Sanders",
        "Jaylin Lane",

    ]

    merged = join_to_get_ranked_order(fantasy_pros_rankings_ppr, df_cleaned)
    merged = add_is_rookie_col(merged)
    merged = add_is_lottery_ticket_col(merged, lottery_list)
    merged = add_handcuff_col(merged, handcuffs)
    merged = add_fantasypros_sleeper_col(merged, sleepers)
    merged = add_espn_rankings(merged, espn_rankings)
    merged.to_csv("output.csv", index=False)
    print("output.csv file created!!")
    print(list(merged.columns))
    join_check(fantasy_pros_rankings_ppr, df_cleaned)



