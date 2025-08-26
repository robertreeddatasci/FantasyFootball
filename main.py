from rapidfuzz import process, fuzz
import os
from dotenv import load_dotenv
import requests
import pandas as pd
import os
from datetime import datetime

load_dotenv()
CSV_FILE = "nfl_players.csv"
DATE_FILE = "last_retrieval.txt"

def get_account_information():
    # Get the username from .env
    username = os.getenv("SLEEPER_USERNAME")
    url = f"https://api.sleeper.app/v1/user/{username}"

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        print(data)
    else:
        print(f"Error: {response.status_code}")
    
    return



def get_updated_player_data():
    # Check if we have a saved CSV and last retrieval date
    if os.path.exists(CSV_FILE) and os.path.exists(DATE_FILE):
        with open(DATE_FILE, "r") as f:
            last_date = f.read().strip()
        today = datetime.today().strftime("%Y-%m-%d")
        
        if last_date == today:
            print("Loading player data from saved CSV...")
            return pd.read_csv(CSV_FILE)

    # Otherwise, call the API
    print("Fetching player data from Sleeper API...")
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

    # Save the CSV
    df.to_csv(CSV_FILE, index=False)

    # Update the last retrieval date
    today = datetime.today().strftime("%Y-%m-%d")
    with open(DATE_FILE, "w") as f:
        f.write(today)

    return df

def clean_df(df):
    # Main columns in the order you want
    main_columns = [
        "player_id", "full_name", "first_name", "last_name", "position", 
        "team", "team_abbr", "fantasy_positions", "active", "status",
        "age", "height", "weight", "college", "years_exp",
        "injury_status", "injury_notes", "injury_body_part",
        "practice_description", "practice_participation",
        "birth_city", "birth_state", "birth_country", "birth_date"
    ]

    # Add any remaining columns that exist in the DataFrame but aren't in main_columns
    final_columns = main_columns + [col for col in df.columns if col not in main_columns]

    # Reorder DataFrame
    df_ordered = df[final_columns]

    # Save CSV
    df_ordered.to_csv("nfl_players_ordered.csv", index=False)
    print("Saved nfl_players_ordered.csv!")
    return df_ordered


if __name__ == "__main__":
    df = get_updated_player_data()
    df_cleaned = clean_df(df)
    print(df_cleaned.head())




#Steps
#1. Get players data from Sleeper API
#2. Get ranks from FantasyPros CSV download
#3. Add tags to rookies, lottery tickets or handcuffs. 
#4. implement the simulator for draft day. Use rapidfuzz for easy player name entry (ADD FUNCTIONALITY TO REMOVE PLAYERS, OR ADD THEM BACK IF MISTAKE)