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

if __name__ == "__main__":
    df = get_updated_player_data()
    print(len(df.columns))








#Steps
#1. Get players data from Sleeper API
#2. Get ranks from FantasyPros CSV download
#3. Add tags to rookies, lottery tickets or handcuffs. 
#4. implement the simulator for draft day. Use rapidfuzz for easy player name entry (ADD FUNCTIONALITY TO REMOVE PLAYERS, OR ADD THEM BACK IF MISTAKE)