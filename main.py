from fileinput import filename
import json
import pandas as pd
import time
import os
import nba_api
from nba_api.stats.static import players
from nba_api.stats.endpoints import leaguedashplayerstats
def fetch_nba_player_stats(path="data"):
    seasons = [f"{year}-{str(year+1)[-2:]}" for year in range(2000, 2025)]
    all_rows = []
    for season in seasons:
        try: 
            print(f"Fetching data for season {season}")
            response = leaguedashplayerstats.LeagueDashPlayerStats(
                season=season,
                per_mode_detailed="PerGame",
                timeout = 100
            )
        except TimeoutError:
            print(f"TimeoutError for season {season}, retrying...")
            time.sleep(5)
            continue
        result = response.get_dict()["resultSets"][0]
        headers = result["headers"]
        rows = result["rowSet"]

        for row in rows:
            record = dict(zip(headers, row))
            record["SEASON"] = season
            all_rows.append(record)
        time.sleep(1)  # To respect API rate limits

    # Convert everything to one DataFrame
    df = pd.DataFrame(all_rows)
    df = standardize_player_names(df)
    df.to_csv(os.path.join(path, "nba_player_stats_2000_2025.csv"), index=False)
    df.to_json(os.path.join(path, "nba_player_stats_2000_2025.json"), orient="records")

def detect_anomalies(df, path = "data"):
    # Example anomaly detection: Check for missing values in key columns
    key_columns = []
    key_columns.extend(col for col in df.columns if "RANK" not in col and "WNBA_FANTASY_PTS" not in col and "TEAM_COUNT" not in col)
    anomalies = df[key_columns].isnull().sum()
    print(f"Anomalies detected (missing values):{anomalies}")
    #Detect missing seasons for individual players
    grouped = df.groupby("PLAYER_NAME")["SEASON"].apply(set)
    missing_seasons_map = {}

    for player_name, seasons in grouped.items():
        start, end = min(seasons), max(seasons)
        start = int(start.split("-")[0]) + 1
        end = int(end.split("-")[0]) + 1

        missing_seasons = set(range(start, end + 1)) - {
            int(s.split("-")[0]) + 1 for s in seasons
        }

        missing_seasons_to_str = sorted(
            f"{year-1}-{str(year)[-2:]}" for year in missing_seasons
        )

        missing_seasons_map[player_name] = missing_seasons_to_str
    grouped_df = grouped.reset_index()
    grouped_df.columns = ["PLAYER_NAME", "SEASONS"]
    grouped_df["SEASONS"] = grouped_df["SEASONS"].apply(lambda x: sorted(x))
    grouped_df["MISSING_SEASONS"] = grouped_df["PLAYER_NAME"].map(missing_seasons_map)
    grouped_df.to_csv(os.path.join(path, "nba_player_missing_seasons.csv"), index=False)
    print("Missing seasons per player saved to nba_player_missing_seasons.csv")
def standardize_player_names(df):
    # Remove special characters from player names
    # Example: Luka Dončić -> Luka Doncic
    df["PLAYER_NAME"] = df["PLAYER_NAME"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    return df
def all_nba_players(path="data"):
    player_dict = players.get_players() #properties: id, full_name, first_name, last_name, is_active
    #Only pull from players that played from 2000-2025
    filtered_player_dict = []
    filename = os.path.join(path, "all_nba_players.json")
    try:
        with open(filename, 'w') as json_file:
            json.dump(player_dict, json_file, indent=4)
        print(f"Successfully wrote data to {filename}")
    except IOError as e:
        print(f"Error writing to file {filename}: {e}")
def load_players_from_json(filename):
    with open(filename, 'r') as f:
        return json.load(f)
def popper(df, target_col, target_name, target_location):
        target_col = df.pop(target_name)
        df.insert(target_location, target_name, target_col)
        return df
def fetch_current_nba_players(path="data"):
    df = pd.read_json(os.path.join(path, "nba_player_stats_2000_2025.json"))
    modern_players = df["PLAYER_NAME"].unique().tolist()
    all_players = load_players_from_json(os.path.join(path, "all_nba_players.json"))
    modern_player_dict = [p for p in all_players if p["full_name"] in modern_players]
    with open(os.path.join(path, "modern_nba_players.json"), 'w') as f:
        json.dump(modern_player_dict, f, indent=4)
def per_36(file, path = "data"):
    df = pd.read_json(file)
    select_columns = ["PTS", "AST", "REB", "STL", "BLK", "TOV", 'FGM', 'FGA', 'FG_PCT', 'FG3M',
       'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'PF', 'BLKA']
    for col in select_columns:
        if df["MIN"] != 0:
            df[col] = (df[col] / df["MIN"]) * 36 
    df.to_json(os.path.join(os.path.dirname(file), "nba_player_stats_2000_2025_per_36.json"), orient="records")
if __name__ == "__main__":
    folder = "data"
    path = os.path.join(os.getcwd(), folder)
    if not os.path.exists(path):
        os.makedirs(path)
    fetch_nba_player_stats()
    df = pd.read_csv(os.path.join(path, "nba_player_stats_2000_2025.csv"))
    #per_36(os.path.join(path, "nba_player_stats_2000_2025.json"), path=path)
    detect_anomalies(df, path=path)
    all_nba_players(path=path)
    fetch_current_nba_players(path=path)
    #per_36(os.path.join(path, "nba_player_stats_2000_2025.json"), path=path)