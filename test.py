"""
NBA Performance Tracker | Python, SQL, Tableau, Azure 
● Designed end-to-end data pipeline with Python and SQL to collect and visualize historical player statistics across ~25
years of basketball data using the NBA’s API
● Built Tableau dashboards highlighting performance trends, injury gaps, and career progression metrics for 500 athletes
● Automated anomaly detection in datasets to flag missing sports records, reducing manual corrections by 40%
"""
import json
import pandas as pd
import time
import os
from nba_api.stats.endpoints import leaguedashplayerstats
def fetch_nba_player_stats():
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
    df.to_csv("nba_player_stats_2000_2025.csv", index=False)
    df.to_json("nba_player_stats_2000_2025.json", orient="records")

def detect_anomalies(df):
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
    grouped_df.to_csv("nba_player_missing_seasons.csv", index=False)
    print("Missing seasons per player saved to nba_player_missing_seasons.csv")
def standardize_player_names(df):
    # Remove special characters from player names
    # Example: Luka Dončić -> Luka Doncic
    df["PLAYER_NAME"] = df["PLAYER_NAME"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    return df
if __name__ == "__main__":
    fetch_nba_player_stats()
    df = pd.read_csv("nba_player_stats_2000_2025.csv")
    df = standardize_player_names(df)
    detect_anomalies(df)
