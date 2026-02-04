import json
import sqlite3
from pathlib import Path

# ---------- paths ----------
JSON_PATH = Path("data/nba_player_stats_2000_2025.json")
DB_PATH = Path("data/nba_stats.db")


# ---------- helpers ----------
def safe_float(x):
    """Convert to float; return None if missing/invalid (becomes NULL in SQLite)."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def compute_metrics(r):
    """
    Compute:
      eFG% = (FGM + 0.5*FG3M) / FGA
      TS%  = PTS / (2*(FGA + 0.44*FTA))
      per-36 for PTS/REB/AST = stat * 36 / MIN
    """
    min_ = safe_float(r.get("MIN"))
    pts = safe_float(r.get("PTS"))
    reb = safe_float(r.get("REB"))
    ast = safe_float(r.get("AST"))

    fgm = safe_float(r.get("FGM"))
    fga = safe_float(r.get("FGA"))
    fg3m = safe_float(r.get("FG3M"))
    fta = safe_float(r.get("FTA"))

    # eFG%
    efg = None
    if fga is not None and fga > 0 and fgm is not None and fg3m is not None:
        efg = (fgm + 0.5 * fg3m) / fga

    # TS%
    ts = None
    if pts is not None and fga is not None and fta is not None:
        denom = 2 * (fga + 0.44 * fta)
        if denom > 0:
            ts = pts / denom

    # per-36
    pts_36 = reb_36 = ast_36 = None
    if min_ is not None and min_ > 0:
        factor = 36.0 / min_
        if pts is not None:
            pts_36 = pts * factor
        if reb is not None:
            reb_36 = reb * factor
        if ast is not None:
            ast_36 = ast * factor

    return ts, efg, pts_36, reb_36, ast_36


# ---------- main ----------
def main():
    if not JSON_PATH.exists():
        raise FileNotFoundError(f"Missing input JSON: {JSON_PATH}")

    rows = json.loads(JSON_PATH.read_text())
    if not rows:
        raise ValueError("JSON file is empty.")

    # --- schema validation (prevents silent bugs) ---
    required_keys = {
        "PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION", "SEASON",
        "AGE", "GP", "MIN", "W", "L", "W_PCT",
        "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
        "OREB", "DREB", "REB", "AST", "TOV", "STL", "BLK", "PF", "PTS", "PLUS_MINUS",
    }
    missing = required_keys - set(rows[0].keys())
    if missing:
        raise KeyError(f"JSON missing required keys: {sorted(missing)}")

    # recreate db file
    if DB_PATH.exists():
        DB_PATH.unlink()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # speed + durability defaults for local analytics
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    # create table
    cur.execute("""
    CREATE TABLE player_season_stats (
        player_id INTEGER NOT NULL,
        player_name TEXT NOT NULL,
        team_id INTEGER,
        team_abbreviation TEXT,
        season TEXT NOT NULL,
        age REAL,

        gp INTEGER,
        w INTEGER,
        l INTEGER,
        w_pct REAL,
        min REAL,

        fgm REAL, fga REAL,
        fg3m REAL, fg3a REAL,
        ftm REAL, fta REAL,

        oreb REAL, dreb REAL, reb REAL,
        ast REAL, tov REAL,
        stl REAL, blk REAL, pf REAL,
        pts REAL,
        plus_minus REAL,

        ts_pct REAL,
        efg_pct REAL,
        pts_per36 REAL,
        reb_per36 REAL,
        ast_per36 REAL,
        
        PRIMARY KEY (player_id, season)
    );
    """)
#add stl & blk to per_36 calculations later
    insert_sql = """
    INSERT INTO player_season_stats (
        player_id, player_name, team_id, team_abbreviation, season, age,
        gp, w, l, w_pct, min,
        fgm, fga, fg3m, fg3a, ftm, fta,
        oreb, dreb, reb, ast, tov, stl, blk, pf, pts, plus_minus,
        ts_pct, efg_pct, pts_per36, reb_per36, ast_per36
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    """

    # insert rows
    for r in rows:
        ts, efg, p36, r36, a36 = compute_metrics(r)

        values = [
            r["PLAYER_ID"],
            r["PLAYER_NAME"],
            r["TEAM_ID"],
            r["TEAM_ABBREVIATION"],
            r["SEASON"],
            safe_float(r.get("AGE")),

            r.get("GP"),
            r.get("W"),
            r.get("L"),
            safe_float(r.get("W_PCT")),
            safe_float(r.get("MIN")),

            safe_float(r.get("FGM")), safe_float(r.get("FGA")),
            safe_float(r.get("FG3M")), safe_float(r.get("FG3A")),
            safe_float(r.get("FTM")), safe_float(r.get("FTA")),

            safe_float(r.get("OREB")), safe_float(r.get("DREB")), safe_float(r.get("REB")),
            safe_float(r.get("AST")), safe_float(r.get("TOV")),
            safe_float(r.get("STL")), safe_float(r.get("BLK")), safe_float(r.get("PF")),
            safe_float(r.get("PTS")),
            safe_float(r.get("PLUS_MINUS")),

            ts, efg, p36, r36, a36
        ]

        cur.execute(insert_sql, values)

    con.commit()

    # indexes (makes Step 5 retrieval faster)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_player_name ON player_season_stats(player_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_season ON player_season_stats(season);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_team_abbrev ON player_season_stats(team_abbreviation);")
    con.commit()

    # sanity checks
    n = cur.execute("SELECT COUNT(*) FROM player_season_stats;").fetchone()[0]
    print(f"✅ DB created: {DB_PATH}")
    print(f"✅ Rows inserted: {n}")

    # show a few rows as proof it worked
    sample = cur.execute("""
        SELECT player_name, season, team_abbreviation, pts, ts_pct, efg_pct
        FROM player_season_stats
        WHERE ts_pct IS NOT NULL
        ORDER BY ts_pct DESC
        LIMIT 5;
    """).fetchall()

    print("\nTop 5 TS% seasons (sample):")
    for row in sample:
        print(row)

    con.close()


if __name__ == "__main__":
    main()
