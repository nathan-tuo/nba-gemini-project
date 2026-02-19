import json
import sqlite3
import csv
import ast
from pathlib import Path

# ---------- paths ----------
STATS_JSON_PATH = Path("data/nba_player_stats_2000_2025.json")
MISSING_CSV_PATH = Path("data/nba_player_missing_seasons.csv")
DRAFT_JSON_PATH = Path("data/draft_history.json")
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
    ast_ = safe_float(r.get("AST"))

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
        if ast_ is not None:
            ast_36 = ast_ * factor

    return ts, efg, pts_36, reb_36, ast_36


def parse_listish_cell(cell):
    """
    Your CSV columns look like: "['2011-12']" or "[]"
    Use ast.literal_eval safely.
    Return python list.
    """
    if cell is None:
        return []
    cell = str(cell).strip()
    if cell == "":
        return []
    try:
        val = ast.literal_eval(cell)
        return val if isinstance(val, list) else []
    except Exception:
        return []


# ---------- loaders ----------
def create_player_season_stats(cur):
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


def load_player_season_stats(cur):
    if not STATS_JSON_PATH.exists():
        raise FileNotFoundError(f"Missing input JSON: {STATS_JSON_PATH}")

    rows = json.loads(STATS_JSON_PATH.read_text())
    if not rows:
        raise ValueError("Stats JSON file is empty.")

    required_keys = {
        "PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION", "SEASON",
        "AGE", "GP", "MIN", "W", "L", "W_PCT",
        "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
        "OREB", "DREB", "REB", "AST", "TOV", "STL", "BLK", "PF", "PTS", "PLUS_MINUS",
    }
    missing = required_keys - set(rows[0].keys())
    if missing:
        raise KeyError(f"Stats JSON missing required keys: {sorted(missing)}")

    create_player_season_stats(cur)

    insert_sql = """
    INSERT INTO player_season_stats (
        player_id, player_name, team_id, team_abbreviation, season, age,
        gp, w, l, w_pct, min,
        fgm, fga, fg3m, fg3a, ftm, fta,
        oreb, dreb, reb, ast, tov, stl, blk, pf, pts, plus_minus,
        ts_pct, efg_pct, pts_per36, reb_per36, ast_per36
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    """

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

    # indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pss_player_name ON player_season_stats(player_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pss_season ON player_season_stats(season);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pss_team_abbrev ON player_season_stats(team_abbreviation);")

    n = cur.execute("SELECT COUNT(*) FROM player_season_stats;").fetchone()[0]
    print(f"✅ Rows inserted (player_season_stats): {n}")


def rebuild_players_from_stats(cur):
    """
    IMPORTANT:
    players table is derived from player_season_stats,
    so joins by player_id will always work.
    """
    cur.execute("DROP TABLE IF EXISTS players;")
    cur.execute("""
    CREATE TABLE players AS
    SELECT DISTINCT
        player_id,
        player_name AS full_name
    FROM player_season_stats;
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_players_player_id ON players(player_id);")

    n = cur.execute("SELECT COUNT(*) FROM players;").fetchone()[0]
    print(f"✅ Rows inserted (players from stats): {n}")


def load_player_missing_seasons(cur):
    if not MISSING_CSV_PATH.exists():
        print(f"⚠️ Missing CSV (skip): {MISSING_CSV_PATH}")
        return 0

    cur.execute("""
    CREATE TABLE player_missing_seasons (
        player_name TEXT NOT NULL,
        seasons_json TEXT NOT NULL,
        missing_seasons_json TEXT NOT NULL
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pms_player_name ON player_missing_seasons(player_name);")

    inserted = 0
    with MISSING_CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        expected = {"PLAYER_NAME", "SEASONS", "MISSING_SEASONS"}
        if not expected.issubset(set(reader.fieldnames or [])):
            raise KeyError(f"Missing CSV columns. Need {expected}, got {reader.fieldnames}")

        for row in reader:
            player_name = (row.get("PLAYER_NAME") or "").strip()
            seasons_list = parse_listish_cell(row.get("SEASONS"))
            missing_list = parse_listish_cell(row.get("MISSING_SEASONS"))

            cur.execute(
                "INSERT INTO player_missing_seasons (player_name, seasons_json, missing_seasons_json) VALUES (?,?,?);",
                (player_name, json.dumps(seasons_list), json.dumps(missing_list))
            )
            inserted += 1

    print(f"✅ Rows inserted (player_missing_seasons): {inserted}")
    return inserted


def load_draft_history(cur):
    if not DRAFT_JSON_PATH.exists():
        print(f"⚠️ Missing JSON (skip): {DRAFT_JSON_PATH}")
        return 0

    rows = json.loads(DRAFT_JSON_PATH.read_text())
    if not rows:
        print("⚠️ Draft JSON empty (skip).")
        return 0

    # minimal schema based on your screenshot
    cur.execute("""
    CREATE TABLE draft_history (
        person_id INTEGER,
        player_name TEXT,
        season TEXT,
        round_number INTEGER,
        round_pick INTEGER,
        overall_pick INTEGER,
        draft_type TEXT,
        team_id INTEGER,
        team_city TEXT,
        team_name TEXT,
        team_abbreviation TEXT,
        organization TEXT,
        organization_type TEXT,
        player_profile_flag INTEGER
    );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_draft_player_name ON draft_history(player_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_draft_season ON draft_history(season);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_draft_overall_pick ON draft_history(overall_pick);")

    insert_sql = """
    INSERT INTO draft_history (
        person_id, player_name, season, round_number, round_pick, overall_pick, draft_type,
        team_id, team_city, team_name, team_abbreviation,
        organization, organization_type, player_profile_flag
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    """

    inserted = 0
    for r in rows:
        cur.execute(insert_sql, (
            r.get("PERSON_ID"),
            r.get("PLAYER_NAME"),
            r.get("SEASON"),
            r.get("ROUND_NUMBER"),
            r.get("ROUND_PICK"),
            r.get("OVERALL_PICK"),
            r.get("DRAFT_TYPE"),
            r.get("TEAM_ID"),
            r.get("TEAM_CITY"),
            r.get("TEAM_NAME"),
            r.get("TEAM_ABBREVIATION"),
            r.get("ORGANIZATION"),
            r.get("ORGANIZATION_TYPE"),
            r.get("PLAYER_PROFILE_FLAG"),
        ))
        inserted += 1

    print(f"✅ Rows inserted (draft_history): {inserted}")
    return inserted


# ---------- main ----------
def main():
    # recreate db
    if DB_PATH.exists():
        DB_PATH.unlink()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # speed + durability defaults for local analytics
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    print(f"✅ DB created: {DB_PATH}")

    # 1) core stats table
    load_player_season_stats(cur)

    # 2) derive players from stats
    rebuild_players_from_stats(cur)

    # 3) missing seasons table (csv -> json text in db)
    load_player_missing_seasons(cur)

    # 4) draft history table
    load_draft_history(cur)

    con.commit()

    # ---- sanity samples ----
    sample_ts = cur.execute("""
        SELECT player_name, season, team_abbreviation, pts, ts_pct, efg_pct
        FROM player_season_stats
        WHERE ts_pct IS NOT NULL
        ORDER BY ts_pct DESC
        LIMIT 5;
    """).fetchall()

    print("\nTop 5 TS% seasons (sample):")
    for row in sample_ts:
        print(row)

    sample_missing = cur.execute("""
        SELECT player_name, missing_seasons_json
        FROM player_missing_seasons
        WHERE missing_seasons_json != '[]'
        LIMIT 5;
    """).fetchall()

    print("\nSample missing seasons:")
    for row in sample_missing:
        print(row)

    sample_draft = cur.execute("""
        SELECT season, overall_pick, player_name, team_abbreviation, organization
        FROM draft_history
        WHERE overall_pick IS NOT NULL
        ORDER BY CAST(season AS INTEGER) DESC, overall_pick ASC
        LIMIT 5;
    """).fetchall()

    print("\nSample draft picks:")
    for row in sample_draft:
        print(row)

    con.close()


if __name__ == "__main__":
    main()
