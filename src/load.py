import pandas as pd
import mysql.connector

def load_players(df: pd.DataFrame, conn) -> int:
    """
    Charge les joueurs dans la base de données MySQL de manière optimisée.
    """
    cursor = conn.cursor()
    
    # Préparation des données pour MySQL (conversion NaN -> None)
    data = []
    for _, row in df.iterrows():
        data.append((
            int(row['player_id']),
            str(row['username']),
            # Provide a placeholder email when missing to satisfy NOT NULL constraint
            row['email'] if pd.notna(row['email']) else f"unknown_{int(row['player_id'])}@example.local",
            row['registration_date'].strftime('%Y-%m-%d') if pd.notna(row['registration_date']) else '2000-01-01',
            row['country'] if pd.notna(row['country']) else None,
            int(row['level']) if pd.notna(row['level']) else 1
        ))

    query = """
    INSERT INTO Players (player_id, username, email, registration_date, country, level)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        username = VALUES(username),
        email = VALUES(email),
        registration_date = VALUES(registration_date),
        country = VALUES(country),
        level = VALUES(level)
    """
    
    try:
        cursor.executemany(query, data)
        conn.commit()
        print(f"✅ Chargé {len(data)} joueurs dans MySQL")
        return len(data)
    except mysql.connector.Error as err:
        print(f"❌ Erreur lors du chargement des joueurs : {err}")
        conn.rollback()
        return 0

def load_scores(df: pd.DataFrame, conn) -> int:
    """
    Charge les scores dans la base de données MySQL.
    """
    cursor = conn.cursor()
    
    # Verify which player_ids actually exist in the database to avoid FK errors
    cursor.execute("SELECT player_id FROM Players")
    existing_players = {int(r[0]) for r in cursor.fetchall()}

    data = []
    skipped = 0
    for _, row in df.iterrows():
        pid = int(row['player_id'])
        if pid not in existing_players:
            skipped += 1
            continue
        data.append((
            str(row['score_id']),
            pid,
            str(row['game']),
            int(row['score']),
            int(row['duration_minutes']) if pd.notna(row['duration_minutes']) else None,
            # Ensure played_at is not NULL to satisfy DB constraint; use a fallback timestamp
            row['played_at'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(row['played_at']) else '2000-01-01 00:00:00',
            row['platform'] if pd.notna(row['platform']) else None
        ))

    query = """
    INSERT INTO Scores (score_id, player_id, game, score, duration_minutes, played_at, platform)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        score = VALUES(score),
        duration_minutes = VALUES(duration_minutes),
        played_at = VALUES(played_at),
        platform = VALUES(platform)
    """
    
    try:
        cursor.executemany(query, data)
        conn.commit()
        print(f"✅ Chargé {len(data)} scores dans MySQL")
        if skipped:
            print(f"⚠️ {skipped} scores ignorés car le joueur n'existe pas en base")
        return len(data)
    except mysql.connector.Error as err:
        print(f"❌ Erreur lors du chargement des scores : {err}")
        conn.rollback()
        return 0