import pandas as pd
import mysql.connector

def load_players(df: pd.DataFrame, conn) -> int:
    """
    Charge les joueurs dans MySQL en utilisant executemany pour la performance.
    """
    cursor = conn.cursor()
    
    # Préparation des données : conversion des types Pandas vers types Python natifs
    # On gère les dates et on remplace les NaN par None (que MySQL comprend comme NULL)
    data_to_load = []
    for _, row in df.iterrows():
        data_to_load.append((
            int(row['player_id']),
            str(row['username']),
            row['email'] if pd.notna(row['email']) else None,
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
        cursor.executemany(query, data_to_load)
        conn.commit()
        print(f"✅ Succès : {cursor.rowcount} lignes traitées dans Players")
        return len(df)
    except mysql.connector.Error as err:
        print(f"❌ Erreur lors du chargement des joueurs : {err}")
        conn.rollback()
        return 0

def load_scores(df: pd.DataFrame, conn) -> int:
    """
    Charge les scores dans MySQL.
    """
    cursor = conn.cursor()
    
    data_to_load = []
    for _, row in df.iterrows():
        data_to_load.append((
            str(row['score_id']),
            int(row['player_id']),
            str(row['game']),
            int(row['score']),
            int(row['duration_minutes']) if pd.notna(row['duration_minutes']) else None,
            row['played_at'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(row['played_at']) else None,
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
        cursor.executemany(query, data_to_load)
        conn.commit()
        print(f"✅ Succès : {cursor.rowcount} lignes traitées dans Scores")
        return len(df)
    except mysql.connector.Error as err:
        print(f"❌ Erreur lors du chargement des scores : {err}")
        conn.rollback()
        return 0
