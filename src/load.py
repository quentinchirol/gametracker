"""
Module ETL : Load
"""

import pandas as pd

# ============== LOAD ==============

def load_players(df: pd.DataFrame, conn) -> int:
    """
    Charge les joueurs dans la base de données MySQL.
    """
    cursor = conn.cursor()
    
    # Requête avec ON DUPLICATE KEY UPDATE pour mettre à jour les infos si player_id existe
    query = """
    INSERT INTO Players 
        (player_id, username, email, registration_date, country, level)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        username = VALUES(username),
        email = VALUES(email),
        registration_date = VALUES(registration_date),
        country = VALUES(country),
        level = VALUES(level)
    """
    
    count = 0
    for _, row in df.iterrows():
        # Conversion sécurisée des types et gestion des NaN/NaT
        values = (
            int(row['player_id']),
            row['username'],
            row['email'] if pd.notna(row['email']) else None,
            row['registration_date'].strftime('%Y-%m-%d') if pd.notna(row['registration_date']) else None,
            row['country'] if pd.notna(row['country']) else None,
            int(row['level']) if pd.notna(row['level']) else 1
        )
        cursor.execute(query, values)
        count += 1
    
    conn.commit()
    print(f"✅ Chargé {count} joueurs dans MySQL")
    return count


def load_scores(df: pd.DataFrame, conn) -> int:
    """
    Charge les scores dans la base de données MySQL.
    """
    cursor = conn.cursor()
    
    query = """
    INSERT INTO Scores 
        (score_id, player_id, game, score, duration_minutes, played_at, platform)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        score = VALUES(score),
        duration_minutes = VALUES(duration_minutes),
        played_at = VALUES(played_at),
        platform = VALUES(platform)
    """
    
    count = 0
    for _, row in df.iterrows():
        # Conversion et gestion des dates/nombres
        values = (
            row['score_id'],
            int(row['player_id']),
            row['game'],
            int(row['score']),
            int(row['duration_minutes']) if pd.notna(row['duration_minutes']) else None,
            row['played_at'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(row['played_at']) else None,
            row['platform'] if pd.notna(row['platform']) else None
        )
        cursor.execute(query, values)
        count += 1
        
    conn.commit()
    print(f"✅ Chargé {count} scores dans MySQL")
    return count 