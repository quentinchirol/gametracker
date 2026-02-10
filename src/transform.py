"""
Module ETL : Transform
"""

import pandas as pd

# ============== TRANSFORM ==============

def transform_players(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme et nettoie les données des joueurs.
    """
    df = df.copy()

    # 1. Supprimer les doublons sur player_id
    df = df.drop_duplicates(subset=['player_id'])

    # 2. Nettoyer les espaces des username (strip)
    df['username'] = df['username'].str.strip()

    # 3. Convertir les dates d’inscription
    df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')

    # 4. Remplacer les emails invalides (sans @) par None
    df['email'] = df['email'].where(df['email'].str.contains('@', na=False), None)

    print(f"✅ Joueurs transformés : {len(df)}")
    return df


def transform_scores(df: pd.DataFrame, valid_player_ids: pd.Series) -> pd.DataFrame:
    """
    Transforme les scores et élimine les références orphelines.
    
    Args:
        df: DataFrame brut des scores.
        valid_player_ids: Liste (Series) des player_id valides issus de transform_players.
    """
    df = df.copy()

    # 1. Supprimer les doublons sur score_id
    df = df.drop_duplicates(subset=['score_id'])

    # 2. Convertir les dates et les scores en types numériques
    df['played_at'] = pd.to_datetime(df['played_at'], errors='coerce')
    df['score'] = pd.to_numeric(df['score'], errors='coerce')

    # 3. Supprimer les lignes avec un score négatif ou nul
    df = df[df['score'] > 0]

    # 4. Supprimer les scores dont le player_id n’est pas dans valid_player_ids
    # C'est ici que l'on gère les références orphelines
    df = df[df['player_id'].isin(valid_player_ids)]

    print(f"✅ Scores transformés : {len(df)}")
    return df 