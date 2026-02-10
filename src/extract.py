"""
Module ETL : Extract
"""

from pathlib import Path
import pandas as pd

# ============== EXTRACT ==============

def extract(filepath: str) -> pd.DataFrame:
    """
    Extrait les données d'un fichier CSV.

    Args:
        filepath: Chemin vers le fichier CSV.

    Returns:
        pd.DataFrame: DataFrame contenant les données.

    Raises:
        FileNotFoundError: Si le fichier n'existe pas.
    """
    path = Path(filepath)
    
    if not path.exists():
        raise FileNotFoundError(f"Fichier non trouvé : {filepath}")
    
    df = pd.read_csv(filepath)
    print(f"Extrait {len(df)} lignes de {path.name}")
    
    return df 