import logging
from src.database import database_connection
from src.extract import extract
from src.transform import transform_players, transform_scores
from src.load import load_players, load_scores
from src.report import generate_report

# Configuration du logging pour suivre l'exécution
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def run_pipeline():
    """Orchestre le pipeline ETL complet."""
    
    logging.info("Démarrage du pipeline ETL Gametracker...")

    try:
        # 1. Ouverture de la connexion via le context manager
        with database_connection() as conn:
            
            # --- PHASE JOUEURS ---
            logging.info("Phase 1 : Traitement des joueurs")
            df_players_raw = extract('data/Players.csv')
            df_players_clean = transform_players(df_players_raw)
            
            # On charge les joueurs en premier (contrainte FK)
            load_players(df_players_clean, conn)
            
            # On récupère les IDs valides pour filtrer les scores
            valid_player_ids = df_players_clean['player_id']

            # --- PHASE SCORES ---
            logging.info("Phase 2 : Traitement des scores")
            df_scores_raw = extract('data/Scores.csv')
            
            # Filtrage des scores avec les IDs de joueurs valides
            df_scores_clean = transform_scores(df_scores_raw, valid_player_ids)
            
            # Chargement des scores
            load_scores(df_scores_clean, conn)

        # --- PHASE RAPPORT ---
        logging.info("Phase 3 : Génération du rapport de synthèse")
        generate_report()

        logging.info("🚀 Pipeline exécuté avec succès du début à la fin !")

    except Exception as e:
        logging.error(f"Le pipeline a échoué : {e}")
        raise  # On relance l'erreur pour que le script Bash (run_pipeline.sh) puisse l'intercepter

if __name__ == "__main__":
    run_pipeline()