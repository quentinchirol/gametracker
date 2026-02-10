#!/bin/bash

# Arrête le script à la première erreur rencontrée
set -e

echo "===================================================="
echo "🚀 DÉMARRAGE DU PIPELINE GAMETRACKER"
echo "===================================================="

# 1. Attente de la base de données
echo "⏳ [1/4] Attente de la base de données..."
./scripts/wait_for_db.sh

# 2. Initialisation des tables
echo "📂 [2/4] Initialisation des tables SQL..."
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" --skip-ssl < scripts/init-db.sql

# 3. Exécution du pipeline ETL (Python)
echo "⚙️ [3/4] Lancement de l'ETL (Extract, Transform, Load)..."
# On utilise une commande python pour appeler les fonctions dans l'ordre
python3 -c "
from src.database import database_connection
from src.extract import extract
from src.transform import transform_players, transform_scores
from src.load import load_players, load_scores

with database_connection() as conn:
    print('  -> Traitement des joueurs...')
    df_p = transform_players(extract('./data/Players.csv'))
    load_players(df_p, conn)
    
    print('  -> Traitement des scores...')
    df_s = transform_scores(extract('./data/Scores.csv'), df_p['player_id'])
    load_scores(df_s, conn)
"

# 4. Génération du rapport
echo "📊 [4/4] Génération du rapport final..."
python3 -c "from src.report import generate_report; generate_report()"

echo "===================================================="
echo "✅ PIPELINE TERMINÉ AVEC SUCCÈS !"
echo "===================================================="