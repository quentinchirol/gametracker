import os
from datetime import datetime
from src.database import database_connection

def generate_report():
    """Interroge la base de données et génère le fichier output/rapport.txt"""
    
    # Création du dossier de sortie si absent
    if not os.path.exists('output'):
        os.makedirs('output')
        
    report_path = 'output/rapport.txt'

    with database_connection() as conn:
        cursor = conn.cursor(dictionary=True)

        # 1. Statistiques générales
        cursor.execute("SELECT COUNT(*) as nb_p FROM Players")
        nb_players = cursor.fetchone()['nb_p']
        
        cursor.execute("SELECT COUNT(*) as nb_s FROM Scores")
        nb_scores = cursor.fetchone()['nb_s']
        
        cursor.execute("SELECT COUNT(DISTINCT game) as nb_g FROM Scores")
        nb_games = cursor.fetchone()['nb_g']

        # 2. Top 5 des meilleurs scores (avec JOIN pour le pseudo)
        query_top5 = """
            SELECT p.username, s.game, s.score 
            FROM Scores s
            JOIN Players p ON s.player_id = p.player_id
            ORDER BY s.score DESC
            LIMIT 5
        """
        cursor.execute(query_top5)
        top_5 = cursor.fetchall()

        # 3. Score moyen par jeu
        cursor.execute("SELECT game, AVG(score) as average FROM Scores GROUP BY game")
        avg_scores = cursor.fetchall()

        # 4. Répartition des joueurs par pays
        cursor.execute("SELECT country, COUNT(*) as total FROM Players GROUP BY country ORDER BY total DESC")
        countries = cursor.fetchall()

        # 5. Répartition des sessions par plateforme
        cursor.execute("SELECT platform, COUNT(*) as total FROM Scores GROUP BY platform ORDER BY total DESC")
        platforms = cursor.fetchall()

    # Écriture du fichier rapport.txt
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("=" * 52 + "\n")
        f.write("GAMETRACKER - Rapport de synthese\n")
        f.write(f"Genere le : {datetime.now().strftime('%Y -%m -%d %H:%M:%S')}\n")
        f.write("=" * 52 + "\n\n")

        f.write("--- Statistiques generales ---\n")
        f.write(f"Nombre de joueurs : {nb_players}\n")
        f.write(f"Nombre de scores : {nb_scores}\n")
        f.write(f"Nombre de jeux : {nb_games}\n\n")

        f.write("--- Top 5 des meilleurs scores ---\n")
        for i, row in enumerate(top_5, 1):
            f.write(f"{i}. {row['username']} | {row['game']} | {row['score']}\n")
        f.write("\n")

        f.write("--- Score moyen par jeu ---\n")
        for row in avg_scores:
            f.write(f"{row['game']} : {row['average']:.1f}\n")
        f.write("\n")

        f.write("--- Joueurs par pays ---\n")
        for row in countries:
            # Gère le cas où country est NULL
            pays = row['country'] if row['country'] else "Inconnu"
            f.write(f"{pays} : {row['total']}\n")
        f.write("\n")

        f.write("--- Sessions par plateforme ---\n")
        for row in platforms:
            plateforme = row['platform'] if row['platform'] else "Non spécifiée"
            f.write(f"{plateforme} : {row['total']}\n")
        
        f.write("\n" + "BUT Science des Données Automatisation et Tests" + "\n")
        f.write("=" * 52 + "\n")

    print(f"✅ Rapport généré avec succès dans : {report_path}")

if __name__ == "__main__":
    generate_report()