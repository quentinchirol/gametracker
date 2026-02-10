-- Création de la table Players
CREATE TABLE IF NOT EXISTS Players (
    player_id INT PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(200) NULL, 
    registration_date DATE NOT NULL,
    country VARCHAR(100),
    level INT DEFAULT 1
);

-- Création de la table Scores
CREATE TABLE IF NOT EXISTS Scores (
    score_id VARCHAR(20) PRIMARY KEY,
    player_id INT NOT NULL,
    game VARCHAR(100) NOT NULL,
    score INT NOT NULL,
    duration_minutes INT,
    played_at DATETIME NOT NULL,
    platform VARCHAR(50),
    CONSTRAINT fk_player
        FOREIGN KEY (player_id) 
        REFERENCES Players(player_id)
        ON DELETE CASCADE
);
