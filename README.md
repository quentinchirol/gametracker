# GameTracker ETL Pipeline

GameTracker is a data engineering mini-project designed to ingest, transform, and load gaming data (Players and Scores) into a MySQL database for analysis. It includes a complete ETL (Extract, Transform, Load) pipeline and an automated reporting module.

## 📂 Project Structure

```
├── data/               # Source CSV files (Players.csv, Scores.csv)
├── output/             # Generated reports (rapport.txt)
├── scripts/            # Helper scripts (DB wait, pipeline runner)
├── src/                # Python source code
│   ├── extract.py      # Data extraction
│   ├── transform.py    # Data cleaning and transformation
│   ├── load.py         # Database loading
│   ├── database.py     # Database connection management
│   ├── report.py       # Report generation
│   └── main.py         # Pipeline entry point
├── docker-compose.yml  # Docker services definition
├── Dockerfile          # Python application container
└── requirements.txt    # Python dependencies
```

## 🚀 Prerequisites

- **Docker** and **Docker Compose** installed on your machine.
- No local Python database setup is required (handled by Docker).

## 🛠️ Installation & Setup

1.  **Clone the repository** (if applicable) or navigate to the project folder.

2.  **Start the services** using Docker Compose:
    ```bash
    docker-compose up --build
    ```
    This command will:
    - Build the Python application image.
    - Start a MySQL database container (`etl_mysql`).
    - Start the application container (`etl_app`).

3.  **Wait for initialization**:
    The system is configured to wait for the database to be ready. You will see logs indicating "Attente de la base de données...".

## ▶️ Usage

### Evaluation
The corrector will clone the repository and run the following commands:

1.  **Start the environment**:
    ```bash
    docker compose up -d --build
    ```

2.  **Execute the pipeline**:
    ```bash
    docker compose exec app ./scripts/run_pipeline.sh
    ```

### Check Results
- **Reports**: The generated report is available at `output/rapport.txt`.
- **Database**: Connect to `localhost:3306` (User: `etl_user`, Pass: `etl_pass`, DB: `etl_db`).

## ⚙️ Key Features

- **Robust Database Connection**: Implements retry logic to handle container startup timing.
- **Data Cleaning**:
    - Deduplication of players and scores.
    - Email validation.
    - Orphan score removal (integrity checks).
- **Automated Reporting**: Generates a text summary of key statistics (Top players, Average scores, etc.).
