# Data Pipeline – NASA NeoWs

An ETL (Extract, Transform, Load) pipeline that fetches Near-Earth Object (asteroid) data from NASA’s NeoWs API, transforms it into a clean tabular format, and stores the results as both CSV and SQLite files.

## Project Structure
```
data-pipeline-nasa-neows/
├── data/
│ ├── processed/ # CSVs or temporary outputs
│ └── warehouse/ # SQLite database storage
├── sample_data/ # Cached demo JSONs for offline testing
├── src/
│ └── utils/ # Helper functions
├── requirements.txt # Python dependencies
├── .gitignore
├── LICENSE
└── README.md
```

## Setup
1. Clone the repository:
   ```bash
   git clone git@github.com:bendmiller1/data-pipeline-nasa-neows.git
   cd data-pipeline-nasa-neows
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt