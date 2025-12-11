# NASA Near-Earth Objects ETL Pipeline

A production-ready Python ETL pipeline that extracts real-time asteroid tracking data from NASA's NeoWs API, transforms complex nested JSON into normalized tabular format, and loads into a SQLite data warehouse. Processes 100+ asteroid close approach events per week with comprehensive error handling and idempotent data loading.

## 🚀 Key Features

- **Real-time Data Processing**: Integrates with NASA's official NeoWs API to fetch live asteroid tracking data
- **Robust ETL Architecture**: Modular design with separate Extract, Transform, and Load stages
- **Data Validation**: Comprehensive error handling with retry logic and exponential backoff
- **Idempotent Loading**: Safe reprocessing of date ranges without data duplication
- **Flexible Execution**: Demo mode with sample data and live mode with real API calls
- **Production Ready**: CLI interface, detailed console output, and integration testing

## 📊 Data Capabilities

- **Volume**: Processes 138+ asteroid events per 7-day period
- **Scope**: Tracks potentially hazardous asteroids (PHAs) with detailed orbital characteristics
- **Precision**: Asteroid diameters from 1.2m to 1+ km, miss distances from 100k to 74+ million km
- **Real-world Impact**: Identifies and tracks potentially hazardous objects approaching Earth

## 🏗️ Architecture

```
data-pipeline-nasa-neows/
├── src/
│   ├── pipeline.py      # CLI orchestrator and main entry point
│   ├── fetch.py         # NASA API integration with retry logic
│   ├── transform.py     # JSON-to-DataFrame transformation
│   ├── load.py          # SQLite warehouse loading
│   ├── config.py        # Configuration management
│   └── utils/           # Date validation and environment helpers
├── data/
│   ├── processed/       # CSV outputs for analysis
│   └── warehouse/       # SQLite database storage
├── tests/               # Integration and unit tests
├── sample_data/         # Demo data for offline development
└── requirements.txt     # Python dependencies
```

## 🛠️ Installation & Setup

1. **Clone and setup environment:**
   ```bash
   git clone https://github.com/bendmiller1/data-pipeline-nasa-neows.git
   cd data-pipeline-nasa-neows
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Optional: Set up NASA API key** (uses DEMO_KEY by default):
   ```bash
   echo "NASA_API_KEY=your_api_key_here" > .env
   ```

## 🚀 Usage

### Quick Start (Demo Mode)
```bash
# Run with sample data
python -m src.pipeline --mode feed --start 2025-10-01 --end 2025-10-07 --demo
```

### Live Data Processing
```bash
# Fetch real-time data from NASA API
python -m src.pipeline --mode feed --start 2025-10-07 --end 2025-10-13 --live
```

### CLI Options
- `--mode {feed,browse,migrate}`: Pipeline execution mode (feed mode implemented, browse mode planned, migrate handles database migrations)
- `--start YYYY-MM-DD`: Start date for data collection (required for feed mode)
- `--end YYYY-MM-DD`: End date for data collection (required for feed mode)
- `--target VERSION`: Target migration version (for migrate mode, optional)
- `--dry-run`: Preview migration changes without executing (for migrate mode)
- `--rollback`: Rollback migrations to target version (for migrate mode)
- `--demo`: Force demo mode (use local sample data)
- `--live`: Force live mode (use NASA API)

### Database Migrations
The pipeline includes a comprehensive migration system for schema management:
```bash
# Apply all pending migrations
python -m src.pipeline --mode migrate

# Preview migration changes
python -m src.pipeline --mode migrate --dry-run

# Rollback to a specific version
python -m src.pipeline --mode migrate --rollback --target 001
```

Migrations run automatically during feed mode to ensure database schema is up-to-date.

## 📈 Sample Output

**Console Output:**
```
[pipeline] Running feed ETL for [2025-10-07 to 2025-10-13] (DEMO_MODE=0)
[LIVE_MODE] GET https://api.nasa.gov/neo/rest/v1/feed params={...}
[transform] CSV saved to: data/processed/neows_latest.csv
[pipeline] CSV output written to: data/processed/neows_latest.csv
[load] Pre-delete: removed 0 rows in [2025-10-07 .. 2025-10-13]
[pipeline] Loaded 138 rows into SQLite database at: data/warehouse/neows_data.db
[pipeline] Feed ETL completed successfully. Ad Astra!
```

**Data Sample:**
| id | name | close_approach_date | diameter_min_km | diameter_max_km | is_potentially_hazardous | relative_velocity_kps | miss_distance_km | orbiting_body |
|----|------|-------------------|-----------------|-----------------|-------------------------|---------------------|------------------|---------------|
| 3559911 | (2011 ET74) | 2025-10-07 | 0.0153 | 0.0342 | False | 18.08 | 65301756.59 | Earth |
| 54550580 | (2025 TT1) | 2025-10-07 | 0.0369 | 0.0825 | False | 14.76 | 10084891.39 | Earth |

## 🔧 Technical Stack

- **Python 3.x**: Core development language
- **Pandas**: Data manipulation and transformation
- **Requests**: HTTP client for API integration
- **SQLite**: Lightweight data warehouse
- **Pytest**: Comprehensive testing framework
- **NASA NeoWs API**: Official NASA Near-Earth Object data source

## 🧪 Testing

Run the comprehensive test suite:
```bash
# Run all tests
python -m pytest tests/ -v

# Run integration tests specifically
python -m pytest tests/test_pipeline_integration.py -v
```

## 📊 Database Schema

The SQLite warehouse uses an optimized schema with composite primary keys:

```sql
CREATE TABLE neows (
    id TEXT,
    name TEXT,
    close_approach_date TEXT,
    absolute_magnitude_h REAL,
    diameter_min_km REAL,
    diameter_max_km REAL,
    is_potentially_hazardous BOOLEAN,
    relative_velocity_kps REAL,
    miss_distance_km REAL,
    orbiting_body TEXT,
    PRIMARY KEY (close_approach_date, id)
);
```

## 🔄 Data Pipeline Flow

1. **Extract**: Fetch JSON data from NASA NeoWs API with retry logic
2. **Transform**: Flatten nested JSON structure into normalized DataFrame
3. **Load**: Insert into SQLite with idempotent date-range replacement
4. **Output**: Generate CSV for analysis and maintain data warehouse

## 🛡️ Error Handling

- **HTTP Timeouts**: Exponential backoff retry logic
- **Rate Limiting**: Automatic retry with progressive delays
- **Data Validation**: JSON structure and required field verification
- **Idempotent Operations**: Safe reprocessing without data corruption

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 🙏 Acknowledgments

- NASA's Near-Earth Object Web Service (NeoWs) for providing comprehensive asteroid data
- The open-source Python community for excellent libraries and tools

---

**Contact**: [LinkedIn Profile](https://linkedin.com/in/bdmillerprofessional) | **Portfolio**: [GitHub Profile](https://github.com/bendmiller1)

*This project demonstrates production-ready ETL pipeline development, API integration, data engineering best practices, and comprehensive testing methodologies.*