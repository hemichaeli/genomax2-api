# GenoMAX2 Local Setup

## Your Folder Structure

```
C:\Projects\GenoMAX2\PostgreSQL\
├── genomax_engine.py       <- The brain (recommendation logic)
├── api_server.py           <- Web API endpoints
├── load_data.py            <- Database loader
├── 1_install.bat           <- Step 1: Install Python packages
├── 2_load_data.bat         <- Step 2: Load data into PostgreSQL
├── 3_start_server.bat      <- Step 3: Start the API server
├── GENOMAX_FINAL_140.csv
├── GENOMAX_OS_Engine_Data.csv
└── Supliful_GenoMAX_catalog.csv
```

## First Time Setup

### Step 1: Install Python
Download from https://www.python.org/downloads/
CHECK "Add Python to PATH" during installation!

### Step 2: Create Database
1. Open pgAdmin
2. Right-click Databases > Create > Database
3. Name: genomax2
4. Click Save

### Step 3: Run the Setup
1. Double-click `1_install.bat` (installs Python packages)
2. Double-click `2_load_data.bat` (loads CSV data into database)
3. Double-click `3_start_server.bat` (starts the API)

### Step 4: Test It
Open your browser: http://localhost:8000/docs

## API Endpoints

| URL | What it does |
|-----|--------------|
| http://localhost:8000/ | Health check |
| http://localhost:8000/docs | Swagger UI (interactive) |
| http://localhost:8000/goals | List all goals |
| http://localhost:8000/recommend | Get recommendations (POST) |
| http://localhost:8000/ingredient/Ashwagandha | Ingredient details |
| http://localhost:8000/products | All Supliful products |

## Example API Call (PowerShell)

```powershell
$body = @{
    gender = "male"
    goals = @("Sleep Optimization", "Stress & Mood")
    medications = @()
    conditions = @()
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8000/recommend" -Method Post -ContentType "application/json" -Body $body
```

## Database Connection

- Host: localhost
- Port: 5432
- Database: genomax2
- User: postgres
- Password: 1!Qaz2wsx

Connection string:
```
postgresql://postgres:1!Qaz2wsx@localhost:5432/genomax2
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| "Python not found" | Reinstall Python, check "Add to PATH" |
| "psycopg2 error" | Run: pip install psycopg2-binary |
| "Connection refused" | Make sure PostgreSQL is running |
| "Database not found" | Create 'genomax2' database in pgAdmin |
