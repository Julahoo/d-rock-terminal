# Telemarketing Operations & Financial Dashboard

## Overview
This is a 3-Pillar internal agency application (Operations Command, Campaign Execution, Audience Health) designed to track omnichannel telemarketing efficiency, SLA volumes, and 15% rev-share margins.

## Tech Stack
* **Frontend:** Streamlit
* **Data Engine:** Pandas
* **Database:** PostgreSQL / SQLAlchemy
* **Data Viz:** Plotly
* **Hosting:** Railway.app

## Development Methodology
### Spec-Driven Development (SDD)
All architectural rules and project specifications live in `SPEC.md`. All feature implementations, bug fixes, and system changes must be strictly logged in `DEVLOG.md` before execution to maintain a single source of truth.

## Local Setup Instructions
1. **Clone the repository:**
   ```bash
   git clone <repository_url>
   cd <repository_directory>
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the environment:**
   Create a `.env` file in the root directory and add your PostgreSQL connection string:
   ```env
   DATABASE_URL=postgresql://user:password@localhost:5432/dbname
   ```

4. **Launch the application:**
   ```bash
   streamlit run app.py
   ```
