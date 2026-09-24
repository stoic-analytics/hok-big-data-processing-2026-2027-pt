# Big Data Processing – FH Kufstein Tirol (DSIA, 3rd semester)

Hands-on material for the course. Everything runs in VS Code inside a **dev container**,
so every student has the identical environment (Python 3.12, Java 17, pandas, Polars,
DuckDB, PySpark, Delta Lake).

## Option A – GitHub Codespaces (recommended, nothing to install)
1. Sign in to github.com (a free account is enough).
2. On this repository: **Code → Codespaces → Create codespace on main**.
3. Wait ~3 minutes for the first build. VS Code opens in your browser.
   (Optional: open it in your local VS Code via the "Codespaces" extension.)
4. In the terminal: `python get_data.py --months 6`

💡 Stop your codespace when you are done (github.com/codespaces → ⋯ → Stop),
otherwise it keeps using your free monthly hours.
Students can get more free hours via the GitHub Student Developer Pack (education.github.com).

## Option B – Local (Docker Desktop required)
1. Install Docker Desktop and the VS Code extension **Dev Containers**.
2. Clone this repo, open it in VS Code → "Reopen in Container".
3. In the terminal: `python get_data.py --months 6`

## Sessions
| # | Topic | Folder |
|---|---|---|
| 1 | Big Data 2026 – do you really need a cluster? | `session-01/` |
| 2 | Volume I – Spark fundamentals | *coming* |
| 3 | Volume II – Spark performance | *coming* |
| 4 | Variety & Veracity – Lakehouse | *coming* |
| 5 | Velocity – Stream processing with Kafka | *coming* |
| 6 | Big Data for AI & production | *coming* |
