# Big Data Processing – FH Kufstein Tirol (DSIA, 3rd semester) Winter Semester 2026/2027

Hands-on material for the course. Everything runs in VS Code inside a **dev container**,
so every student has the identical environment (Python 3.12, Java 17, pandas, Polars,
DuckDB, PySpark, Delta Lake).

## Option A – GitHub Codespaces (recommended, nothing to install)
1. Sign in to github.com (a free account is enough).
2. On this repository: **Fork → Create fork**. Your own copy keeps your work.
3. On **your fork**: **Code → Codespaces → Create codespace on main**.
4. Wait ~3 minutes for the first build. VS Code opens in your browser.
   (Optional: open it in your local VS Code via the "Codespaces" extension.)
5. In the terminal: `python get_data.py --months 6`

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
| 2 | Big Data 2026 – do you really need a cluster? Part II | `session-01/`Update |
| 3 | Volume I – Spark fundamentals | *coming* |
| 4 | Volume II – Spark performance | *coming* |
| 5 | Variety & Veracity – Lakehouse | *coming* |
| 6 | Velocity – Stream processing with Kafka | *coming* |
| 7 | Big Data for AI & production | *coming* |