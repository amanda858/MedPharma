import json
import os
import sqlite3


def inspect_db(path: str) -> dict:
    out = {"exists": os.path.exists(path)}
    if not out["exists"]:
        return out

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        out["kinder_clients"] = [
            dict(r)
            for r in cur.execute(
                """
                SELECT id, username, company, role, is_active
                FROM clients
                WHERE lower(company) LIKE '%kinder%'
                   OR lower(username) LIKE '%kinder%'
                ORDER BY id
                """
            ).fetchall()
        ]
        out["prod_recent"] = [
            dict(r)
            for r in cur.execute(
                """
                SELECT id, client_id, work_date, username, category
                FROM team_production
                ORDER BY id DESC
                LIMIT 30
                """
            ).fetchall()
        ]
        out["prod_by_client"] = [
            dict(r)
            for r in cur.execute(
                """
                SELECT client_id, COUNT(*) AS cnt, MIN(work_date) AS min_date, MAX(work_date) AS max_date
                FROM team_production
                GROUP BY client_id
                ORDER BY cnt DESC
                LIMIT 30
                """
            ).fetchall()
        ]
        out["clients"] = [
            dict(r)
            for r in cur.execute(
                """
                SELECT id, username, company, role, is_active
                FROM clients
                ORDER BY id
                """
            ).fetchall()
        ]
    except Exception as exc:
        out["query_error"] = str(exc)
    finally:
        conn.close()
    return out


paths = []
for candidate in [os.getenv("DB_PATH", ""), "/data/leads.db", "data/leads.db"]:
    if candidate and candidate not in paths:
        paths.append(candidate)

report = {p: inspect_db(p) for p in paths}
print(json.dumps(report, ensure_ascii=True))
