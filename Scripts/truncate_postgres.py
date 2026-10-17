import os
import psycopg2

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "nyc-postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "database": os.environ.get("POSTGRES_DB", "nyc_taxi"),
    "user":     os.environ.get("POSTGRES_USER", "nyc_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", "nyc_password"),
}


def truncate_table():
    print("="*60)
    print("TRUNCATING POSTGRESQL TABLE")
    print("=" * 60)

    conn=psycopg2.connect(**DB_CONFIG)
    cursor=conn.cursor()

    cursor.execute("TRUNCATE TABLE taxi_trips;")

    conn.commit()

    cursor.close()
    conn.close()

    print("taxi_strips table truncated successfully")


if __name__=="__main__":
    truncate_table()