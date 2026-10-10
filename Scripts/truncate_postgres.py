import psycopg2

DB_CONFIG={
       "host": "nyc-postgres",
    "port": 5432,
    "database": "nyc_taxi",
    "user": "nyc_user",
    "password": "nyc_password", 
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