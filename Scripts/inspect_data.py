import pandas as pd

FILE_PATH = "yellow_tripdata_2026-01.parquet"


def inspect_data():
    df = pd.read_parquet(FILE_PATH)

    print("=" * 50)
    print("DATASET INFORMATION")
    print("=" * 50)

    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print("\nColumns:")
    for column in df.columns:
        print(f"  - {column}")

    print("\nData Types:")
    print(df.dtypes)

    print("\nFirst 5 Records:")
    print(df.head().to_string())

    print("\nMissing Values:")
    print(df.isnull().sum())


if __name__ == "__main__":
    inspect_data()