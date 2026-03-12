import pandas as pd
from src.database import engine

def main():
    query = "SELECT column_name FROM information_schema.columns WHERE table_name='ops_telemarketing_snapshots'"
    df = pd.read_sql(query, engine)
    print("Columns in ops_telemarketing_snapshots:")
    print(df['column_name'].tolist())

if __name__ == "__main__":
    main()
