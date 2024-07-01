import os
import requests
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def fetch_sports_data(api_key):
    url = f"https://www.thesportsdb.com/api/v2/json/{api_key}/livescore/all"
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if "all" in data:
        df = pd.DataFrame(data["all"])
        return df

def insert_into_postgres(df):
    try:
        conn = psycopg2.connect(
            host=os.getenv("HOST"),
            database=os.getenv("DB"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        cursor = conn.cursor()

        # Check existing idEvent values in the database
        cursor.execute("SELECT idEvent FROM student.ha_livescores")
        existing_events = set(row[0] for row in cursor.fetchall())

        # Insert or update DataFrame rows into PostgreSQL table
        df_columns = list(df.columns)
        columns = ",".join(df_columns)
        
        for i, row in df.iterrows():
            if row['idEvent'] in existing_events:
                # Update existing row
                update_sql = f"""
                    UPDATE student.ha_livescores
                    SET idLiveScore = %s, intHomeScore = %s, intAwayScore = %s, intEventScore = %s,
                        intEventScoreTotal = %s, strStatus = %s, strProgress = %s,
                        strEventTime = %s, dateEvent = %s, updated = current_timestamp
                    WHERE idEvent = %s
                """
                update_data = (
                    row['idLiveScore'], row['intHomeScore'], row['intAwayScore'], row['intEventScore'],
                    row['intEventScoreTotal'], row['strStatus'], row['strProgress'],
                    row['strEventTime'], row['dateEvent'], row['idEvent']
                )
                cursor.execute(update_sql, update_data)
            else:
                # Insert new row
                insert_sql = f"INSERT INTO student.ha_livescores ({columns}) VALUES ({', '.join(['%s']*len(df_columns))})"
                cursor.execute(insert_sql, tuple(row))
                existing_events.add(row['idEvent'])

            conn.commit()

    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    api_key = os.getenv("API_KEY")
    
    df = fetch_sports_data(api_key)
    
    if df is not None:
        insert_into_postgres(df)

if __name__ == "__main__":
    main()