# db.py
import os
import pyodbc
from dotenv import load_dotenv

# Load .env file
load_dotenv()

def get_connection():
    conn = pyodbc.connect(
        f"Driver={{{os.getenv('SQL_DRIVER')}}};"
        f"Server={os.getenv('SQL_SERVER')};"
        f"Database={os.getenv('SQL_DATABASE')};"
        f"Uid={os.getenv('SQL_USER')};"
        f"Pwd={os.getenv('SQL_PASSWORD')};"
        f"Encrypt={os.getenv('SQL_ENCRYPT')};"
        f"TrustServerCertificate={os.getenv('SQL_TRUST_CERT')};"
        f"Connection Timeout={os.getenv('SQL_TIMEOUT')};"
    )
    return conn


#Testing connection

#def test_connection():
#   try:
#       conn = get_connection()
#        cursor = conn.cursor()
#        cursor.execute("SELECT 1")
#        result = cursor.fetchone()
#        print("Connection successful. Test query result:", result[0])
#        conn.close()
#    except Exception as e:
#        print("Connection failed:", e)

#if __name__ == "__main__":
#    test_connection()''
