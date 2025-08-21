import mysql.connector
from mysql.connector import Error
import time


MYSQL_HOST = 'localhost'   
MYSQL_PORT = 3307           #
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root'
MYSQL_DB = 'steam_db'
MAX_RETRIES = 3
RETRY_DELAY = 2

def test_mysql_connection():
    print("🧪 Testing MySQL Connection...")
    print("=" * 40)
    print(f"Host: {MYSQL_HOST}:{MYSQL_PORT}")
    print(f"Database: {MYSQL_DB}")
    print(f"User: {MYSQL_USER}")
    print("=" * 40)
    
    for i in range(MAX_RETRIES):
        try:
            connection = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DB
            )
            
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            connection.close()
            
            if result and result[0] == 1:
                print("✅ CONNECTION SUCCESSFUL!")
                print("MySQL connection is working properly.")
                return True
                
        except Error as e:
            print(f"❌ Attempt {i+1}/{MAX_RETRIES} failed: {e}")
            
            if i == MAX_RETRIES - 1:
                print("💥 All connection attempts failed!")
                return False
            
            wait_time = RETRY_DELAY ** i
            print(f"⏳ Waiting {wait_time} seconds before next attempt...")
            time.sleep(wait_time)
    
    return False

def main():
    success = test_mysql_connection()
    
    print("\n" + "=" * 40)
    if success:
        print("🎉 FINAL RESULT: CONNECTION SUCCESSFUL!")
        exit(0)
    else:
        print("💥 FINAL RESULT: CONNECTION FAILED!")
        
if __name__ == "__main__":
    main()