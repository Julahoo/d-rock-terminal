import time
import requests
import sys

def main():
    """
    CRON JOB: Synthetically pre-warms the Streamlit application cache.
    Triggered via Railway cron daily at 04:30 AM.
    """
    # The application's public (or internal) Railway URL
    # Replace URL with the actual production Railway domain for the app
    APP_URL = "https://crmtracker.up.railway.app" # Using typical Railway pattern
    
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting Cache Pre-warming Protocol for {APP_URL}")
    print("Sending synthetic GET request to trigger @st.cache_data hydration...")

    try:
        # A simple GET request is enough to wake the container and trigger app.py execution
        # which will fire the unified data access block we wrote globally lines 355-385
        response = requests.get(APP_URL, timeout=120)
        
        if response.status_code == 200:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] SUCCESS: Cache pre-warmed. Status Code: 200")
            print("The application has fetched the latest data from PostgreSQL and locked it into RAM.")
            sys.exit(0)
        else:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] WARNING: Received status code {response.status_code}")
            sys.exit(1)
            
    except requests.exceptions.Timeout:
        # Streamlit might just take really long to boot if data is huge, which is fine, 
        # the read_sql is doing its work in the background.
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] TIMEOUT: The request timed out, but the container was awakened.")
        print("Assuming cache hydration is ongoing in the background.")
        sys.exit(0)
    except Exception as e:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Failed to reach application. {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
