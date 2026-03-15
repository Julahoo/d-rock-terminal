import os
from pathlib import Path
from src.ingestion import load_operations_data_from_uploads

class MockUploadedFile:
    def __init__(self, path):
        self.path = path
        self.name = os.path.basename(path)
        with open(path, "rb") as f:
            self.data = f.read()

    def seek(self, offset):
        pass

    def read(self):
        return self.data

def _get_bytes(self):
    return self.data
    
MockUploadedFile.read = _get_bytes

def main():
    raw_dir = Path("data/raw/callsu_daily")
    if not raw_dir.exists():
        print("No local data found.")
        return
        
    print(f"Scanning {raw_dir}...")
    files = list(raw_dir.glob("*.csv")) + list(raw_dir.glob("*.xlsx"))
    
    if not files:
        print("No CSVs/XLSX found.")
        return
        
    print(f"Found {len(files)} files. Mocking Streamlit UploadedFile objects...")
    
    # Due to Streamlit's internal seek(0) in the backend, we need a robust mock wrapper
    class StreamlitFileMock:
        def __init__(self, filepath):
            self.name = filepath.name
            self._path = filepath
            
        def seek(self, arg):
            pass
            
        def read(self):
            with open(self._path, 'rb') as f:
                return f.read()
                
        def __iter__(self):
            with open(self._path, 'rb') as f:
                yield from f

    mock_files = []
    
    # We actually just need to pass an object with .name and .seek() that pandas can read. 
    # The easiest way is an io.BytesIO object with a .name attribute.
    import io
    for f in files:
        try:
            with open(f, 'rb') as file_obj:
                b = io.BytesIO(file_obj.read())
                b.name = f.name
                mock_files.append(b)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    print(f"Executing vectorized load_operations_data_from_uploads on {len(mock_files)} files...")
    
    try:
        import time
        start = time.time()
        df = load_operations_data_from_uploads(mock_files)
        end = time.time()
        
        print(f"\n✅ SUCCESS: Vectorized Ingestion Matrix passed!")
        print(f"Rows Processed: {len(df):,}")
        print(f"Columns Output: {len(df.columns)}")
        print(f"Time Elapsed: {end - start:.2f} seconds")
        print(f"Speed: {len(df)/(end - start):,.0f} rows/second")
        
        if len(df) > 0:
            print("\nSample Data:")
            print(df[['ops_date', 'ops_client', 'campaign_name', 'country', 'true_cac']].head())
            
    except Exception as e:
        print(f"\n❌ FAILED: Python Exception during vectorized ingestion.")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
