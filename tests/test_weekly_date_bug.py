import pandas as pd
from datetime import datetime, timedelta
import pytest

def test_original_failing_case():
    # Attempting to mimic `latest_snaps` coming from the DB without explicit datetime typing
    latest_snaps = pd.DataFrame({"ops_date": ["2026-03-24", "2026-03-25", "2026-03-26"]})
    last_thursday = pd.Timestamp("2026-03-25")
    
    with pytest.raises(TypeError):
        # This will fail because '<=' not supported between 'str' and 'Timestamp'
        macro_df = latest_snaps[latest_snaps['ops_date'] <= last_thursday].copy()
        
def test_hypothesis_fix():
    latest_snaps = pd.DataFrame({"ops_date": ["2026-03-24", "2026-03-25", "2026-03-26"]})
    last_thursday = pd.Timestamp("2026-03-25")
    
    # The Fix
    macro_df = latest_snaps.copy()
    macro_df['ops_date'] = pd.to_datetime(macro_df['ops_date'], errors='coerce')
    macro_df = macro_df[macro_df['ops_date'] <= last_thursday]
    
    assert len(macro_df) == 2

if __name__ == "__main__":
    test_original_failing_case()
    test_hypothesis_fix()
    print("All tests passed: Failing test safely caught TypeError, and Hypothesis Fix successfully filtered the DataFrame.")
