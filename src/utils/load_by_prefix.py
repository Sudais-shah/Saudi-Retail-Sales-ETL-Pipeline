import os
import pandas as pd

def load_latest_file_by_prefix(directory, prefix):
    """
    Load the latest versioned file by prefix using Pandas.
    """
    # Get all files that match the prefix
    files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(".csv")]
    if not files:
        raise FileNotFoundError(f"No files found with prefix '{prefix}' in {directory}")
    
    # Sort files by version (_v1, _v2, etc.)
    files.sort(key=lambda x: int(x.split('_v')[-1].split('.')[0]), reverse=True)
    latest_file = files[0]
    path = os.path.join(directory, latest_file)

    print(f"[INFO] Loading latest file: {latest_file}")

    # Load CSV with Pandas
    df = pd.read_csv(path)
    print(f"[INFO] DataFrame loaded successfully. Shape: {df.shape}")
    return df


