import pandas as pd
import glob
import os

def read_flink_output_no_header(base_path: str) -> pd.DataFrame:
    """
    Reads partitioned, header-less CSV files produced by a Flink FileSink,
    handling Flink's default file naming (no .csv extension).

    Args:
        base_path: The root directory of the Flink sink output
                   (e.g., 'output/ctr_results').

    Returns:
        A single pandas DataFrame containing the data from all partitions.
    """
    # Define the column names that match your Flink sink table DDL
    column_names = [
        'window_start',
        'window_end',
        'impressions',
        'clicks',
        'ctr'
    ]

    # --- THE KEY CHANGE IS HERE ---
    # The glob pattern now looks for files starting with 'part-' and does
    # not require a .csv extension. This matches Flink's default naming.
    file_pattern = os.path.join(base_path, '**', 'part-*')
    
    # We use glob to find all part-files recursively.
    part_files = glob.glob(file_pattern, recursive=True)

    if not part_files:
        print(f"No committed part-files found in '{base_path}'.")
        print("Make sure files are committed (not '.inprogress') and the path is correct.")
        return pd.DataFrame()

    all_dataframes = []

    print(f"Found {len(part_files)} part-files to read...")

    for file_path in part_files:
        # Ignore any potential directories that might match the pattern
        if not os.path.isfile(file_path):
            continue

        # Read the individual file, applying the manual header
        df_part = pd.read_csv(
            file_path,
            header=None,
            names=column_names
        )

        # Extract Partition Information from the Path (logic is unchanged)
        parent_dir = os.path.basename(os.path.dirname(file_path))
        if '=' in parent_dir:
            partition_key, partition_value = parent_dir.split('=', 1)
            df_part[partition_key] = partition_value

        all_dataframes.append(df_part)

    # If no valid files were processed, return an empty DataFrame
    if not all_dataframes:
        print("Warning: Pattern matched some paths, but none were valid files.")
        return pd.DataFrame()

    # Concatenate all the individual DataFrames into a single one
    final_df = pd.concat(all_dataframes, ignore_index=True)

    return final_df

if __name__ == '__main__':
    results_path = 'output/ctr_results'

    ctr_data = read_flink_output_no_header(results_path)

    if not ctr_data.empty:
        print("\nSuccessfully loaded the data into a single DataFrame:")
        print(ctr_data.head())