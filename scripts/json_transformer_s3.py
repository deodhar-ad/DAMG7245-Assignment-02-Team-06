import os
import io
import pandas as pd
import json
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from s3_utils import list_folders_in_s3, s3_client, list_files_in_s3

# S3 Paths
S3_EXTRACT_FOLDER = "sec_extracted_tsv/"
S3_JSON_FOLDER = "sec_json_data/"

def read_tsv_from_s3(s3_path):
    """Reads a TSV file from S3 into a Pandas DataFrame."""
    try:
        obj = s3_client.get_object(Bucket=os.getenv("S3_BUCKET_NAME"), Key=s3_path)
        return pd.read_csv(io.BytesIO(obj["Body"].read()), sep="\t", dtype=str)
    except Exception as e:
        print(f"Error reading {s3_path} from S3: {e}")
        return pd.DataFrame()

def safe_int(value, default=0):
    """Safely converts a value to int, handling NaN values."""
    try:
        return int(float(value)) if pd.notna(value) and str(value).strip() else default
    except ValueError:
        return default

def process_sub_row(sub_row, dfNum, dfPre, dfTag):
    """Processes a single submission row into JSON format."""
    try:
        fiscal_year = safe_int(sub_row.get("fy", 0))
        fiscal_period = str(sub_row.get("fp", "Unknown")).strip().upper() if sub_row.get("fp") else "Unknown"

        json_output = {
            "symbol": sub_row.get("name", "Unknown"),
            "name": sub_row.get("name", "Unknown"),
            "country": sub_row.get("countryba", "Unknown"),
            "city": sub_row.get("cityba", "Unknown"),
            "year": fiscal_year,
            "quarter": fiscal_period,
            "data": {"bs": [], "cf": [], "ic": []}
        }

        # Optimized filtering
        filteredNum = dfNum[dfNum['adsh'] == sub_row['adsh']]
        statement_mapping = dfPre[dfPre['adsh'] == sub_row['adsh']].set_index('tag')['stmt'].to_dict()
        tag_mapping = dfTag.set_index('tag')['tlabel'].to_dict()

        for _, num_row in filteredNum.iterrows():
            statement_type = statement_mapping.get(num_row['tag'], None)
            entry = {
                "concept": num_row['tag'],
                "label": tag_mapping.get(num_row['tag'], num_row['tag']),
                "value": safe_int(num_row['value']),
                "unit": num_row['uom']
            }

            if statement_type == 'BS':
                json_output['data']['bs'].append(entry)
            elif statement_type == 'CF':
                json_output['data']['cf'].append(entry)
            elif statement_type == 'IC':
                json_output['data']['ic'].append(entry)

        return json_output

    except Exception as e:
        print(f"Error processing row {sub_row.get('name', 'Unknown')}: {e}")
        return None

def process_quarter(quarter_folder):
    """Processes a single quarter into JSON format and uploads to S3."""
    print(f"Processing quarter: {quarter_folder}")

    quarter_s3_path = f"{S3_EXTRACT_FOLDER}{quarter_folder}/"

    # List files in this quarter folder
    quarter_files = list_files_in_s3(quarter_s3_path)
    print(f"Files in {quarter_folder}: {quarter_files}")

    if not quarter_files:
        print(f"No extracted TSV files found in S3 for {quarter_folder}. Skipping...")
        return

    print(f"Loading TSV files from S3 for {quarter_folder}...")
    try:
        dfSub = read_tsv_from_s3(f"{quarter_s3_path}sub.txt")
        dfNum = read_tsv_from_s3(f"{quarter_s3_path}num.txt")
        dfPre = read_tsv_from_s3(f"{quarter_s3_path}pre.txt")
        dfTag = read_tsv_from_s3(f"{quarter_s3_path}tag.txt")

        if dfSub.empty or dfNum.empty or dfPre.empty or dfTag.empty:
            print(f"Missing data in {quarter_folder}, skipping...")
            return

        print(f"TSV files loaded for {quarter_folder}. Processing JSON...")
    except Exception as e:
        print(f"Error reading TSV files for {quarter_folder}: {e}")
        return

    json_output_list = []

    # Use ThreadPoolExecutor for faster row processing
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda row: process_sub_row(row, dfNum, dfPre, dfTag), dfSub.to_dict("records")))

    # Remove None values (failed transformations)
    json_output_list = [res for res in results if res is not None]

    # Convert JSON to string and upload to S3
    json_data = json.dumps(json_output_list, indent=4)
    json_s3_path = f"{S3_JSON_FOLDER}{quarter_folder}.json"

    try:
        s3_client.put_object(Bucket=os.getenv("S3_BUCKET_NAME"), Key=json_s3_path, Body=json_data)
        print(f"JSON for {quarter_folder} uploaded to S3 at {json_s3_path}")
    except Exception as e:
        print(f"Error uploading JSON for {quarter_folder} to S3: {e}")

def parallel_json_processing():
    """Runs JSON transformation for each quarter in parallel."""
    quarters = list_folders_in_s3(S3_EXTRACT_FOLDER)  # Get folders (not files)

    #print(f"DEBUG: List of extracted quarter folders in S3: {quarters}")

    if not quarters:
        print("⚠ No quarter-wise extracted data found in S3! Exiting...")
        return

    # Extract just the quarter names from the full S3 path
    quarter_names = [folder.rstrip("/").split("/")[-1] for folder in quarters]

    print(f"Found {len(quarter_names)} quarters to process")

    num_cores = os.cpu_count()
    print(f"Starting multiprocessing with {num_cores} cores...")

    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = {executor.submit(process_quarter, quarter): quarter for quarter in quarter_names}

        for future in futures:
            try:
                future.result()  # Ensure exceptions inside processes are logged
            except Exception as e:
                print(f"Error in processing {futures[future]}: {e}")

    print("Multiprocessing completed.")

if __name__ == "__main__":
    parallel_json_processing()
