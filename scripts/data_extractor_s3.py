import os
import io
import zipfile
from s3_utils import list_files_in_s3, s3_client

S3_ZIP_FOLDER = "sec_raw_zips/"
S3_EXTRACT_FOLDER = "sec_extracted_tsv/"

def extract_quarterly(zip_filename):
    """Extracts a ZIP file directly from S3 and stores extracted TSVs in quarter-wise folders in S3."""
    s3_zip_path = os.path.join(S3_ZIP_FOLDER, zip_filename)

    # Download ZIP file into memory
    zip_obj = s3_client.get_object(Bucket=os.getenv("S3_BUCKET_NAME"), Key=s3_zip_path)
    zip_data = io.BytesIO(zip_obj['Body'].read())  # Load into memory

    # Get quarter folder name from ZIP filename (e.g., "2024q1")
    quarter_folder = zip_filename.replace(".zip", "")

    with zipfile.ZipFile(zip_data, "r") as z:
        for extracted_file in z.namelist():
            file_data = z.read(extracted_file)  # Read file into memory
            
            # 🔹 FIX: Ensure correct folder structure by explicitly adding a `/`
            s3_file_path = f"{S3_EXTRACT_FOLDER}{quarter_folder}/{extracted_file}"

            # Upload extracted file to S3
            s3_client.put_object(Bucket=os.getenv("S3_BUCKET_NAME"), Key=s3_file_path, Body=file_data)
            print(f"Uploaded: {s3_file_path} to S3.")

    print(f"Finished processing: {zip_filename}")

def extract_all_quarters():
    """Fetches ZIP files from S3 and extracts them to quarter-wise folders in S3."""
    zip_files = [file.split("/")[-1] for file in list_files_in_s3(S3_ZIP_FOLDER) if file.endswith(".zip")]

    if not zip_files:
        print("⚠ No ZIP files found in S3!")
        return

    for zip_file in zip_files:
        extract_quarterly(zip_file)

    print("All ZIP files extracted and TSV files uploaded to quarter-wise folders in S3.")

if __name__ == "__main__":
    extract_all_quarters()
