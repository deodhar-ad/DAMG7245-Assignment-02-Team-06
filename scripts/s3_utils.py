import os
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS S3 Configuration
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Initialize S3 Client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

def upload_file_to_s3(local_path, s3_path):
    """Uploads a file to S3."""
    try:
        s3_client.upload_file(local_path, S3_BUCKET_NAME, s3_path)
        print(f"Uploaded: {s3_path} to S3.")
    except Exception as e:
        print(f"Error uploading {local_path}: {e}")

def download_file_from_s3(s3_path, local_path):
    """Downloads a file from S3."""
    try:
        s3_client.download_file(S3_BUCKET_NAME, s3_path, local_path)
        print(f"Downloaded {s3_path} from S3.")
    except Exception as e:
        print(f"Error downloading {s3_path}: {e}")

def list_files_in_s3(prefix):
    """Lists all files in S3 with the given prefix (folder)."""
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]
    except Exception as e:
        print(f"Error listing files in S3: {e}")
        return []

def delete_local_file(file_path):
    """Deletes a local file after uploading to S3."""
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted local file: {file_path}")

def list_folders_in_s3(prefix):
    """
    Lists only top-level folders (quarters) in the specified S3 prefix.
    Used by JSON Transformer to detect available quarters.
    """
    response = s3_client.list_objects_v2(Bucket=os.getenv("S3_BUCKET_NAME"), Prefix=prefix, Delimiter="/")

    # Extract only folder names (not individual files)
    folders = [obj["Prefix"] for obj in response.get("CommonPrefixes", [])] if "CommonPrefixes" in response else []
    
    return folders