import os
import boto3
from botocore.client import Config

# ================= CẤU HÌNH MINIO =================
MINIO_ENDPOINT = 'http://10.10.10.100:9000'
ACCESS_KEY = 'B3AGGF445I6OCYEXUL2K'
SECRET_KEY = 'BmIwSo6auhfpnwzPUAPXDk9fDUMplJwb05TubJuO'
BUCKET_NAME = 'ingestion-zone'

# ================= THƯ MỤC CHỨA DATA =================
LOCAL_DATA_DIR = os.path.expanduser('~/eventsim_raw_data')
DESTINATION_FOLDER = 'events/'

def get_s3_client():
    return boto3.client('s3',
                        endpoint_url=MINIO_ENDPOINT,
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY,
                        config=Config(signature_version='s3v4'),
                        region_name='us-east-1')

def upload_raw_data():
    s3 = get_s3_client()

    # Quét tất cả các file trong thư mục raw
    for filename in os.listdir(LOCAL_DATA_DIR):
        local_file_path = os.path.join(LOCAL_DATA_DIR, filename)
        # Đường dẫn trên MinIO: bronze/events/ten_file.json
        s3_key = f"{DESTINATION_FOLDER}{filename}"

        print(f"🚀 Uploading: {filename} to MinIO ({s3_key})...")
        try:
            s3.upload_file(local_file_path, BUCKET_NAME, s3_key)
            print(f"✅ Success: {filename}")
        except Exception as e:
            print(f"❌ Error {filename}: {e}")

if __name__ == "__main__":
    print("Start Ingestion Progress to Bronze Layer...")
    upload_raw_data()
    print("Finished!")