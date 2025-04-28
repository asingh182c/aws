import boto3
import csv
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'smarteamtotc-legacypoc'
    output_folder = 'index/'  # Specify the folder where the CSV file will be saved
    
    # Create a temporary file to store CSV data
    tmp_file = '/tmp/s3_objects.csv'
    
    # Open the CSV file for writing
    with open(tmp_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Folder Name', 'Object Name', 'Size (Bytes)', 'Object URL'])
        
        # List objects in the bucket
        response = s3.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                size = obj['Size']
                
                if key.endswith('/'):
                    folder_name = key
                else:
                    folder_name = os.path.dirname(key)
                    object_name = os.path.basename(key)
                    object_url = f"https://{bucket_name}.smarteamtotc-legacypoc.s3.us-east-1.amazonaws.com/{key}"
                    
                    writer.writerow([folder_name, object_name, size, object_url])
    
    # Upload the CSV file to the specified folder in the S3 bucket
    s3.upload_file(tmp_file, bucket_name, f'{output_folder}s3_objects.csv')
    
    return {
        'statusCode': 0,
        'body': 'CSV file created and uploaded successfully!'
    }
