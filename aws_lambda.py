import boto3
import csv
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'your-source-bucket-name'
    output_bucket_name = 'your-output-bucket-name'
    output_folder = 'output-folder/'
    output_file = 'object_info.csv'
    
    # List objects in the source bucket
    response = s3.list_objects_v2(Bucket=bucket_name)
    
    # Prepare CSV file
    csv_file_path = f'/tmp/{output_file}'
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Name', 'URL', 'Type'])
        
        # Iterate over each object in the bucket
        for obj in response.get('Contents', []):
            object_name = obj['Key']
            object_url = f"https://{bucket_name}.s3.amazonaws.com/{object_name}"
            object_type = obj['Key'].split('.')[-1] if '.' in obj['Key'] else 'Unknown'
            
            # Write object info to CSV
            writer.writerow([object_name, object_url, object_type])
    
    # Upload CSV file to the output folder in the same bucket
    s3.upload_file(csv_file_path, output_bucket_name, f"{output_folder}{output_file}")
    
    return {
        'statusCode': 200,
        'body': f"CSV file created and uploaded to {output_folder}{output_file} in bucket {output_bucket_name}"
    }
