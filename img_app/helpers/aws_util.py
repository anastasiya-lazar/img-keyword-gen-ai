import boto3
import os


def list_s3_objects(bucket_name, prefix='', region_name='eu-west-1'):
    """
    List all objects in a public S3 bucket with a given prefix.
    Returns a list of dictionaries containing object keys and URLs. Use AWS credentials if provided.
    """
    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
        region_name=region_name
    )
    s3_client = session.client('s3')

    print(f"Listing objects in bucket {bucket_name} with prefix {prefix}")

    continuation_token = None
    objects = []

    while True:
        list_kwargs = {
            "Bucket": bucket_name,
            "Prefix": prefix
        }
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token

        response = s3_client.list_objects_v2(**list_kwargs)

        if 'Contents' in response:
            for obj in response['Contents']:
                if not obj['Key'].endswith('/'):
                    # Generate URL for each object
                    object_url = f"https://{bucket_name}.s3.{region_name}.amazonaws.com/{obj['Key']}"
                    # Add object key and URL to the list
                    objects.append({'key': obj['Key'], 'url': object_url})

        # Check if there are more pages; if not, exit the loop
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    print(f"Found {len(objects)} objects in bucket {bucket_name} with prefix {prefix}")
    print(objects)

    return objects
