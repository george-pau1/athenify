import json
import boto3

s3 = boto3.client('s3')
BUCKET_NAME = 'user-following'  # Replace with your actual bucket name

def lambda_handler(event, context):
    print("Lambda handler started.")
    print(f"Received event: {json.dumps(event)}")  # Log the event to debug issues with the request payload
    
    # Try to parse the body directly if event contains raw body content
    try:
        if isinstance(event, str):  # In case the event is passed as a string
            event = json.loads(event)
        
        if 'body' in event:  # If the event has a 'body' field, parse it
            body = event['body']
            if isinstance(body, str):
                body = json.loads(body)  # Parse the JSON string in the body
        else:
            body = event
        
        # Extract the required fields from the body
        username = body['username']
        
    except (KeyError, json.JSONDecodeError) as e:
        print(f"Error parsing request body: {e}")
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",  # Allow Content-Type header
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"  # Allow these methods
            },
            "body": json.dumps({"status": "error", "message": "Invalid input format"})
        }

    # Fetch the folder associated with the username in the S3 bucket
    try:
        file_key = f"{username}/usernames.json"  # Path to the usernames.json file in the bucket
        response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        usernames = json.loads(file_content)  # The file is a JSON array, so we directly load it as a list
    
    except s3.exceptions.NoSuchKey:
        print(f"File not found for user {username}")
        return {
            "statusCode": 404,
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",  
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },
            "body": json.dumps({"status": "error", "message": "File not found"})
        }
    except Exception as e:
        print(f"Error accessing S3: {e}")
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",  
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },
            "body": json.dumps({"status": "error", "message": "Internal server error"})
        }

    print("Lambda handler finished.")
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Headers": "Content-Type",  
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"  
        },
        "body": json.dumps({
            "status": "success",
            "data": usernames  # Directly return the usernames array
        })
    }
