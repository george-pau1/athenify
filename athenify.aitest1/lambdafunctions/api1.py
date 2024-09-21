import json
import boto3
import requests

def lambda_handler(event, context):
    # Instagram scraper API details
    api_url = "change this"
    api_key = "change this"
    
    #This is the new code: /////
    #///////
    
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
    
    # S3 bucket details
    bucket_name = "user-following"
    
    
    if not username:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Username is required"})
        }
    
    # Prepare API request
    querystring = {"username_or_id": username, "count": "2", "version": "v2"} # Make sure to change this later
    headers = {
        ""
    }
    
    # Send request to Instagram API
    try:
        response = requests.get(api_url, headers=headers, params=querystring, timeout=10)
    except requests.exceptions.RequestException as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Error while requesting Instagram API: {str(e)}"})
        }
    
    if response.status_code != 200:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to fetch data from Instagram API"})
        }
    
    # Extract user data from the API response
    user_data = response.json().get("data", {}).get("users", [])
    if not user_data:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": "No users found in the Instagram response"})
        }
    
    usernames = [user["username"] for user in user_data]
    
    # Initialize S3 client
    try:
        s3 = boto3.client('s3')
        print("S3 client initialized successfully.")
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Failed to initialize S3 client: {str(e)}"})
        }
    
    # Create folder and upload JSON file to S3
    folder_name = username
    file_name = f"{folder_name}/usernames.json"
    
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(usernames),
            ContentType="application/json"
        )
        print(f"Usernames successfully uploaded to {file_name} in S3 bucket {bucket_name}.")
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Failed to upload file to S3: {str(e)}"})
        }
    
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps({
            "message": "Usernames successfully uploaded to S3",
            "successful_usernames": usernames
        })
    }
