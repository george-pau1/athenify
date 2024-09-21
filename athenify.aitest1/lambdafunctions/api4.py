import requests
import json
import boto3
import time

def fetch_user_reels(usernames, api_key, bucket_name):
    #Took out api call here
    base_url = ""
    headers=""

    # Initialize S3 client with debugging information
    try:
        s3 = boto3.client('s3')
        print("S3 client initialized successfully.")
    except Exception as e:
        print("Failed to initialize S3 client.")
        print(f"Error: {e}")
        return {"status": "error", "message": "Failed to initialize S3 client."}

    results = []  # Array to hold the results for each user
    
    for username in usernames:
        try:
            print(f"Fetching reels for username: {username}")
            
            # Make the request for each username
            url = f"{base_url}/{username}"
            querystring = {"count": "30"}
            print(f"Request URL: {url}")
            print(f"Headers: {headers}")
            print(f"Query Parameters: {querystring}")
            
            response = requests.get(url, headers=headers, params=querystring)
            print(f"Response Status Code: {response.status_code}")
            print(f"Response Content: {response.content}")
            
            # Check for successful response
            if response.status_code == 200:
                data = response.json()
                print(f"Response JSON: {json.dumps(data, indent=4)}")
                
                # Append the result to the array
                results.append({
                    "username": username,
                    "data": data
                })
                
                # Save each response to a separate JSON file in /tmp directory
                file_name = f"/tmp/{username}_reels.json"
                try:
                    with open(file_name, "w") as f:
                        json.dump(data, f, indent=4)
                    print(f"Saved data to file: {file_name}")
                except Exception as e:
                    print(f"Failed to save data to file: {file_name}")
                    print(f"Error: {e}")
                    continue

                # Upload the JSON file to S3
                try:
                    s3.upload_file(file_name, bucket_name, f"{username}_reels.json")
                    print(f"Uploaded {username}_reels.json to S3 bucket {bucket_name}")
                except Exception as e:
                    print(f"Failed to upload {username}_reels.json to S3.")
                    print(f"Error: {e}")
                
                print(f"Successfully fetched data for {username}")
            else:
                print(f"Failed to fetch reels for {username}. Status code: {response.status_code}")
                print(f"Response Content: {response.content}")
        
        except Exception as e:
            print(f"Error fetching data for {username}: {e}")
        
        # Rate limiting: wait for 1 second before making the next API call
        time.sleep(3)

    # Return the array with all user data
    return results

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
        api_key = body['api_key']
        usernames = body['usernames']
        bucket_name = body['bucket_name']
        
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
    
    print(f"Received API key: {api_key}")
    print(f"Usernames: {usernames}")
    print(f"S3 Bucket Name: {bucket_name}")

    if not usernames:
        print("No usernames provided.")
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",  # Allow Content-Type header
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"  # Allow these methods
            },
            "body": json.dumps({"status": "error", "message": "No usernames provided"})
        }

    # Fetch the user reels and upload to the specified S3 bucket
    all_user_data = fetch_user_reels(usernames, api_key, bucket_name)

    print("Lambda handler finished.")
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Headers": "Content-Type",  # Allow Content-Type header
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"  # Allow these methods
        },
        "body": json.dumps({
            "status": "success",
            "data": all_user_data
        })
    }
