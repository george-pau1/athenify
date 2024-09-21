import json
import requests
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Function to check if the value "1" is present anywhere in a nested dictionary or list
def contains_one(data):
    if data is None:
        return False
    
    if isinstance(data, dict):
        return any(contains_one(value) or '1' in str(value) for value in data.values())
    
    elif isinstance(data, list):
        return any(contains_one(item) or '1' in str(item) for item in data)
    
    return '1' in str(data)
    

def make_request_with_retry(url, headers, params=None, max_retries=5):
    retry_count = 0
    backoff_time = 2  # Start with 2 seconds

    while retry_count < max_retries:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            time.sleep(2)  # Wait for 2 seconds between successful requests
            return response
        elif response.status_code == 429:  # Rate limit error
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                wait_time = int(retry_after)
            else:
                wait_time = backoff_time
            
            print(f"Rate limit hit. Waiting for {wait_time} seconds before retrying...")
            time.sleep(wait_time)
            backoff_time *= 2  # Exponentially increase the wait time
            retry_count += 1
        else:
            # Wait for 2 seconds between requests even if the status code is not 200 or 429
            time.sleep(4)
            return response
    
    # If all retries fail, return None
    return None


def lambda_handler(event, context):
    try:
        # Parse the incoming event for the request body
        try:
            if isinstance(event, str):  # In case the event is passed as a string
                event = json.loads(event)
            
            if 'body' in event:  # If the event has a 'body' field, parse it
                body = event['body']
                if isinstance(body, str):
                    body = json.loads(body)  # Parse the JSON string in the body
            else:
                body = event

            # Validate necessary fields in the request body
            if not isinstance(body, dict):
                raise ValueError("Request body is not a valid dictionary.")
            if 'usernames' not in body or not isinstance(body['usernames'], list):
                raise ValueError("Missing or invalid 'usernames' field.")
            if 'niche' not in body or not isinstance(body['niche'], str):
                raise ValueError("Missing or invalid 'niche' field.")
            if 'level' not in body or not isinstance(body['level'], str):
                raise ValueError("Missing or invalid 'level' field.")
            if 'followercount' not in body or not isinstance(body['followercount'], str):
                raise ValueError("Missing or invalid 'followercount' field.")
        
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing request body: {e}")
            return {
                "statusCode": 400,
                "headers": {
                    "Access-Control-Allow-Headers": "Content-Type",
                    "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
                },
                "body": json.dumps({"status": "error", "message": str(e)})
            }
        
        # Extract the required fields from the body
        usernames = body['usernames']
        niche = body['niche']
        level = body['level']
        followercount = body['followercount']
        
        print(f"Followercount from the body: {followercount}")
        print(f"Level from the body: {level}")

        # Define API base URL and headers
        url_base = ""
        querystring = {"count": "15"}
        headers = {
            ""
        }

        successful_usernames = []
        logs = []

        for username in usernames:
            try:
                url = f"{url_base}{username}"
                response = make_request_with_retry(url, headers, params=querystring)

                if response.status_code != 200:
                    logs.append(f"Failed to retrieve data for {username}, status code: {response.status_code}")
                    continue  # Skip to the next username if the request failed

                data = response.json()
                
                # Safeguard: Validate response structure
                if not data or 'data' not in data or 'items' not in data['data']:
                    logs.append(f"No 'items' key found in the data for {username}")
                    continue

                items = data['data']['items']
                all_combined_items = []

                for item in items:
                    media = item.get('media', {}) if isinstance(item.get('media'), dict) else {}
                    user = media.get('user', {}) if isinstance(media.get('user'), dict) else {}
                    music_metadata = media.get('music_metadata', {}) if isinstance(media.get('music_metadata'), dict) else {}

                    extracted_data = {
                        "caption": media.get('caption', {}).get('text', None) if isinstance(media.get('caption'), dict) else None,
                        "username": user.get('username', None),
                        "full_name": user.get('full_name', None),
                        "text": media.get('caption', {}).get('text', None) if isinstance(media.get('caption'), dict) else None,
                        "hashtags": media.get('hashtags', []) if isinstance(media.get('hashtags'), list) else [],
                        "is_verified": user.get('is_verified', None)
                    }

                    all_combined_items.append(extracted_data)

                # Prepare the input for the niche function
                all_combined_items_str = [json.dumps(item) for item in all_combined_items]
                nicheinput = "".join(all_combined_items_str) if all_combined_items_str else "Return "

                nicheurl = "change this"
                bodyniche = {
                    "model": "gpt-4o-mini",
                    "message": nicheinput,
                    "niche": niche,
                    "level": level
                }

                responseniche = requests.post(nicheurl, json=bodyniche)

                if responseniche.status_code != 200:
                    logs.append(f"Error from niche API for {username}: {responseniche.status_code}")
                    if responseniche.status_code == 500 or responseniche.status_code == "500":
                        successful_usernames.append(username)  # Allow usernames even on niche API failure
                    continue

                niche_result = responseniche.json()

                if niche_result and contains_one(niche_result):
                    successful_usernames.append(username)

            except Exception as e:
                logs.append(f"Error processing username {username}: {str(e)}")
        
        filtered_usernames = []
        for username in successful_usernames:
            try:
                base_url = ""
                url = ""
                response = requests.get(url, headers=headers)
                
                if response.status_code != 200:
                    logs.append(f"Failed to fetch data for {username}, status code: {response.status_code}")
                    continue

                data = response.json()

                # Safeguard: Validate follower count access
                if not data or 'data' not in data or 'edge_followed_by' not in data['data']:
                    logs.append(f"Missing follower count data for {username}")
                    continue

                follower_count = int(data['data']['edge_followed_by'].get('count', 0))
                print(f"Username: {username}, Follower Count: {follower_count}")

                if follower_count <= int(followercount):
                    filtered_usernames.append(username)

            except Exception as e:
                logs.append(f"Error processing follower count for {username}: {str(e)}")

        # Return the list of successful usernames with CORS headers and logs
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({
                "successful_usernames": filtered_usernames,
                "logs": logs
            })
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({
                "error": str(e)
            })
        }
