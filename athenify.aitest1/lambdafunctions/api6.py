import json
import boto3
from botocore.exceptions import ClientError

# S3 client initialization
s3 = boto3.client('s3')

def calculate_performance_score(video):
    weight_likes = 0.65
    weight_comments = 0.3
    weight_plays = 0.05
    
    like_score = video.get('like_count', 0) * weight_likes
    comment_score = video.get('comment_count', 0) * weight_comments
    play_score = video.get('play_count', 0) * weight_plays
    print(like_score)
    print(comment_score)
    total_score = like_score + comment_score + play_score
    
    return total_score

def lambda_handler(event, context):
    # Log the incoming event
    print(f"Received event: {json.dumps(event)}")
    
    # Parse the body to extract usernames and X
    try:
        if 'body' in event:
            body = json.loads(event['body'])
        else:
            body = event
        
        usernames = body.get('usernames', [])
        X = int(body.get('X', 5))  # Default to 5 if X is not provided
        
    except (KeyError, json.JSONDecodeError, ValueError) as e:
        print(f"Error parsing request body: {e}")
        return {"status": "error", "message": "Invalid input format"}
    
    if not usernames:
        print("No usernames provided.")
        return {"status": "error", "message": "No usernames provided"}
    
    # Ensure X does not exceed 5 * len(usernames)
    max_videos = 5 * len(usernames)
    if X > max_videos:
        print(f"X exceeds the allowed maximum: {X} > {max_videos}")
        return {"status": "error", "message": f"X cannot exceed {max_videos}"}
    
    # S3 Bucket name where the top 5 videos JSON files are stored
    bucket_name = 'top5videos-eachcreator'
    
    all_videos = []

    # Process each username
    for username in usernames:
        try:
            # Construct the filename based on the username
            file_name = f"{username}_top5_videos.json"
            
            # Fetch the file from the S3 bucket
            response = s3.get_object(Bucket=bucket_name, Key=file_name)
            json_data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Append videos to the all_videos list
            for video in json_data:
                video['username'] = username  # Preserve the username in the video data
                video['performance_score'] = calculate_performance_score(video)
                all_videos.append(video)
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"File not found for username {username}: {file_name}")
            else:
                print(f"Error processing username {username}: {e}")
    
    # Sort all videos by performance score in descending order
    sorted_videos = sorted(all_videos, key=lambda x: x['performance_score'], reverse=True)
    
    # Get the top X videos
    top_videos = sorted_videos[:X]
    
    # Return the sorted top X videos
    return {
        "status": "success",
        "data": top_videos
    }
