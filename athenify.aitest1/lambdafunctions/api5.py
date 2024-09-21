import json
import boto3

# S3 client initialization
s3 = boto3.client('s3')

def calculate_performance_score(video):
    weight_likes = 0.4
    weight_comments = 0.3
    weight_plays = 0.3
    
    # print(video)
    
    like_score = video.get('like_count', 0) * weight_likes
    comment_score = video.get('comment_count', 0) * weight_comments
    play_score = video.get('play_count', 0) * weight_plays
    
    total_score = like_score + comment_score + play_score
    
    return total_score

def parse_video_metadata(media_data):
    parsed_data = {}
    
    # Engagement and Interaction
    parsed_data['like_count'] = media_data.get('like_count', 0)
    parsed_data['comment_count'] = media_data.get('comment_count', 0)
    parsed_data['play_count'] = media_data.get('play_count', 0)
    parsed_data['has_liked'] = media_data.get('has_liked', False)
    
    #print(f"This is the has liked:{media_data}") This works
    
    # Content and Context
    caption = media_data.get('caption', None)
    if caption and isinstance(caption, dict):  # Ensure caption is not None and is a dictionary
        parsed_data['caption_text'] = caption.get('text', '')
    else:
        parsed_data['caption_text'] = ''

    parsed_data['hashtags'] = [word for word in parsed_data['caption_text'].split() if word.startswith('#')]
    parsed_data['tagged_accounts'] = []

    usertags = media_data.get('usertags', None)
    if usertags and isinstance(usertags, dict):  # Ensure usertags is not None and is a dictionary
        in_tags = usertags.get('in', None)
        if in_tags and isinstance(in_tags, list):  # Ensure 'in' is not None and is a list
            parsed_data['tagged_accounts'] = [
                tag.get('user', {}).get('username', '') 
                for tag in in_tags 
                if isinstance(tag, dict) and 'user' in tag and isinstance(tag.get('user'), dict) and 'username' in tag['user']
            ]

    
    #Original Sound Handling
    clips_metadata = media_data.get('clips_metadata', {})
    if not clips_metadata == None:
        original_sound_info = clips_metadata.get('original_sound_info', None)
        if not original_sound_info == None:
            parsed_data['original_sound'] = original_sound_info.get('audio_asset_id', None) if original_sound_info else None
    
    # Visual and Technical Quality
    parsed_data['video_quality'] = [
        version['width'] for version in media_data.get('video_versions', [])
        if 'width' in version
    ]
    parsed_data['video_duration'] = media_data.get('video_duration', 0)
    parsed_data['has_audio'] = media_data.get('has_audio', False)
    
    # Extracting video URL
    video_versions = media_data.get('video_versions', [])
    if video_versions:
        parsed_data['video_url'] = video_versions[0].get('url', '')
    
    # User and Account Details
    user_data = media_data.get('user', {})
    parsed_data['account_private'] = user_data.get('is_private', True)
    parsed_data['account_verified'] = user_data.get('is_verified', False)
    parsed_data['profile_pic_url'] = user_data.get('profile_pic_url', '')
    parsed_data['username'] = user_data.get('username', '')
    
    # Platform-Specific Features
    parsed_data['can_viewer_save'] = media_data.get('can_viewer_save', False)
    parsed_data['can_viewer_reshare'] = media_data.get('can_viewer_reshare', False)
    
    # Hashtags and Keywords
    parsed_data['relevant_hashtags'] = parsed_data.get('hashtags', [])
    
    # Engagement Optimization
    mashup_info = clips_metadata.get('mashup_info', {})
    if not mashup_info == None:
        parsed_data['mashup_allowed'] = mashup_info.get('mashups_allowed', False)
        parsed_data['mashup_count'] = mashup_info.get('non_privacy_filtered_mashups_media_count', 0)
    
    # Metadata and Algorithmic Factors
    parsed_data['logging_info_token'] = media_data.get('logging_info_token', '')
    parsed_data['tracking_token'] = media_data.get('organic_tracking_token', '')
    
    # Cross-Promotion Potential
    parsed_data['tagged_brands'] = [
        tag['user']['username'] for tag in media_data.get('usertags', {}).get('in', [])
        if 'user' in tag and tag['user'].get('is_verified', False)
    ]
    
    return parsed_data

def parse_and_rank_videos(json_data):
    video_list = []
    
    for item in json_data['data']['items']:
        media_data = item.get('media', {})
        if media_data:  # Ensuring media data exists
            parsed_video = parse_video_metadata(media_data)
            parsed_video['performance_score'] = calculate_performance_score(parsed_video)
            video_list.append(parsed_video)
    
    # Sort videos by performance score in descending order
    sorted_videos = sorted(video_list, key=lambda x: x['performance_score'], reverse=True)
    
    # Get the top 5 performing videos
    top_5_videos = sorted_videos[:5]
    
    return top_5_videos

def lambda_handler(event, context):
    # Log the incoming event
    print(f"Received event: {json.dumps(event)}")
    
    # Parse the body to extract usernames
    try:
        if 'body' in event:
            body = json.loads(event['body'])
        else:
            body = event
        
        usernames = body.get('usernames', [])
    except (KeyError, json.JSONDecodeError) as e:
        print(f"Error parsing request body: {e}")
        return {"status": "error", "message": "Invalid input format"}
    
    if not usernames:
        print("No usernames provided.")
        return {"status": "error", "message": "No usernames provided"}
    
    # S3 Bucket names
    source_bucket_name = 'instascraper'
    destination_bucket_name = 'top5videos-eachcreator'
    
    # Process each username
    for username in usernames:
        try:
            # Construct the filename based on the username
            file_name = f"{username}_reels.json"
            
            # Fetch the file from the source S3 bucket
            response = s3.get_object(Bucket=source_bucket_name, Key=file_name)
            json_data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Parse and rank videos
            top_5_videos = parse_and_rank_videos(json_data)
            
            # Prepare the data to be stored in the destination S3 bucket
            output_file_name = f"{username}_top5_videos.json"
            output_data = json.dumps(top_5_videos, indent=4)
            
            # Upload the JSON file to the destination S3 bucket
            s3.put_object(Bucket=destination_bucket_name, Key=output_file_name, Body=output_data)
            
            print(f"Successfully stored top 5 videos for {username} in {destination_bucket_name}/{output_file_name}")
            
        except Exception as e:
            print(f"Error processing username {username}: {e}")
    
    return {
        "status": "success",
        "message": "Top 5 videos stored in S3 bucket for each user."
    }
