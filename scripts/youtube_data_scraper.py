from googleapiclient.discovery import build
import pandas as pd
from pytube import YouTube

def fetch_youtube_data(api_key, genre, max_results=500):
    """
    Fetch top YouTube video details for a given genre and save data to a CSV file.
    
    Parameters:
    - api_key: str, YouTube Data API key
    - genre: str, The genre to search for (e.g., "Technology")
    - max_results: int, Total number of videos to fetch (default is 500)
    
    Returns:
    - data: List of dictionaries with video data.
    """
    youtube = build('youtube', 'v3', developerKey=api_key)
    video_data = []
    next_page_token = None
    results_fetched = 0
    
    while results_fetched < max_results:
        # Fetch videos for the given genre
        search_response = youtube.search().list(
            q=genre,
            part='id,snippet',
            type='video',
            maxResults=min(50, max_results - results_fetched),
            pageToken=next_page_token
        ).execute()

        for item in search_response['items']:
            video_id = item['id']['videoId']
            video_response = youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=video_id
            ).execute()

            for video in video_response['items']:
                # Extract data points
                snippet = video['snippet']
                content_details = video['contentDetails']
                statistics = video['statistics']

                captions_available = False
                captions_text = None
                try:
                    yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
                    caption = yt.captions.get_by_language_code('en')
                    if caption:
                        captions_available = True
                        captions_text = caption.generate_srt_captions()
                except:
                    captions_available = False

                video_data.append({
                    "Video URL": f"https://www.youtube.com/watch?v={video_id}",
                    "Title": snippet.get('title'),
                    "Description": snippet.get('description'),
                    "Channel Title": snippet.get('channelTitle'),
                    "Keyword Tags": snippet.get('tags', []),
                    "YouTube Video Category": snippet.get('categoryId'),
                    "Video Published At": snippet.get('publishedAt'),
                    "Video Duration": content_details.get('duration'),
                    "View Count": statistics.get('viewCount'),
                    "Comment Count": statistics.get('commentCount'),
                    "Captions Available": captions_available,
                    "Caption Text": captions_text
                })

        results_fetched += len(search_response['items'])
        next_page_token = search_response.get('nextPageToken')
        if not next_page_token:
            break

    # Save data to CSV
    df = pd.DataFrame(video_data)
    filename = f"{genre.replace(' ', '_')}_youtube_data.csv"
    df.to_csv(filename, index=False)
    return video_data
