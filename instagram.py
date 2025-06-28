import discord
from discord import app_commands, ui
import instagrapi
import os
import time
import logging
import requests
import io
import json
import itertools
from dotenv import load_dotenv
from typing import Optional, Tuple, List, Dict
from datetime import datetime, timezone, UTC

logging.basicConfig(filename="bot.log", level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))
INSTAGRAM_ACCOUNTS = [
    {"username": os.getenv("INSTAGRAM_USERNAME_1"), "password": os.getenv("INSTAGRAM_PASSWORD_1"), "session_file": "ig_session_1.json"},
    {"username": os.getenv("INSTAGRAM_USERNAME_2"), "password": os.getenv("INSTAGRAM_PASSWORD_2"), "session_file": "ig_session_2.json"},
    {"username": os.getenv("INSTAGRAM_USERNAME_3"), "password": os.getenv("INSTAGRAM_PASSWORD_3"), "session_file": "ig_session_3.json"},
]

INSTAGRAM_ACCOUNTS = [acc for acc in INSTAGRAM_ACCOUNTS if acc["username"] and acc["password"]]
if not INSTAGRAM_ACCOUNTS:
    logging.error("No valid Instagram accounts provided in .env")
    raise ValueError("No valid Instagram accounts provided in .env")

INSTAGRAM_POST_CACHE = {}
INSTAGRAM_STORY_CACHE = {}
CACHE_VALIDITY_SECONDS = 300
INSTAGRAM_USERNAMES_TO_MONITOR = ["avamax"]
LAST_IG_POST_FILE = "last_ig_post_shortcode_{}.json"
LAST_IG_STORY_FILE = "last_ig_story_{}.json"
LAST_FOLLOWER_COUNT_FILE = "last_follower_count_{}.txt"
DISCORD_FILE_SIZE_LIMIT = 8 * 1024 * 1024  

ig_clients = []
current_client_index = itertools.cycle(range(len(INSTAGRAM_ACCOUNTS)))

def get_next_client() -> Tuple[instagrapi.Client, str]:
    """Get the next Instagram client in the rotation."""
    index = next(current_client_index)
    return ig_clients[index], INSTAGRAM_ACCOUNTS[index]["username"]

def initialize_instagram_clients() -> None:
    """Initialize Instagram clients for each account."""
    for account in INSTAGRAM_ACCOUNTS:
        username = account["username"]
        password = account["password"]
        session_file = account["session_file"]
        try:
            client = instagrapi.Client()
            if os.path.exists(session_file):
                client.load_settings(session_file)
                logging.info(f"Loaded Instagram session for {username} from {session_file}")
            else:
                client.login(username, password)
                with open(session_file, 'w') as f:
                    json.dump(client.get_settings(), f)
                logging.info(f"Saved Instagram session for {username} to {session_file}")
            ig_clients.append(client)
        except Exception as e:
            logging.error(f"Instagram authentication failed for {username}: {e}")
            print(f"Instagram authentication failed for {username}: {e}")

initialize_instagram_clients()
if not ig_clients:
    logging.error("No Instagram clients initialized successfully")
    raise ValueError("No Instagram clients initialized successfully")

def load_last_ig_post_shortcode(username: str) -> Dict:
    """Load the last Instagram post shortcode history for a user."""
    file = LAST_IG_POST_FILE.format(username)
    try:
        with open(file, "r") as f:
            data = json.load(f)
            if not isinstance(data, dict) or "posts" not in data:
                data = {"latest_post": {}, "posts": []}
            logging.debug(f"Loaded Instagram post shortcode history for {username}: {data}")
            return data
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        logging.warning(f"No valid Instagram post shortcode history found for {username}, starting fresh")
        return {"latest_post": {}, "posts": []}

def save_last_ig_post_shortcode(
    username: str,
    shortcode: str,
    timestamp: Optional[str],
    channel_id: Optional[int] = None,
    message_id: Optional[int] = None,
    marked_deleted: bool = False,
    deleted_at: Optional[str] = None,
    like_count: Optional[int] = None,
    comment_count: Optional[int] = None
) -> None:
    """Save the Instagram post shortcode history for a user."""
    file = LAST_IG_POST_FILE.format(username)
    history = load_last_ig_post_shortcode(username)
    posts = history.get("posts", [])
    for entry in posts:
        if entry["shortcode"] == str(shortcode):
            if channel_id and str(channel_id) not in entry["channel_ids"]:
                entry["channel_ids"].append(str(channel_id))
                if message_id:
                    entry["message_ids"][str(channel_id)] = str(message_id)
            entry["marked_deleted"] = marked_deleted
            if deleted_at:
                entry["deleted_at"] = deleted_at
            if like_count is not None:
                entry["like_count"] = like_count
            if comment_count is not None:
                entry["comment_count"] = comment_count
            break
    else:
        new_entry = {
            "shortcode": str(shortcode),
            "channel_ids": [str(channel_id)] if channel_id else [],
            "message_ids": {str(channel_id): str(message_id)} if channel_id and message_id else {},
            "timestamp": timestamp or "1970-01-01 00:00:00 UTC",
            "marked_deleted": marked_deleted,
            "deleted_at": deleted_at if deleted_at else None,
            "like_count": like_count if like_count is not None else None,
            "comment_count": comment_count if comment_count is not None else None
        }
        posts.append(new_entry)
    latest_post = history.get("latest_post", {})
    if not latest_post or (timestamp and timestamp > latest_post.get("timestamp", "1970-01-01 00:00:00 UTC")):
        history["latest_post"] = {
            "shortcode": str(shortcode),
            "timestamp": timestamp or "1970-01-01 00:00:00 UTC"
        }
    history["posts"] = posts
    try:
        with open(file, "w") as f:
            json.dump(history, f, indent=4)
        logging.debug(f"Saved Instagram post shortcode history for {username}: {history}")
    except Exception as e:
        logging.error(f"Error saving Instagram post shortcode history for {username}: {e}")
        print(f"Error saving Instagram post shortcode history for {username}: {e}")

def load_last_ig_story(username: str) -> Dict:
    """Load the last Instagram story history for a user."""
    file = LAST_IG_STORY_FILE.format(username)
    try:
        with open(file, "r") as f:
            data = json.load(f)
            if not isinstance(data, dict) or "stories" not in data:
                data = {"latest_story": {}, "stories": []}
            logging.debug(f"Loaded Instagram story history for {username}: {data}")
            return data
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        logging.warning(f"No valid Instagram story history found for {username}, starting fresh")
        return {"latest_story": {}, "stories": []}

def save_last_ig_story(
    username: str,
    story_id: str,
    timestamp: Optional[str],
    channel_id: Optional[int] = None,
    message_id: Optional[int] = None,
    expired: bool = False,
    expired_at: Optional[str] = None
) -> None:
    """Save the Instagram story history for a user."""
    file = LAST_IG_STORY_FILE.format(username)
    history = load_last_ig_story(username)
    stories = history.get("stories", [])
    for entry in stories:
        if entry["story_id"] == str(story_id):
            if channel_id and str(channel_id) not in entry["channel_ids"]:
                entry["channel_ids"].append(str(channel_id))
                if message_id:
                    entry["message_ids"][str(channel_id)] = str(message_id)
            entry["expired"] = expired
            if expired_at:
                entry["expired_at"] = expired_at
            break
    else:
        new_entry = {
            "story_id": str(story_id),
            "channel_ids": [str(channel_id)] if channel_id else [],
            "message_ids": {str(channel_id): str(message_id)} if channel_id and message_id else {},
            "timestamp": timestamp or "1970-01-01 00:00:00 UTC",
            "expired": expired,
            "expired_at": expired_at if expired_at else None
        }
        stories.append(new_entry)
    latest_story = history.get("latest_story", {})
    if not latest_story or (timestamp and timestamp > latest_story.get("timestamp", "1970-01-01 00:00:00 UTC")):
        history["latest_story"] = {
            "story_id": str(story_id),
            "timestamp": timestamp or "1970-01-01 00:00:00 UTC"
        }
    history["stories"] = stories
    try:
        with open(file, "w") as f:
            json.dump(history, f, indent=4)
        logging.debug(f"Saved Instagram story history for {username}: {history}")
    except Exception as e:
        logging.error(f"Error saving Instagram story history for {username}: {e}")
        print(f"Error saving Instagram story history for {username}: {e}")

def load_last_follower_count(username: str) -> Optional[int]:
    """Load the last follower count for a user."""
    file = LAST_FOLLOWER_COUNT_FILE.format(username)
    try:
        with open(file, "r") as f:
            count = int(f.read().strip())
            logging.debug(f"Loaded last follower count for {username}: {count}")
            return count
    except (FileNotFoundError, ValueError):
        logging.warning(f"No valid last follower count found for {username}, starting fresh")
        return None

def save_last_follower_count(username: str, count: int) -> None:
    """Save the last follower count for a user."""
    file = LAST_FOLLOWER_COUNT_FILE.format(username)
    try:
        with open(file, "w") as f:
            f.write(str(count))
        logging.debug(f"Saved last follower count for {username}: {count}")
    except Exception as e:
        logging.error(f"Error saving follower count for {username}: {e}")
        print(f"Error saving follower count for {username}: {e}")

def download_profile_picture(user, username: str, retries: int = 3) -> Tuple[Optional[io.BytesIO], Optional[str], str]:
    """Download the profile picture for a user."""
    profile_pic_url = str(getattr(user, 'profile_pic_url_hd', user.profile_pic_url))
    for attempt in range(retries):
        try:
            ig_client, ig_username = get_next_client()
            logging.debug(f"Attempting to download profile picture for {username} using {ig_username} (attempt {attempt + 1}/{retries})")
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            cookies = ig_client.get_settings().get('cookies', {})
            response = requests.get(profile_pic_url, headers=headers, cookies=cookies)
            response.raise_for_status()
            filename = f"profile_{username}.jpg"
            logging.info(f"Successfully downloaded profile picture for {username}: {filename}")
            return io.BytesIO(response.content), filename, profile_pic_url
        except Exception as e:
            logging.error(f"Error downloading profile picture for {username} (attempt {attempt + 1}): {e}")
            print(f"Error downloading profile picture for {username} (attempt {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt * 10)
                continue
            logging.warning(f"Exhausted retries for profile picture download for {username}")
            return None, None, profile_pic_url
    return None, None, profile_pic_url

def download_instagram_media(post_url: str, media, retries: int = 5) -> Tuple[List[Tuple[io.BytesIO, str]], List[str]]:
    """Download media for an Instagram post or story."""
    media_items = []
    for attempt in range(retries):
        try:
            ig_client, username = get_next_client()
            logging.debug(f"Attempting instagrapi media fetch for {post_url} using {username} (attempt {attempt + 1}/{retries}, media_type: {media.media_type})")
            if media.media_type == 8:  # Carousel (for posts)
                media_info = ig_client.media_info(media.pk)
                logging.debug(f"Media info structure: {vars(media_info)}")
                resources = getattr(media_info, 'resources', getattr(media_info, 'carousel_media', []))
                if not resources:
                    logging.warning(f"No resources or carousel_media found for carousel post {post_url} (attempt {attempt + 1})")
                    return [], []
                logging.debug(f"Found {len(resources)} resources for {post_url}")
                for idx, resource in enumerate(resources):
                    try:
                        logging.debug(f"Resource {idx+1} details: {vars(resource)}")
                        media_url = None
                        extension = None
                        if resource.media_type == 1:
                            if hasattr(resource, 'image_versions2') and resource.image_versions2 and resource.image_versions2.get('candidates'):
                                media_url = str(resource.image_versions2['candidates'][0]['url'])
                                extension = '.jpg'
                            elif hasattr(resource, 'thumbnail_url') and resource.thumbnail_url:
                                media_url = str(resource.thumbnail_url)
                                extension = '.jpg'
                            else:
                                resource_info = ig_client.media_info(resource.pk)
                                logging.debug(f"Resource {idx+1} re-fetched info: {vars(resource_info)}")
                                if hasattr(resource_info, 'image_versions2') and resource_info.image_versions2 and resource_info.image_versions2.get('candidates'):
                                    media_url = str(resource_info.image_versions2['candidates'][0]['url'])
                                    extension = '.jpg'
                                elif hasattr(resource_info, 'thumbnail_url') and resource_info.thumbnail_url:
                                    media_url = str(resource_info.thumbnail_url)
                                    extension = '.jpg'
                                else:
                                    logging.warning(f"Skipping resource {idx+1} in {post_url}: No valid image URL (media_type: 1)")
                                    continue
                        elif resource.media_type == 2:
                            if hasattr(resource, 'video_versions') and resource.video_versions:
                                media_url = str(resource.video_versions[0].url)
                                extension = '.mp4'
                            elif hasattr(resource, 'video_url') and resource.video_url:
                                media_url = str(resource.video_url)
                                extension = '.mp4'
                            else:
                                resource_info = ig_client.media_info(resource.pk)
                                logging.debug(f"Resource {idx+1} re-fetched info: {vars(resource_info)}")
                                if hasattr(resource_info, 'video_versions') and resource_info.video_versions:
                                    media_url = str(resource_info.video_versions[0].url)
                                    extension = '.mp4'
                                elif hasattr(resource_info, 'video_url') and resource_info.video_url:
                                    media_url = str(resource_info.video_url)
                                    extension = '.mp4'
                                elif hasattr(resource_info, 'thumbnail_url') and resource_info.thumbnail_url:
                                    media_url = str(resource_info.thumbnail_url)
                                    extension = '.jpg'
                                    logging.info(f"Falling back to thumbnail for video resource {idx+1} in {post_url}")
                                else:
                                    logging.warning(f"Skipping resource {idx+1} in {post_url}: No valid video URL (media_type: 2)")
                                    continue
                        else:
                            logging.warning(f"Skipping resource {idx+1} in {post_url}: Unsupported media type (media_type: {getattr(resource, 'media_type', 'unknown')})")
                            continue
                        headers = {
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                        }
                        cookies = ig_client.get_settings().get('cookies', {})
                        response = requests.get(media_url, headers=headers, cookies=cookies)
                        response.raise_for_status()
                        filename = f"instagram_{post_url.split('/')[-2]}_{idx+1}{extension}"
                        media_data = io.BytesIO(response.content)
                        file_size = len(media_data.getvalue())
                        logging.info(f"Downloaded media {idx+1} for {post_url}: {filename}, size: {file_size} bytes")
                        if file_size > DISCORD_FILE_SIZE_LIMIT:
                            logging.warning(f"Media {filename} exceeds Discord file size limit ({DISCORD_FILE_SIZE_LIMIT} bytes)")
                            continue
                        media_items.append((media_data, filename))
                        logging.info(f"Successfully downloaded media {idx+1} for {post_url}: {filename}")
                    except requests.RequestException as e:
                        logging.error(f"Error downloading resource {idx+1} for {post_url}: {e}")
                        continue
                time.sleep(5)
                return media_items, [item[1] for item in media_items]
            else:  # Single photo or video (for posts or stories)
                logging.debug(f"Media details: {vars(media)}")
                media_url = None
                extension = None
                if media.media_type == 2:
                    if hasattr(media, 'video_versions') and media.video_versions:
                        media_url = str(media.video_versions[0].url)
                        extension = '.mp4'
                    elif hasattr(media, 'video_url') and media.video_url:
                        media_url = str(media.video_url)
                        extension = '.mp4'
                    else:
                        media_info = ig_client.media_info(media.pk)
                        logging.debug(f"Media re-fetched info: {vars(media_info)}")
                        if hasattr(media_info, 'video_versions') and media_info.video_versions:
                            media_url = str(media_info.video_versions[0].url)
                            extension = '.mp4'
                        elif hasattr(media_info, 'video_url') and media_info.video_url:
                            media_url = str(media_info.video_url)
                            extension = '.mp4'
                        elif hasattr(media_info, 'thumbnail_url') and media_info.thumbnail_url:
                            media_url = str(media_info.thumbnail_url)
                            extension = '.jpg'
                            logging.info(f"Falling back to thumbnail for video media {post_url}")
                        else:
                            logging.warning(f"No valid video URL for {post_url} (media_type: 2)")
                            return [], []
                elif media.media_type == 1:
                    if hasattr(media, 'image_versions2') and media.image_versions2 and media.image_versions2.get('candidates'):
                        media_url = str(media.image_versions2['candidates'][0]['url'])
                        extension = '.jpg'
                    elif hasattr(media, 'thumbnail_url') and media.thumbnail_url:
                        media_url = str(media.thumbnail_url)
                        extension = '.jpg'
                    else:
                        media_info = ig_client.media_info(media.pk)
                        logging.debug(f"Media re-fetched info: {vars(media_info)}")
                        if hasattr(media_info, 'image_versions2') and media_info.image_versions2 and media_info.image_versions2.get('candidates'):
                            media_url = str(media_info.image_versions2['candidates'][0]['url'])
                            extension = '.jpg'
                        elif hasattr(media_info, 'thumbnail_url') and media_info.thumbnail_url:
                            media_url = str(media_info.thumbnail_url)
                            extension = '.jpg'
                        else:
                            logging.warning(f"No valid image URL for {post_url} (media_type: 1)")
                            return [], []
                if not media_url:
                    logging.warning(f"Unsupported media type {media.media_type} or no media found for {post_url} (attempt {attempt + 1})")
                    return [], []
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                }
                cookies = ig_client.get_settings().get('cookies', {})
                response = requests.get(media_url, headers=headers, cookies=cookies)
                response.raise_for_status()
                filename = f"instagram_{post_url.split('/')[-2]}{extension}"
                media_data = io.BytesIO(response.content)
                file_size = len(media_data.getvalue())
                logging.info(f"Downloaded media for {post_url}: {filename}, size: {file_size} bytes")
                if file_size > DISCORD_FILE_SIZE_LIMIT:
                    logging.warning(f"Media {filename} exceeds Discord file size limit ({DISCORD_FILE_SIZE_LIMIT} bytes)")
                    return [], []
                time.sleep(5)
                return [(media_data, filename)], [filename]
        except Exception as e:
            if str(e).startswith("429"):
                logging.warning(f"Instagram rate limit hit for {post_url} with {username}, switching account")
                print(f"Instagram rate limit hit for {post_url} with {username}, switching account")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt * 10)
                    continue
            logging.error(f"Error downloading media for {post_url} (attempt {attempt + 1}): {e}")
            print(f"Error downloading media for {post_url} (attempt {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt * 10)
                continue
            logging.warning(f"Exhausted retries for media fetch for {post_url}")
            return [], []
    return [], []

async def fetch_instagram_post_for_user(username: str, channel_id: Optional[int] = None, retries: int = 3) -> Tuple[Optional[Dict], List]:
    """Fetch the first two non-pinned Instagram posts for a user, select the newer one if the second is more recent, and check for deleted posts."""
    shortcode_history = load_last_ig_post_shortcode(username)
    shortcode_list = [entry["shortcode"] for entry in shortcode_history.get("posts", [])]
    latest_post = shortcode_history.get("latest_post", {})
    latest_shortcode = latest_post.get("shortcode", "")
    latest_timestamp = latest_post.get("timestamp", "1970-01-01 00:00:00 UTC")
    for attempt in range(retries):
        try:
            ig_client, ig_username = get_next_client()
            logging.debug(f"Attempting to fetch Instagram posts for @{username} using {ig_username}, attempt {attempt + 1}/{retries}, shortcode_history: {shortcode_list}, latest_shortcode: {latest_shortcode}, latest_timestamp: {latest_timestamp}, channel_id: {channel_id}")
            user_id = ig_client.user_id_from_username(username)
            user = ig_client.user_info_by_username(username)
            profile_data, profile_filename, profile_pic_url = download_profile_picture(user, username)
            posts = ig_client.user_medias(user_id, amount=3) 
            logging.debug(f"Fetched {len(posts)} posts for @{username}")
            if not posts:
                logging.info(f"No Instagram posts found for @{username}")
                print(f"No Instagram posts found for @{username}")
                deleted_posts = [
                    {"entry": entry, "username": username} for entry in shortcode_history.get("posts", [])
                    if channel_id and str(channel_id) in entry["message_ids"]
                ]
                logging.debug(f"Potential deleted posts for @{username} (no posts fetched): {[entry['entry']['shortcode'] for entry in deleted_posts]}")
                if profile_data and profile_filename:
                    INSTAGRAM_POST_CACHE[username] = INSTAGRAM_POST_CACHE.get(username, {})
                    INSTAGRAM_POST_CACHE[username]["profile"] = {
                        "profile_data": io.BytesIO(profile_data.getvalue()),
                        "profile_filename": profile_filename,
                        "timestamp": time.time()
                    }
                return None, deleted_posts

            non_pinned_posts = []
            fetched_shortcodes = []
            for post in posts:
                post = ig_client.media_info(post.pk)
                if not hasattr(post, 'is_pinned') or not post.is_pinned:
                    logging.debug(f"Post {post.code} is not pinned, adding to non_pinned_posts")
                    non_pinned_posts.append(post)
                    fetched_shortcodes.append(post.code)
                else:
                    logging.debug(f"Skipping pinned post {post.code} with pinned icon")
            logging.debug(f"Fetched non-pinned shortcodes for @{username}: {fetched_shortcodes}")

            if not non_pinned_posts:
                logging.info(f"No non-pinned Instagram posts found for @{username}")
                print(f"No non-pinned Instagram posts found for @{username}")
                deleted_posts = [
                    {"entry": entry, "username": username} for entry in shortcode_history.get("posts", [])
                    if entry["shortcode"] in shortcode_list and channel_id and str(channel_id) in entry["message_ids"]
                ]
                logging.debug(f"Potential deleted posts for @{username} (no non-pinned posts): {[entry['entry']['shortcode'] for entry in deleted_posts]}")
                if profile_data and profile_filename:
                    INSTAGRAM_POST_CACHE[username] = INSTAGRAM_POST_CACHE.get(username, {})
                    INSTAGRAM_POST_CACHE[username]["profile"] = {
                        "profile_data": io.BytesIO(profile_data.getvalue()),
                        "profile_filename": profile_filename,
                        "timestamp": time.time()
                    }
                return None, deleted_posts

            deleted_posts = [
                {"entry": entry, "username": username} for entry in shortcode_history.get("posts", [])
                if entry["shortcode"] in shortcode_list and entry["shortcode"] not in fetched_shortcodes and channel_id and str(channel_id) in entry["message_ids"]
            ]
            if deleted_posts:
                logging.info(f"Detected deleted posts for @{username}: {[entry['entry']['shortcode'] for entry in deleted_posts]}")

            if len(non_pinned_posts) > 1:
                first_post, second_post = non_pinned_posts[:2]
                if second_post.taken_at > first_post.taken_at:
                    post = second_post
                    logging.debug(f"Selected newer second post for @{username}: {post.code} with timestamp {post.taken_at}")
                else:
                    post = first_post
                    logging.debug(f"Selected first post (not newer) for @{username}: {post.code} with timestamp {post.taken_at}")
            else:
                post = non_pinned_posts[0]  # Only one non-pinned post available
                logging.debug(f"Only one non-pinned post available for @{username}: {post.code}")

            post_timestamp = post.taken_at.strftime("%Y-%m-%d %H:%M:%S UTC") if post.taken_at else "1970-01-01 00:00:00 UTC"
            channel_ids = next((entry["channel_ids"] for entry in shortcode_history.get("posts", []) if entry["shortcode"] == post.code), [])
            
            if post.code not in shortcode_list or (channel_id and str(channel_id) not in channel_ids):
                save_last_ig_post_shortcode(
                    username=username,
                    shortcode=post.code,
                    timestamp=post_timestamp,
                    channel_id=None,
                    like_count=post.like_count,
                    comment_count=post.comment_count
                )
                logging.info(f"{'New post' if post.code not in shortcode_list else 'Existing post, new channel'} found for @{username}, shortcode: {post.code}, ID: {post.pk}, timestamp: {post_timestamp}, likes: {post.like_count}, comments: {post.comment_count}")
                post_url = f"https://www.instagram.com/p/{post.code}/"
                media_data_list, filename_list = download_instagram_media(post_url, post)
                for media_data, _ in media_data_list:
                    if media_data:
                        media_data.seek(0)
                if media_data_list and filename_list:
                    logging.info(f"Media downloaded for {post_url}: {filename_list}")
                else:
                    logging.warning(f"Failed to download media for post {post_url}")
                if profile_data and profile_filename:
                    INSTAGRAM_POST_CACHE[username] = INSTAGRAM_POST_CACHE.get(username, {})
                    INSTAGRAM_POST_CACHE[username]["profile"] = {
                        "profile_data": io.BytesIO(profile_data.getvalue()),
                        "profile_filename": profile_filename,
                        "timestamp": time.time()
                    }
                return {
                    "platform": "Instagram",
                    "type": "post",
                    "username": username,
                    "text": post.caption_text or "No caption",
                    "url": post_url,
                    "id": post.pk,
                    "shortcode": post.code,
                    "media_data_list": media_data_list,
                    "filename_list": filename_list,
                    "timestamp": post_timestamp,
                    "is_deleted_post": False,
                    "like_count": post.like_count,
                    "comment_count": post.comment_count,
                    "profile_filename": profile_filename,
                    "profile_data": profile_data
                }, deleted_posts
            logging.info(f"No new Instagram post for @{username} (shortcode: {post.code}, timestamp: {post_timestamp}, already processed for channel {channel_id}, channel_ids: {channel_ids})")
            print(f"No new Instagram post for @{username} (shortcode: {post.code}, timestamp: {post_timestamp}, already processed for channel {channel_id}, channel_ids: {channel_ids})")
            if profile_data and profile_filename:
                INSTAGRAM_POST_CACHE[username] = INSTAGRAM_POST_CACHE.get(username, {})
                INSTAGRAM_POST_CACHE[username]["profile"] = {
                    "profile_data": io.BytesIO(profile_data.getvalue()),
                    "profile_filename": profile_filename,
                    "timestamp": time.time()
                }
            return None, deleted_posts
        except Exception as e:
            if str(e).startswith("429"):
                logging.warning(f"Instagram rate limit hit for @{username} with {ig_username}, switching account")
                print(f"Instagram rate limit hit for @{username} with {ig_username}, switching account")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt * 10)
                    continue
            elif isinstance(e, KeyError) and 'data' in str(e):
                logging.error(f"KeyError: 'data' in Instagram API response for @{username}: {e}")
                print(f"KeyError: 'data' in Instagram API response for @{username}: {e}")
                try:
                    logging.debug(f"Raw API response: {ig_client.last_json}")
                except:
                    logging.debug("No last_json available")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt * 10)
                    continue
            else:
                logging.error(f"Error fetching Instagram posts for @{username}: {e}")
                print(f"Error fetching Instagram posts for @{username}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt * 10)
                    continue
            logging.warning(f"Exhausted retries for fetching Instagram posts for @{username}")
            return None, []
    return None, []

async def fetch_instagram_stories_for_user(username: str, channel_id: Optional[int] = None, retries: int = 3) -> List[Dict]:
    """Fetch active Instagram stories for a user."""
    story_history = load_last_ig_story(username)
    story_ids = [entry["story_id"] for entry in story_history.get("stories", [])]
    stories_output = []
    for attempt in range(retries):
        try:
            ig_client, ig_username = get_next_client()
            logging.debug(f"Attempting to fetch Instagram stories for @{username} using {ig_username}, attempt {attempt + 1}/{retries}, story_history: {story_ids}, channel_id: {channel_id}")
            user_id = ig_client.user_id_from_username(username)
            user = ig_client.user_info_by_username(username)
            profile_data, profile_filename, profile_pic_url = download_profile_picture(user, username)
            stories = ig_client.user_stories(user_id)
            logging.debug(f"Fetched {len(stories)} stories for @{username}")
            if not stories:
                logging.info(f"No active Instagram stories found for @{username}")
                print(f"No active Instagram stories found for @{username}")
                if profile_data and profile_filename:
                    INSTAGRAM_STORY_CACHE[username] = INSTAGRAM_STORY_CACHE.get(username, {})
                    INSTAGRAM_STORY_CACHE[username]["profile"] = {
                        "profile_data": io.BytesIO(profile_data.getvalue()),
                        "profile_filename": profile_filename,
                        "timestamp": time.time()
                    }
                return []

            fetched_story_ids = []
            for story in stories:
                story_id = str(story.pk)
                fetched_story_ids.append(story_id)
                story_timestamp = story.taken_at.strftime("%Y-%m-%d %H:%M:%S UTC") if story.taken_at else "1970-01-01 00:00:00 UTC"
                channel_ids = next((entry["channel_ids"] for entry in story_history.get("stories", []) if entry["story_id"] == story_id), [])

                if story_id not in story_ids or (channel_id and str(channel_id) not in channel_ids):
                    # Instagram stories don't have a direct URL, so use profile URL
                    story_url = f"https://www.instagram.com/stories/{username}/{story_id}/"
                    media_data_list, filename_list = download_instagram_media(story_url, story)
                    for media_data, _ in media_data_list:
                        if media_data:
                            media_data.seek(0)
                    if media_data_list and filename_list:
                        logging.info(f"Media downloaded for story {story_url}: {filename_list}")
                    else:
                        logging.warning(f"Failed to download media for story {story_url}")

                    save_last_ig_story(
                        username=username,
                        story_id=story_id,
                        timestamp=story_timestamp,
                        channel_id=None
                    )
                    logging.info(f"New story found for @{username}, story_id: {story_id}, timestamp: {story_timestamp}")
                    stories_output.append({
                        "platform": "Instagram",
                        "type": "story",
                        "username": username,
                        "text": getattr(story, 'caption_text', "No caption") or "No caption",
                        "url": story_url,
                        "id": story.pk,
                        "shortcode": story_id,
                        "media_data_list": media_data_list,
                        "filename_list": filename_list,
                        "timestamp": story_timestamp,
                        "is_deleted_post": False, 
                        "like_count": None, 
                        "comment_count": None,  
                        "profile_filename": profile_filename,
                        "profile_data": profile_data
                    })

            expired_stories = [
                {"entry": entry, "username": username} for entry in story_history.get("stories", [])
                if entry["story_id"] not in fetched_story_ids and channel_id and str(channel_id) in entry["message_ids"]
            ]
            if expired_stories:
                logging.info(f"Detected expired stories for @{username}: {[entry['entry']['story_id'] for entry in expired_stories]}")
                current_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
                for expired_story in expired_stories:
                    save_last_ig_story(
                        username=username,
                        story_id=expired_story["entry"]["story_id"],
                        timestamp=expired_story["entry"]["timestamp"],
                        channel_id=channel_id,
                        expired=True,
                        expired_at=current_utc
                    )

            if profile_data and profile_filename:
                INSTAGRAM_STORY_CACHE[username] = INSTAGRAM_STORY_CACHE.get(username, {})
                INSTAGRAM_STORY_CACHE[username]["profile"] = {
                    "profile_data": io.BytesIO(profile_data.getvalue()),
                    "profile_filename": profile_filename,
                    "timestamp": time.time()
                }
            return stories_output
        except Exception as e:
            if str(e).startswith("429"):
                logging.warning(f"Instagram rate limit hit for stories @{username} with {ig_username}, switching account")
                print(f"Instagram rate limit hit for stories @{username} with {ig_username}, switching account")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt * 10)
                    continue
            elif isinstance(e, KeyError) and 'data' in str(e):
                logging.error(f"KeyError: 'data' in Instagram API response for stories @{username}: {e}")
                print(f"KeyError: 'data' in Instagram API response for stories @{username}: {e}")
                try:
                    logging.debug(f"Raw API response: {ig_client.last_json}")
                except:
                    logging.debug("No last_json available")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt * 10)
                    continue
            else:
                logging.error(f"Error fetching Instagram stories for @{username}: {e}")
                print(f"Error fetching Instagram stories for @{username}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt * 10)
                    continue
            logging.warning(f"Exhausted retries for fetching Instagram stories for @{username}")
            return []
    return []

async def fetch_instagram_content(channel_id: Optional[int] = None) -> Tuple[List, List]:
    """Fetch Instagram posts and stories for monitored users."""
    posts = []
    deleted_posts = []
    stories = []
    current_time = time.time()
    
    for username in INSTAGRAM_USERNAMES_TO_MONITOR:
        # Fetch posts
        post, deleted = await fetch_instagram_post_for_user(username, channel_id=channel_id)
        if post:
            INSTAGRAM_POST_CACHE[username] = {
                "post": post,
                "timestamp": current_time
            }
            logging.debug(f"Cached new Instagram post for @{username}, shortcode: {post['shortcode']}, timestamp: {post['timestamp']}, is_deleted_post: {post['is_deleted_post']}")
            posts.append(post)
        if deleted:
            deleted_posts.extend(deleted)
            logging.debug(f"Collected deleted posts for @{username}: {[entry['entry']['shortcode'] for entry in deleted]}")
        
        # Fetch stories
        user_stories = await fetch_instagram_stories_for_user(username, channel_id=channel_id)
        if user_stories:
            INSTAGRAM_STORY_CACHE[username] = INSTAGRAM_STORY_CACHE.get(username, {})
            for story in user_stories:
                INSTAGRAM_STORY_CACHE[username][story["shortcode"]] = {
                    "story": story,
                    "timestamp": current_time
                }
                logging.debug(f"Cached new Instagram story for @{username}, story_id: {story['shortcode']}, timestamp: {story['timestamp']}")
                stories.append(story)
    
    return posts + stories, deleted_posts

async def userdetails_instagram(username: str = "avamax") -> Tuple[discord.Embed, Optional[discord.File]]:
    """Fetch Instagram user details for the userdetails command."""
    try:
        ig_client, ig_username = get_next_client()
        user = ig_client.user_info_by_username(username)
        last_follower_count = load_last_follower_count(username)
        current_follower_count = user.follower_count
        save_last_follower_count(username, current_follower_count)

        follower_change = None
        change_text = "No previous follower count available."
        if last_follower_count is not None:
            follower_change = current_follower_count - last_follower_count
            if follower_change > 0:
                change_text = f"Gained {follower_change} followers since last check."
            elif follower_change < 0:
                change_text = f"Lost {abs(follower_change)} followers since last check."
            else:
                change_text = "No change in follower count since last check."

        current_time = time.time()
        last_post_time = "No non-pinned posts found."
        last_post_id = "N/A"
        last_story_time = "No stories found."
        last_story_id = "N/A"
        if username in INSTAGRAM_POST_CACHE:
            cached = INSTAGRAM_POST_CACHE[username]
            if cached.get("post") and current_time - cached["timestamp"] < CACHE_VALIDITY_SECONDS:
                logging.info(f"Using cached post time for @{username}, shortcode: {cached['post']['shortcode']}")
                print(f"Using cached post time for @{username}")
                last_post_time = cached["post"]["timestamp"]
                last_post_id = cached["post"]["id"]
            else:
                logging.debug(f"Cache expired or empty for posts @{username}, fetching new post time")

        if username in INSTAGRAM_STORY_CACHE:
            cached_stories = INSTAGRAM_STORY_CACHE[username]
            for story_id, cached in cached_stories.items():
                if story_id != "profile" and cached.get("story") and current_time - cached["timestamp"] < CACHE_VALIDITY_SECONDS:
                    logging.info(f"Using cached story time for @{username}, story_id: {cached['story']['shortcode']}")
                    print(f"Using cached story time for @{username}")
                    last_story_time = cached["story"]["timestamp"]
                    last_story_id = cached["story"]["id"]
                    break

        if last_post_time == "No non-pinned posts found":
            posts = ig_client.user_medias(user.pk, amount=5)
            if posts:
                non_pinned_posts = []
                for post in posts:
                    post = ig_client.media_info(post.pk)
                    if not hasattr(post, 'is_pinned') or not post.is_pinned:
                        logging.debug(f"Post {post.code} is not pinned, adding to non_pinned_posts for @{username}")
                        non_pinned_posts.append(post)
                    else:
                        logging.debug(f"Skipping pinned post {post.code} with pinned icon for @{username}")
                
                if non_pinned_posts:
                    first_non_pinned_post = non_pinned_posts[0]
                    last_post_time = first_non_pinned_post.taken_at.strftime("%Y-%m-%d %H:%M:%S UTC") if first_non_pinned_post.taken_at else "Unknown"
                    last_post_id = first_non_pinned_post.pk
                    post_url = f"https://www.instagram.com/p/{first_non_pinned_post.code}/"
                    media_data_list, filename_list = download_instagram_media(post_url, first_non_pinned_post)
                    for media_data, _ in media_data_list:
                        if media_data:
                            media_data.seek(0)
                    INSTAGRAM_POST_CACHE[username] = {
                        "post": {
                            "platform": "Instagram",
                            "type": "post",
                            "username": username,
                            "text": first_non_pinned_post.caption_text or "No caption",
                            "url": post_url,
                            "id": first_non_pinned_post.pk,
                            "shortcode": first_non_pinned_post.code,
                            "media_data_list": media_data_list,
                            "filename_list": filename_list,
                            "timestamp": last_post_time,
                            "is_deleted_post": False,
                            "like_count": first_non_pinned_post.like_count,
                            "comment_count": first_non_pinned_post.comment_count
                        },
                        "timestamp": current_time
                    }
                    profile_data, profile_filename, _ = download_profile_picture(user, username)
                    if profile_data and profile_filename:
                        INSTAGRAM_POST_CACHE[username]["profile"] = {
                            "profile_data": io.BytesIO(profile_data.getvalue()),
                            "profile_filename": profile_filename,
                            "timestamp": current_time
                        }
                    logging.debug(f"Cached new post time for @{username}, shortcode: {first_non_pinned_post.code}")
                else:
                    logging.info(f"No non-pinned posts found for @{username}")
                    print(f"No non-pinned posts found for @{username}")
                    profile_data, profile_filename, _ = download_profile_picture(user, username)
                    if profile_data and profile_filename:
                        INSTAGRAM_POST_CACHE[username] = INSTAGRAM_POST_CACHE.get(username, {})
                        INSTAGRAM_POST_CACHE[username]["profile"] = {
                            "profile_data": io.BytesIO(profile_data.getvalue()),
                            "profile_filename": profile_filename,
                            "timestamp": current_time
                        }

        if last_story_time == "No stories found":
            stories = ig_client.user_stories(user.pk)
            if stories:
                first_story = stories[0]
                last_story_time = first_story.taken_at.strftime("%Y-%m-%d %H:%M:%S UTC") if first_story.taken_at else "Unknown"
                last_story_id = first_story.pk
                story_url = f"https://www.instagram.com/stories/{username}/{first_story.pk}/"
                media_data_list, filename_list = download_instagram_media(story_url, first_story)
                for media_data, _ in media_data_list:
                    if media_data:
                        media_data.seek(0)
                INSTAGRAM_STORY_CACHE[username] = INSTAGRAM_STORY_CACHE.get(username, {})
                INSTAGRAM_STORY_CACHE[username][first_story.pk] = {
                    "story": {
                        "platform": "Instagram",
                        "type": "story",
                        "username": username,
                        "text": getattr(first_story, 'caption_text', "No caption") or "No caption",
                        "url": story_url,
                        "id": first_story.pk,
                        "shortcode": str(first_story.pk),
                        "media_data_list": media_data_list,
                        "filename_list": filename_list,
                        "timestamp": last_story_time,
                        "is_deleted_post": False,
                        "like_count": None,
                        "comment_count": None
                    },
                    "timestamp": current_time
                }
                profile_data, profile_filename, _ = download_profile_picture(user, username)
                if profile_data and profile_filename:
                    INSTAGRAM_STORY_CACHE[username]["profile"] = {
                        "profile_data": io.BytesIO(profile_data.getvalue()),
                        "profile_filename": profile_filename,
                        "timestamp": current_time
                    }
                logging.debug(f"Cached new story time for @{username}, story_id: {first_story.pk}")
            else:
                logging.info(f"No stories found for @{username}")
                print(f"No stories found for @{username}")
                profile_data, profile_filename, _ = download_profile_picture(user, username)
                if profile_data and profile_filename:
                    INSTAGRAM_STORY_CACHE[username] = INSTAGRAM_STORY_CACHE.get(username, {})
                    INSTAGRAM_STORY_CACHE[username]["profile"] = {
                        "profile_data": io.BytesIO(profile_data.getvalue()),
                        "profile_filename": profile_filename,
                        "timestamp": current_time
                    }

        profile_data, profile_filename, _ = download_profile_picture(user, username)

        embed = discord.Embed(
            title=f"{username} | Instagram",
            color=0xC13584
        )
        embed.add_field(name="Full Name", value=user.full_name or "N/A", inline=True)
        embed.add_field(name="Followers", value=current_follower_count, inline=True)
        embed.add_field(name="Following", value=user.following_count, inline=True)
        embed.add_field(name="Bio", value=user.biography or "N/A", inline=False)
        embed.add_field(name="Follower Change", value=change_text, inline=False)
        embed.add_field(name="Last Post", value=last_post_time, inline=True)
        embed.add_field(name="Last Story", value=last_story_time, inline=True)
        embed.set_footer(text=f"Post ID: {last_post_id} | Story ID: {last_story_id}")

        file = None
        if profile_data and profile_filename:
            file = discord.File(profile_data, filename=profile_filename)
            embed.set_thumbnail(url=f"attachment://{profile_filename}")
            logging.info(f"Embedding profile picture for @{username}: {profile_filename}")

        logging.info(f"Prepared user details for @{username}")
        return embed, file
    except Exception as e:
        logging.error(f"Error fetching user details for @{username}: {e}")
        print(f"Error fetching user details for @{username}: {e}")
        raise