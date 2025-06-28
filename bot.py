import discord
from discord import app_commands, ui
from discord.ext import tasks
import os
from dotenv import load_dotenv
import logging
import requests
import io
import time
from datetime import datetime, timezone, UTC, timedelta
from typing import Optional
from instagram import fetch_instagram_content, INSTAGRAM_POST_CACHE, INSTAGRAM_STORY_CACHE, save_last_ig_post_shortcode, save_last_ig_story, load_last_ig_story, userdetails_instagram

logging.basicConfig(filename="bot.log", level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
if not DISCORD_TOKEN:
    logging.error("DISCORD_TOKEN is not set in .env")
    raise ValueError("DISCORD_TOKEN is not set in .env")

CHECK_INTERVAL = 60
AUTO_POST_CHANNEL_FILE = "auto_post_channel.txt"
INSTAGRAM_LOGO_PATH = os.path.join(os.path.dirname(__file__), "instagram.png")  # Path to instagram.png
DISCORD_FILE_SIZE_LIMIT = 8 * 1024 * 1024  # 8MB default for free servers
STORY_EXPIRATION_HOURS = 24  # Instagram stories expire after 24 hours

intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

class PostView(ui.View):
    def __init__(self, url: str, content_type: str):
        super().__init__(timeout=None)
        self.add_item(ui.Button(label=f"View {content_type.capitalize()}", url=url, style=discord.ButtonStyle.link))

def load_auto_post_channel() -> Optional[int]:
    """Load the auto-post channel ID."""
    try:
        with open(AUTO_POST_CHANNEL_FILE, "r") as f:
            channel_id = f.read().strip()
            if channel_id:
                channel_id = int(channel_id)
                logging.debug(f"Loaded auto-post channel ID: {channel_id}")
                return channel_id
            return None
    except (FileNotFoundError, ValueError):
        logging.warning("No valid auto-post channel ID found, starting fresh")
        return None

def save_auto_post_channel(channel_id: Optional[int]) -> None:
    """Save the auto-post channel ID."""
    try:
        with open(AUTO_POST_CHANNEL_FILE, "w") as f:
            f.write(str(channel_id) if channel_id else "")
        logging.debug(f"Saved auto-post channel ID: {channel_id}")
    except Exception as e:
        logging.error(f"Error saving auto-post channel ID: {e}")
        print(f"Error saving auto-post channel ID: {e}")

@tasks.loop(seconds=CHECK_INTERVAL)
async def check_social_posts():
    """Periodically check for new Instagram posts and stories, and update deleted/expired content."""
    auto_post_channel_id: Optional[int] = load_auto_post_channel()
    if not auto_post_channel_id:
        logging.info("No auto-post channel set, skipping check_social_posts")
        print("No auto-post channel set, skipping check_social_posts")
        return

    channel = bot.get_channel(auto_post_channel_id)
    if not channel:
        logging.error(f"Error: Auto-post channel {auto_post_channel_id} not found")
        print(f"Error: Auto-post channel {auto_post_channel_id} not found")
        return

    content_items, deleted_posts = await fetch_instagram_content(channel_id=auto_post_channel_id)
    
    current_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    instagram_logo = discord.File(INSTAGRAM_LOGO_PATH, filename="instagram.png") if os.path.exists(INSTAGRAM_LOGO_PATH) else None
    if not instagram_logo:
        logging.warning(f"Instagram logo file not found at {INSTAGRAM_LOGO_PATH}")

    for deleted_post in deleted_posts:
        shortcode = deleted_post["entry"]["shortcode"]
        username = deleted_post["username"]
        message_id = deleted_post["entry"]["message_ids"].get(str(auto_post_channel_id))
        logging.debug(f"Processing deleted post {shortcode} for @{username}, message_id: {message_id}, channel_id: {auto_post_channel_id}")
        if message_id:
            try:
                message = await channel.fetch_message(int(message_id))
                if message.embeds and len(message.embeds) > 0 and message.embeds[0].description and "**Deleted Post**: This post has been deleted." in message.embeds[0].description:
                    logging.debug(f"Message {message_id} for {shortcode} already marked as deleted, skipping edit")
                    continue
                embed = message.embeds[0] if message.embeds and len(message.embeds) > 0 else discord.Embed(
                    title="Deleted Instagram Post",
                    color=0xC13584
                )
                original_caption = embed.description if message.embeds and len(message.embeds) > 0 and embed.description and "**Deleted Post**: This post has been deleted." not in embed.description else "No caption"
                deletion_notice = "\n\n**Deleted Post**: This post has been deleted."
                embed.description = f"{original_caption}{deletion_notice}"
                posted_at = deleted_post["entry"]["timestamp"]
                post_id = deleted_post["entry"]["message_ids"].get(str(auto_post_channel_id), "N/A")
                posted_at_discord = posted_at or "Unknown"
                if posted_at and isinstance(posted_at, str):
                    try:
                        posted_at_dt = datetime.strptime(posted_at, "%Y-%m-%d %H:%M:%S UTC")
                        posted_at_discord = f"<t:{int(posted_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                    except ValueError as e:
                        logging.error(f"Invalid posted_at timestamp format for {shortcode}: {posted_at}, error: {e}")
                        posted_at_discord = posted_at or "Unknown"
                else:
                    logging.warning(f"posted_at is None or not a string for {shortcode}: {posted_at}")
                    posted_at_discord = "Unknown"

                deleted_at = deleted_post["entry"].get("deleted_at", current_utc)
                deleted_at_discord = deleted_at
                if deleted_at and isinstance(deleted_at, str):
                    try:
                        deleted_at_dt = datetime.strptime(deleted_at, "%Y-%m-%d %H:%M:%S UTC")
                        deleted_at_discord = f"<t:{int(deleted_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                    except ValueError as e:
                        logging.error(f"Invalid deleted_at timestamp format for {shortcode}: {deleted_at}, error: {e}")
                        deleted_at_discord = deleted_at or current_utc
                else:
                    logging.warning(f"deleted_at is None or not a string for {shortcode}: {deleted_at}, using current_utc")
                    deleted_at_discord = f"<t:{int(datetime.strptime(current_utc, '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc).timestamp())}:F>"

                like_count = deleted_post["entry"].get("like_count", None)
                comment_count = deleted_post["entry"].get("comment_count", None)
                if like_count is None or comment_count is None:
                    cached_post = INSTAGRAM_POST_CACHE.get(username, {}).get("post", {})
                    if cached_post.get("shortcode") == shortcode:
                        like_count = cached_post.get("like_count", "Unknown")
                        comment_count = cached_post.get("comment_count", "Unknown")
                    else:
                        like_count = "Unknown"
                        comment_count = "Unknown"

                embed.clear_fields()
                embed.add_field(name="Post ID", value=post_id, inline=True)
                embed.add_field(name="Shortcode", value=shortcode, inline=True)
                embed.add_field(name="Posted At", value=posted_at_discord, inline=True)
                embed.add_field(name="Deleted At", value=deleted_at_discord, inline=True)
                embed.add_field(name="Likes", value=like_count, inline=True)
                embed.add_field(name="Comments", value=comment_count, inline=True)

                embed.set_author(name=f"@{username} | Instagram", icon_url="attachment://instagram.png" if instagram_logo else None)

                files = [instagram_logo] if instagram_logo else []
                profile_data = None
                profile_filename = None
                if username in INSTAGRAM_POST_CACHE and "profile" in INSTAGRAM_POST_CACHE[username]:
                    cached_profile = INSTAGRAM_POST_CACHE[username]["profile"]
                    if time.time() - cached_profile["timestamp"] < 300:
                        profile_data = io.BytesIO(cached_profile["profile_data"].getvalue())
                        profile_filename = cached_profile["profile_filename"]
                        files.append(discord.File(profile_data, filename=profile_filename))
                        embed.set_thumbnail(url=f"attachment://{profile_filename}")
                        logging.debug(f"Using cached profile picture for deleted post {shortcode}: {profile_filename}")

                if message.attachments:
                    for attachment in message.attachments:
                        if attachment.filename.lower().endswith(('.jpg', '.mp4')):
                            response = requests.get(attachment.url)
                            response.raise_for_status()
                            file_size = len(response.content)
                            if file_size <= DISCORD_FILE_SIZE_LIMIT:
                                files.append(discord.File(io.BytesIO(response.content), filename=attachment.filename))
                                logging.debug(f"Retaining attachment for deleted post {shortcode}: {attachment.filename}")
                            else:
                                logging.warning(f"Attachment {attachment.filename} for deleted post {shortcode} exceeds Discord file size limit ({DISCORD_FILE_SIZE_LIMIT} bytes)")
                await message.edit(content="", embed=embed, attachments=files, view=None)
                save_last_ig_post_shortcode(
                    username=username,
                    shortcode=shortcode,
                    timestamp=posted_at or "1970-01-01 00:00:00 UTC",
                    channel_id=auto_post_channel_id,
                    message_id=message_id,
                    marked_deleted=True,
                    deleted_at=deleted_at or current_utc,
                    like_count=like_count if like_count != "Unknown" else None,
                    comment_count=comment_count if comment_count != "Unknown" else None
                )
                logging.info(f"Edited message {message_id} in channel {auto_post_channel_id} for deleted post {shortcode} with deletion notice")
                print(f"Edited message {message_id} in channel {auto_post_channel_id} for deleted post {shortcode}")
            except discord.NotFound:
                logging.warning(f"Message {message_id} for deleted post {shortcode} not found in channel {auto_post_channel_id}")
                print(f"Message {message_id} for deleted post {shortcode} not found in channel {auto_post_channel_id}")
            except discord.errors.HTTPException as e:
                logging.error(f"HTTP error editing message {message_id} for deleted post {shortcode}: {e}")
                print(f"HTTP error editing message {message_id} for deleted post {shortcode}: {e}")
            except Exception as e:
                logging.error(f"Error editing message {message_id} for deleted post {shortcode}: {e}")
                print(f"Error editing message {message_id} for deleted post {shortcode}: {e}")

    for username in INSTAGRAM_STORY_CACHE:
        story_history = load_last_ig_story(username)
        for story_entry in story_history.get("stories", []):
            story_id = story_entry["story_id"]
            message_id = story_entry["message_ids"].get(str(auto_post_channel_id))
            if message_id and story_entry.get("expired"):
                try:
                    message = await channel.fetch_message(int(message_id))
                    if message.embeds and len(message.embeds) > 0 and message.embeds[0].description and "**Expired Story**: This story has expired." in message.embeds[0].description:
                        logging.debug(f"Message {message_id} for story {story_id} already marked as expired, skipping edit")
                        continue
                    embed = message.embeds[0] if message.embeds and len(message.embeds) > 0 else discord.Embed(
                        title="Expired Instagram Story",
                        color=0xC13584
                    )
                    expiration_notice = "**Expired Story**: This story has expired."
                    embed.description = expiration_notice
                    posted_at = story_entry["timestamp"]
                    story_id_field = story_id
                    posted_at_discord = posted_at or "Unknown"
                    if posted_at and isinstance(posted_at, str):
                        try:
                            posted_at_dt = datetime.strptime(posted_at, "%Y-%m-%d %H:%M:%S UTC")
                            posted_at_discord = f"<t:{int(posted_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                        except ValueError as e:
                            logging.error(f"Invalid posted_at timestamp format for story {story_id}: {posted_at}, error: {e}")
                            posted_at_discord = posted_at or "Unknown"
                    else:
                        logging.warning(f"posted_at is None or not a string for story {story_id}: {posted_at}")
                        posted_at_discord = "Unknown"

                    expired_at = story_entry.get("expired_at", current_utc)
                    expired_at_discord = expired_at
                    if expired_at and isinstance(expired_at, str):
                        try:
                            expired_at_dt = datetime.strptime(expired_at, "%Y-%m-%d %H:%M:%S UTC")
                            expired_at_discord = f"<t:{int(expired_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                        except ValueError as e:
                            logging.error(f"Invalid expired_at timestamp format for story {story_id}: {expired_at}, error: {e}")
                            expired_at_discord = expired_at or current_utc
                    else:
                        logging.warning(f"expired_at is None or not a string for story {story_id}: {expired_at}, using current_utc")
                        expired_at_discord = f"<t:{int(datetime.strptime(current_utc, '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc).timestamp())}:F>"

                    embed.clear_fields()
                    embed.add_field(name="Story ID", value=story_id_field, inline=True)
                    embed.add_field(name="Posted At", value=posted_at_discord, inline=True)
                    embed.add_field(name="Expired At", value=expired_at_discord, inline=True)

                    embed.set_author(name=f"@{username} | Instagram", icon_url="attachment://instagram.png" if instagram_logo else None)

                    files = [instagram_logo] if instagram_logo else []
                    profile_data = None
                    profile_filename = None
                    if username in INSTAGRAM_STORY_CACHE and "profile" in INSTAGRAM_STORY_CACHE[username]:
                        cached_profile = INSTAGRAM_STORY_CACHE[username]["profile"]
                        if time.time() - cached_profile["timestamp"] < 300:
                            profile_data = io.BytesIO(cached_profile["profile_data"].getvalue())
                            profile_filename = cached_profile["profile_filename"]
                            files.append(discord.File(profile_data, filename=profile_filename))
                            embed.set_thumbnail(url=f"attachment://{profile_filename}")
                            logging.debug(f"Using cached profile picture for expired story {story_id}: {profile_filename}")

                    if message.attachments:
                        for attachment in message.attachments:
                            if attachment.filename.lower().endswith(('.jpg', '.mp4')):
                                response = requests.get(attachment.url)
                                response.raise_for_status()
                                file_size = len(response.content)
                                if file_size <= DISCORD_FILE_SIZE_LIMIT:
                                    files.append(discord.File(io.BytesIO(response.content), filename=attachment.filename))
                                    logging.debug(f"Retaining attachment for expired story {story_id}: {attachment.filename}")
                                else:
                                    logging.warning(f"Attachment {attachment.filename} for expired story {story_id} exceeds Discord file size limit ({DISCORD_FILE_SIZE_LIMIT} bytes)")
                    await message.edit(content="", embed=embed, attachments=files, view=None)
                    logging.info(f"Edited message {message_id} in channel {auto_post_channel_id} for expired story {story_id} with expiration notice")
                    print(f"Edited message {message_id} in channel {auto_post_channel_id} for expired story {story_id}")
                except discord.NotFound:
                    logging.warning(f"Message {message_id} for expired story {story_id} not found in channel {auto_post_channel_id}")
                    print(f"Message {message_id} for expired story {story_id} not found in channel {auto_post_channel_id}")
                except discord.errors.HTTPException as e:
                    logging.error(f"HTTP error editing message {message_id} for expired story {story_id}: {e}")
                    print(f"HTTP error editing message {message_id} for expired story {story_id}: {e}")
                except Exception as e:
                    logging.error(f"Error editing message {message_id} for expired story {story_id}: {e}")
                    print(f"Error editing message {message_id} for expired story {story_id}: {e}")

    if not content_items:
        logging.info("No new Instagram posts or stories found for auto-post")
        print("No new Instagram posts or stories found for auto-post")
        return

    for item in content_items:
        instagram_logo = discord.File(INSTAGRAM_LOGO_PATH, filename="instagram.png") if os.path.exists(INSTAGRAM_LOGO_PATH) else None
        if not instagram_logo:
            logging.warning(f"Instagram logo file not found at {INSTAGRAM_LOGO_PATH}")
        content_type = item['type']
        identifier = item['shortcode']
        view = PostView(item['url'], content_type)
        if item['media_data_list'] and item['filename_list']:
            files = [instagram_logo] if instagram_logo else []
            embeds = []
            for idx, (media_data, filename) in enumerate(item['media_data_list']):
                file_size = len(media_data.getvalue())
                logging.debug(f"Processing {content_type} media {idx+1} for {item['url']}: {filename}, size: {file_size} bytes")
                if file_size > DISCORD_FILE_SIZE_LIMIT:
                    logging.warning(f"Media {filename} exceeds Discord file size limit ({DISCORD_FILE_SIZE_LIMIT} bytes)")
                    continue
                files.append(discord.File(io.BytesIO(media_data.getvalue()), filename=filename))
                embed = discord.Embed(
                    title=f"New Instagram {content_type.capitalize()}{' (Media ' + str(idx+1) + ')' if idx > 0 else ''}",
                    description=item['text'] if content_type == "post" and idx == 0 else "" if content_type == "story" else f"Additional media {idx+1} for {content_type}",
                    color=0xC13584
                )
                posted_at = item['timestamp']
                posted_at_discord = posted_at or "Unknown"
                if posted_at and isinstance(posted_at, str):
                    try:
                        posted_at_dt = datetime.strptime(posted_at, "%Y-%m-%d %H:%M:%S UTC")
                        posted_at_discord = f"<t:{int(posted_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                        if content_type == "story":
                            expires_at_dt = posted_at_dt + timedelta(hours=STORY_EXPIRATION_HOURS)
                            expires_at_discord = f"<t:{int(expires_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                    except ValueError as e:
                        logging.error(f"Invalid posted_at timestamp format for {identifier}: {posted_at}, error: {e}")
                        posted_at_discord = posted_at or "Unknown"
                        expires_at_discord = "Unknown" if content_type == "story" else None
                else:
                    logging.warning(f"posted_at is None or not a string for {identifier}: {posted_at}")
                    posted_at_discord = "Unknown"
                    expires_at_discord = "Unknown" if content_type == "story" else None

                like_count = item.get('like_count', "N/A" if content_type == "story" else "Unknown")
                comment_count = item.get('comment_count', "N/A" if content_type == "story" else "Unknown")

                embed.clear_fields()
                if content_type == "post":
                    embed.add_field(name="Post ID", value=item['id'], inline=True)
                    embed.add_field(name="Shortcode", value=identifier, inline=True)
                    embed.add_field(name="Posted At", value=posted_at_discord, inline=True)
                    embed.add_field(name="Likes", value=like_count, inline=True)
                    embed.add_field(name="Comments", value=comment_count, inline=True)
                else:  # Story
                    embed.add_field(name="Story ID", value=identifier, inline=True)
                    embed.add_field(name="Posted At", value=posted_at_discord, inline=True)
                    embed.add_field(name="Expires At", value=expires_at_discord, inline=True)

                embed.set_author(name=f"@{item['username']} | Instagram", icon_url="attachment://instagram.png" if instagram_logo else None)
                if item.get('profile_data') and item.get('profile_filename'):
                    profile_data = io.BytesIO(item['profile_data'].getvalue())
                    files.append(discord.File(profile_data, filename=item['profile_filename']))
                    embed.set_thumbnail(url=f"attachment://{item['profile_filename']}")
                embeds.append(embed)
            logging.info(f"Auto-posting Instagram {content_type} {identifier} with media {item['filename_list']} to channel {auto_post_channel_id}")
            try:
                message = await channel.send(content="", embeds=embeds, files=files, view=view)
                logging.info(f"Successfully sent message with {len(files)} files and {len(embeds)} embeds for {identifier}")
            except discord.errors.HTTPException as e:
                logging.error(f"Failed to send message for {identifier}: {e}")
                print(f"Failed to send message for {identifier}: {e}")
                continue
        else:
            embed = discord.Embed(
                title=f"New Instagram {content_type.capitalize()}",
                description=item['text'] if content_type == "post" else "",
                color=0xC13584
            )
            posted_at = item['timestamp']
            posted_at_discord = posted_at or "Unknown"
            if posted_at and isinstance(posted_at, str):
                try:
                    posted_at_dt = datetime.strptime(posted_at, "%Y-%m-%d %H:%M:%S UTC")
                    posted_at_discord = f"<t:{int(posted_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                    if content_type == "story":
                        expires_at_dt = posted_at_dt + timedelta(hours=STORY_EXPIRATION_HOURS)
                        expires_at_discord = f"<t:{int(expires_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                except ValueError as e:
                    logging.error(f"Invalid posted_at timestamp format for {identifier}: {posted_at}, error: {e}")
                    posted_at_discord = posted_at or "Unknown"
                    expires_at_discord = "Unknown" if content_type == "story" else None
            else:
                logging.warning(f"posted_at is None or not a string for {identifier}: {posted_at}")
                posted_at_discord = "Unknown"
                expires_at_discord = "Unknown" if content_type == "story" else None

            like_count = item.get('like_count', "N/A" if content_type == "story" else "Unknown")
            comment_count = item.get('comment_count', "N/A" if content_type == "story" else "Unknown")

            embed.clear_fields()
            if content_type == "post":
                embed.add_field(name="Post ID", value=item['id'], inline=True)
                embed.add_field(name="Shortcode", value=identifier, inline=True)
                embed.add_field(name="Posted At", value=posted_at_discord, inline=True)
                embed.add_field(name="Likes", value=like_count, inline=True)
                embed.add_field(name="Comments", value=comment_count, inline=True)
            else:  # Story
                embed.add_field(name="Story ID", value=identifier, inline=True)
                embed.add_field(name="Posted At", value=posted_at_discord, inline=True)
                embed.add_field(name="Expires At", value=expires_at_discord, inline=True)

            embed.set_author(name=f"@{item['username']} | Instagram", icon_url="attachment://instagram.png" if instagram_logo else None)
            files = [instagram_logo] if instagram_logo else []
            if item.get('profile_data') and item.get('profile_filename'):
                profile_data = io.BytesIO(item['profile_data'].getvalue())
                files.append(discord.File(profile_data, filename=item['profile_filename']))
                embed.set_thumbnail(url=f"attachment://{item['profile_filename']}")
            logging.warning(f"No media available for Instagram {content_type} {identifier}")
            try:
                message = await channel.send(content="", embed=embed, files=files, view=view)
            except discord.errors.HTTPException as e:
                logging.error(f"Failed to send message for {identifier}: {e}")
                print(f"Failed to send message for {identifier}: {e}")
                continue
        if content_type == "post":
            save_last_ig_post_shortcode(
                username=item['username'],
                shortcode=item['shortcode'],
                timestamp=item['timestamp'],
                channel_id=auto_post_channel_id,
                message_id=message.id,
                like_count=item.get('like_count'),
                comment_count=item.get('comment_count')
            )
            logging.info(f"Auto-posted Instagram post shortcode {item['shortcode']} to channel {auto_post_channel_id}, message_id: {message.id}")
            print(f"Auto-posted Instagram post shortcode {item['shortcode']} to channel {auto_post_channel_id}, message_id: {message.id}")
        else:
            save_last_ig_story(
                username=item['username'],
                story_id=item['shortcode'],
                timestamp=item['timestamp'],
                channel_id=auto_post_channel_id,
                message_id=message.id
            )
            logging.info(f"Auto-posted Instagram story {item['shortcode']} to channel {auto_post_channel_id}, message_id: {message.id}")
            print(f"Auto-posted Instagram story {item['shortcode']} to channel {auto_post_channel_id}, message_id: {message.id}")

@tree.command(name="ping", description="Check for new Instagram posts and stories in the current channel")
async def ping(interaction: discord.Interaction):
    """Check for new Instagram posts and stories in the current channel."""
    await interaction.response.send_message("🔄 Checking for new Instagram posts and stories, please wait...", ephemeral=True)
    channel = interaction.channel
    if not channel:
        logging.error("Error: Discord channel not found")
        print("Error: Discord channel not found")
        await interaction.followup.send("Error: Discord channel not found", ephemeral=True)
        return
    
    content_items, deleted_posts = await fetch_instagram_content(channel_id=channel.id)
    
    current_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    instagram_logo = discord.File(INSTAGRAM_LOGO_PATH, filename="instagram.png") if os.path.exists(INSTAGRAM_LOGO_PATH) else None
    if not instagram_logo:
        logging.warning(f"Instagram logo file not found at {INSTAGRAM_LOGO_PATH}")

    for deleted_post in deleted_posts:
        shortcode = deleted_post["entry"]["shortcode"]
        username = deleted_post["username"]
        message_id = deleted_post["entry"]["message_ids"].get(str(channel.id))
        logging.debug(f"Processing deleted post {shortcode} for @{username}, message_id: {message_id}, channel_id: {channel.id}")
        if message_id:
            try:
                message = await channel.fetch_message(int(message_id))
                if message.embeds and len(message.embeds) > 0 and message.embeds[0].description and "**Deleted Post**: This post has been deleted." in message.embeds[0].description:
                    logging.debug(f"Message {message_id} for {shortcode} already marked as deleted, skipping edit")
                    continue
                embed = message.embeds[0] if message.embeds and len(message.embeds) > 0 else discord.Embed(
                    title="Deleted Instagram Post",
                    color=0xC13584
                )
                original_caption = embed.description if message.embeds and len(message.embeds) > 0 and embed.description and "**Deleted Post**: This post has been deleted." not in embed.description else "No caption"
                deletion_notice = "\n\n**Deleted Post**: This post has been deleted."
                embed.description = f"{original_caption}{deletion_notice}"
                posted_at = deleted_post["entry"]["timestamp"]
                post_id = deleted_post["entry"]["message_ids"].get(str(channel.id), "N/A")
                posted_at_discord = posted_at or "Unknown"
                if posted_at and isinstance(posted_at, str):
                    try:
                        posted_at_dt = datetime.strptime(posted_at, "%Y-%m-%d %H:%M:%S UTC")
                        posted_at_discord = f"<t:{int(posted_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                    except ValueError as e:
                        logging.error(f"Invalid posted_at timestamp format for {shortcode}: {posted_at}, error: {e}")
                        posted_at_discord = posted_at or "Unknown"
                else:
                    logging.warning(f"posted_at is None or not a string for {shortcode}: {posted_at}")
                    posted_at_discord = "Unknown"

                deleted_at = deleted_post["entry"].get("deleted_at", current_utc)
                deleted_at_discord = deleted_at
                if deleted_at and isinstance(deleted_at, str):
                    try:
                        deleted_at_dt = datetime.strptime(deleted_at, "%Y-%m-%d %H:%M:%S UTC")
                        deleted_at_discord = f"<t:{int(deleted_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                    except ValueError as e:
                        logging.error(f"Invalid deleted_at timestamp format for {shortcode}: {deleted_at}, error: {e}")
                        deleted_at_discord = deleted_at or current_utc
                else:
                    logging.warning(f"deleted_at is None or not a string for {shortcode}: {deleted_at}, using current_utc")
                    deleted_at_discord = f"<t:{int(datetime.strptime(current_utc, '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc).timestamp())}:F>"

                like_count = deleted_post["entry"].get("like_count", None)
                comment_count = deleted_post["entry"].get("comment_count", None)
                if like_count is None or comment_count is None:
                    cached_post = INSTAGRAM_POST_CACHE.get(username, {}).get("post", {})
                    if cached_post.get("shortcode") == shortcode:
                        like_count = cached_post.get("like_count", "Unknown")
                        comment_count = cached_post.get("comment_count", "Unknown")
                    else:
                        like_count = "Unknown"
                        comment_count = "Unknown"

                embed.clear_fields()
                embed.add_field(name="Post ID", value=post_id, inline=True)
                embed.add_field(name="Shortcode", value=shortcode, inline=True)
                embed.add_field(name="Posted At", value=posted_at_discord, inline=True)
                embed.add_field(name="Deleted At", value=deleted_at_discord, inline=True)
                embed.add_field(name="Likes", value=like_count, inline=True)
                embed.add_field(name="Comments", value=comment_count, inline=True)

                embed.set_author(name=f"@{username} | Instagram", icon_url="attachment://instagram.png" if instagram_logo else None)

                files = [instagram_logo] if instagram_logo else []
                profile_data = None
                profile_filename = None
                if username in INSTAGRAM_POST_CACHE and "profile" in INSTAGRAM_POST_CACHE[username]:
                    cached_profile = INSTAGRAM_POST_CACHE[username]["profile"]
                    if time.time() - cached_profile["timestamp"] < 300:
                        profile_data = io.BytesIO(cached_profile["profile_data"].getvalue())
                        profile_filename = cached_profile["profile_filename"]
                        files.append(discord.File(profile_data, filename=profile_filename))
                        embed.set_thumbnail(url=f"attachment://{profile_filename}")
                        logging.debug(f"Using cached profile picture for deleted post {shortcode}: {profile_filename}")

                if message.attachments:
                    for attachment in message.attachments:
                        if attachment.filename.lower().endswith(('.jpg', '.mp4')):
                            response = requests.get(attachment.url)
                            response.raise_for_status()
                            file_size = len(response.content)
                            if file_size <= DISCORD_FILE_SIZE_LIMIT:
                                files.append(discord.File(io.BytesIO(response.content), filename=attachment.filename))
                                logging.debug(f"Retaining attachment for deleted post {shortcode}: {attachment.filename}")
                            else:
                                logging.warning(f"Attachment {attachment.filename} for deleted post {shortcode} exceeds Discord file size limit ({DISCORD_FILE_SIZE_LIMIT} bytes)")
                await message.edit(content="", embed=embed, attachments=files, view=None)
                save_last_ig_post_shortcode(
                    username=username,
                    shortcode=shortcode,
                    timestamp=posted_at or "1970-01-01 00:00:00 UTC",
                    channel_id=channel.id,
                    message_id=message_id,
                    marked_deleted=True,
                    deleted_at=deleted_at or current_utc,
                    like_count=like_count if like_count != "Unknown" else None,
                    comment_count=comment_count if comment_count != "Unknown" else None
                )
                logging.info(f"Edited message {message_id} in channel {channel.id} for deleted post {shortcode} with deletion notice")
                print(f"Edited message {message_id} in channel {channel.id} for deleted post {shortcode}")
            except discord.NotFound:
                logging.warning(f"Message {message_id} for deleted post {shortcode} not found in channel {channel.id}")
                print(f"Message {message_id} for deleted post {shortcode} not found in channel {channel.id}")
            except discord.errors.HTTPException as e:
                logging.error(f"HTTP error editing message {message_id} for deleted post {shortcode}: {e}")
                print(f"HTTP error editing message {message_id} for deleted post {shortcode}: {e}")
            except Exception as e:
                logging.error(f"Error editing message {message_id} for deleted post {shortcode}: {e}")
                print(f"Error editing message {message_id} for deleted post {shortcode}: {e}")

    for username in INSTAGRAM_STORY_CACHE:
        story_history = load_last_ig_story(username)
        for story_entry in story_history.get("stories", []):
            story_id = story_entry["story_id"]
            message_id = story_entry["message_ids"].get(str(channel.id))
            if message_id and story_entry.get("expired"):
                try:
                    message = await channel.fetch_message(int(message_id))
                    if message.embeds and len(message.embeds) > 0 and message.embeds[0].description and "**Expired Story**: This story has expired." in message.embeds[0].description:
                        logging.debug(f"Message {message_id} for story {story_id} already marked as expired, skipping edit")
                        continue
                    embed = message.embeds[0] if message.embeds and len(message.embeds) > 0 else discord.Embed(
                        title="Expired Instagram Story",
                        color=0xC13584
                    )
                    expiration_notice = "**Expired Story**: This story has expired."
                    embed.description = expiration_notice
                    posted_at = story_entry["timestamp"]
                    story_id_field = story_id
                    posted_at_discord = posted_at or "Unknown"
                    if posted_at and isinstance(posted_at, str):
                        try:
                            posted_at_dt = datetime.strptime(posted_at, "%Y-%m-%d %H:%M:%S UTC")
                            posted_at_discord = f"<t:{int(posted_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                        except ValueError as e:
                            logging.error(f"Invalid posted_at timestamp format for story {story_id}: {posted_at}, error: {e}")
                            posted_at_discord = posted_at or "Unknown"
                    else:
                        logging.warning(f"posted_at is None or not a string for story {story_id}: {posted_at}")
                        posted_at_discord = "Unknown"

                    expired_at = story_entry.get("expired_at", current_utc)
                    expired_at_discord = expired_at
                    if expired_at and isinstance(expired_at, str):
                        try:
                            expired_at_dt = datetime.strptime(expired_at, "%Y-%m-%d %H:%M:%S UTC")
                            expired_at_discord = f"<t:{int(expired_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                        except ValueError as e:
                            logging.error(f"Invalid expired_at timestamp format for story {story_id}: {expired_at}, error: {e}")
                            expired_at_discord = expired_at or current_utc
                    else:
                        logging.warning(f"expired_at is None or not a string for story {story_id}: {expired_at}, using current_utc")
                        expired_at_discord = f"<t:{int(datetime.strptime(current_utc, '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc).timestamp())}:F>"

                    embed.clear_fields()
                    embed.add_field(name="Story ID", value=story_id_field, inline=True)
                    embed.add_field(name="Posted At", value=posted_at_discord, inline=True)
                    embed.add_field(name="Expired At", value=expired_at_discord, inline=True)

                    embed.set_author(name=f"@{username} | Instagram", icon_url="attachment://instagram.png" if instagram_logo else None)

                    files = [instagram_logo] if instagram_logo else []
                    profile_data = None
                    profile_filename = None
                    if username in INSTAGRAM_STORY_CACHE and "profile" in INSTAGRAM_STORY_CACHE[username]:
                        cached_profile = INSTAGRAM_STORY_CACHE[username]["profile"]
                        if time.time() - cached_profile["timestamp"] < 300:
                            profile_data = io.BytesIO(cached_profile["profile_data"].getvalue())
                            profile_filename = cached_profile["profile_filename"]
                            files.append(discord.File(profile_data, filename=profile_filename))
                            embed.set_thumbnail(url=f"attachment://{profile_filename}")
                            logging.debug(f"Using cached profile picture for expired story {story_id}: {profile_filename}")

                    if message.attachments:
                        for attachment in message.attachments:
                            if attachment.filename.lower().endswith(('.jpg', '.mp4')):
                                response = requests.get(attachment.url)
                                response.raise_for_status()
                                file_size = len(response.content)
                                if file_size <= DISCORD_FILE_SIZE_LIMIT:
                                    files.append(discord.File(io.BytesIO(response.content), filename=attachment.filename))
                                    logging.debug(f"Retaining attachment for expired story {story_id}: {attachment.filename}")
                                else:
                                    logging.warning(f"Attachment {attachment.filename} for expired story {story_id} exceeds Discord file size limit ({DISCORD_FILE_SIZE_LIMIT} bytes)")
                    await message.edit(content="", embed=embed, attachments=files, view=None)
                    logging.info(f"Edited message {message_id} in channel {channel.id} for expired story {story_id} with expiration notice")
                    print(f"Edited message {message_id} in channel {channel.id} for expired story {story_id}")
                except discord.NotFound:
                    logging.warning(f"Message {message_id} for expired story {story_id} not found in channel {channel.id}")
                    print(f"Message {message_id} for expired story {story_id} not found in channel {channel.id}")
                except discord.errors.HTTPException as e:
                    logging.error(f"HTTP error editing message {message_id} for expired story {story_id}: {e}")
                    print(f"HTTP error editing message {message_id} for expired story {story_id}: {e}")
                except Exception as e:
                    logging.error(f"Error editing message {message_id} for expired story {story_id}: {e}")
                    print(f"Error editing message {message_id} for expired story {story_id}: {e}")

    if not content_items:
        logging.info("No new Instagram posts or stories found for auto-post")
        print("No new Instagram posts or stories found for auto-post")
        await interaction.followup.send("✅ No new Instagram posts or stories found.", ephemeral=True)
        return

    for item in content_items:
        instagram_logo = discord.File(INSTAGRAM_LOGO_PATH, filename="instagram.png") if os.path.exists(INSTAGRAM_LOGO_PATH) else None
        if not instagram_logo:
            logging.warning(f"Instagram logo file not found at {INSTAGRAM_LOGO_PATH}")
        content_type = item['type']
        identifier = item['shortcode']
        view = PostView(item['url'], content_type)
        if item['media_data_list'] and item['filename_list']:
            files = [instagram_logo] if instagram_logo else []
            embeds = []
            for idx, (media_data, filename) in enumerate(item['media_data_list']):
                file_size = len(media_data.getvalue())
                logging.debug(f"Processing {content_type} media {idx+1} for {item['url']}: {filename}, size: {file_size} bytes")
                if file_size > DISCORD_FILE_SIZE_LIMIT:
                    logging.warning(f"Media {filename} exceeds Discord file size limit ({DISCORD_FILE_SIZE_LIMIT} bytes)")
                    continue
                files.append(discord.File(io.BytesIO(media_data.getvalue()), filename=filename))
                embed = discord.Embed(
                    title=f"New Instagram {content_type.capitalize()}{' (Media ' + str(idx+1) + ')' if idx > 0 else ''}",
                    description=item['text'] if content_type == "post" and idx == 0 else "" if content_type == "story" else f"Additional media {idx+1} for {content_type}",
                    color=0xC13584
                )
                posted_at = item['timestamp']
                posted_at_discord = posted_at or "Unknown"
                if posted_at and isinstance(posted_at, str):
                    try:
                        posted_at_dt = datetime.strptime(posted_at, "%Y-%m-%d %H:%M:%S UTC")
                        posted_at_discord = f"<t:{int(posted_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                        if content_type == "story":
                            expires_at_dt = posted_at_dt + timedelta(hours=STORY_EXPIRATION_HOURS)
                            expires_at_discord = f"<t:{int(expires_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                    except ValueError as e:
                        logging.error(f"Invalid posted_at timestamp format for {identifier}: {posted_at}, error: {e}")
                        posted_at_discord = posted_at or "Unknown"
                        expires_at_discord = "Unknown" if content_type == "story" else None
                else:
                    logging.warning(f"posted_at is None or not a string for {identifier}: {posted_at}")
                    posted_at_discord = "Unknown"
                    expires_at_discord = "Unknown" if content_type == "story" else None

                like_count = item.get('like_count', "N/A" if content_type == "story" else "Unknown")
                comment_count = item.get('comment_count', "N/A" if content_type == "story" else "Unknown")

                embed.clear_fields()
                if content_type == "post":
                    embed.add_field(name="Post ID", value=item['id'], inline=True)
                    embed.add_field(name="Shortcode", value=identifier, inline=True)
                    embed.add_field(name="Posted At", value=posted_at_discord, inline=True)
                    embed.add_field(name="Likes", value=like_count, inline=True)
                    embed.add_field(name="Comments", value=comment_count, inline=True)
                else:  
                    embed.add_field(name="Story ID", value=identifier, inline=True)
                    embed.add_field(name="Posted At", value=posted_at_discord, inline=True)
                    embed.add_field(name="Expires At", value=expires_at_discord, inline=True)

                embed.set_author(name=f"@{item['username']} | Instagram", icon_url="attachment://instagram.png" if instagram_logo else None)
                if item.get('profile_data') and item.get('profile_filename'):
                    profile_data = io.BytesIO(item['profile_data'].getvalue())
                    files.append(discord.File(profile_data, filename=item['profile_filename']))
                    embed.set_thumbnail(url=f"attachment://{item['profile_filename']}")
                embeds.append(embed)
            logging.info(f"Posting Instagram {content_type} {identifier} with media {item['filename_list']} to channel {channel.id}")
            try:
                message = await channel.send(content="", embeds=embeds, files=files, view=view)
                logging.info(f"Successfully sent message with {len(files)} files and {len(embeds)} embeds for {identifier}")
            except discord.errors.HTTPException as e:
                logging.error(f"Failed to send message for {identifier}: {e}")
                print(f"Failed to send message for {identifier}: {e}")
                continue
        else:
            embed = discord.Embed(
                title=f"New Instagram {content_type.capitalize()}",
                description=item['text'] if content_type == "post" else "",
                color=0xC13584
            )
            posted_at = item['timestamp']
            posted_at_discord = posted_at or "Unknown"
            if posted_at and isinstance(posted_at, str):
                try:
                    posted_at_dt = datetime.strptime(posted_at, "%Y-%m-%d %H:%M:%S UTC")
                    posted_at_discord = f"<t:{int(posted_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                    if content_type == "story":
                        expires_at_dt = posted_at_dt + timedelta(hours=STORY_EXPIRATION_HOURS)
                        expires_at_discord = f"<t:{int(expires_at_dt.replace(tzinfo=timezone.utc).timestamp())}:F>"
                except ValueError as e:
                    logging.error(f"Invalid posted_at timestamp format for {identifier}: {posted_at}, error: {e}")
                    posted_at_discord = posted_at or "Unknown"
                    expires_at_discord = "Unknown" if content_type == "story" else None
            else:
                logging.warning(f"posted_at is None or not a string for {identifier}: {posted_at}")
                posted_at_discord = "Unknown"
                expires_at_discord = "Unknown" if content_type == "story" else None

            like_count = item.get('like_count', "N/A" if content_type == "story" else "Unknown")
            comment_count = item.get('comment_count', "N/A" if content_type == "story" else "Unknown")

            embed.clear_fields()
            if content_type == "post":
                embed.add_field(name="Post ID", value=item['id'], inline=True)
                embed.add_field(name="Shortcode", value=identifier, inline=True)
                embed.add_field(name="Posted At", value=posted_at_discord, inline=True)
                embed.add_field(name="Likes", value=like_count, inline=True)
                embed.add_field(name="Comments", value=comment_count, inline=True)
            else: 
                embed.add_field(name="Story ID", value=identifier, inline=True)
                embed.add_field(name="Posted At", value=posted_at_discord, inline=True)
                embed.add_field(name="Expires At", value=expires_at_discord, inline=True)

            embed.set_author(name=f"@{item['username']} | Instagram", icon_url="attachment://instagram.png" if instagram_logo else None)
            files = [instagram_logo] if instagram_logo else []
            if item.get('profile_data') and item.get('profile_filename'):
                profile_data = io.BytesIO(item['profile_data'].getvalue())
                files.append(discord.File(profile_data, filename=item['profile_filename']))
                embed.set_thumbnail(url=f"attachment://{item['profile_filename']}")
            logging.warning(f"No media available for Instagram {content_type} {identifier}")
            try:
                message = await channel.send(content="", embed=embed, files=files, view=view)
            except discord.errors.HTTPException as e:
                logging.error(f"Failed to send message for {identifier}: {e}")
                print(f"Failed to send message for {identifier}: {e}")
                continue
        if content_type == "post":
            save_last_ig_post_shortcode(
                username=item['username'],
                shortcode=item['shortcode'],
                timestamp=item['timestamp'],
                channel_id=channel.id,
                message_id=message.id,
                like_count=item.get('like_count'),
                comment_count=item.get('comment_count')
            )
            logging.info(f"Posted Instagram post shortcode {item['shortcode']} to channel {channel.id}, message_id: {message.id}")
            print(f"Posted Instagram post shortcode {item['shortcode']} to channel {channel.id}, message_id: {message.id}")
        else:
            save_last_ig_story(
                username=item['username'],
                story_id=item['shortcode'],
                timestamp=item['timestamp'],
                channel_id=channel.id,
                message_id=message.id
            )
            logging.info(f"Posted Instagram story {item['shortcode']} to channel {channel.id}, message_id: {message.id}")
            print(f"Posted Instagram story {item['shortcode']} to channel {channel.id}, message_id: {message.id}")
    await interaction.followup.send("✅ New Instagram posts and stories checked and posted if available.", ephemeral=True)

@tree.command(name="autopost", description="Enable/disable auto-posting of new Instagram posts and stories to a specified channel")
@app_commands.describe(channel="The channel to auto-post new Instagram posts and stories to (leave empty to disable)")
async def autopost(interaction: discord.Interaction, channel: Optional[discord.TextChannel] = None):
    """Enable or disable auto-posting of Instagram posts and stories."""
    await interaction.response.defer(ephemeral=True)
    try:
        if channel:
            save_auto_post_channel(channel.id)
            logging.info(f"Auto-post channel set to {channel.id} by {interaction.user}")
            print(f"Auto-post channel set to {channel.id}")
            await interaction.followup.send(f"✅ Auto-posting enabled for new Instagram posts and stories in {channel.mention}.", ephemeral=True)
        else:
            save_auto_post_channel(None)
            logging.info(f"Auto-posting disabled by {interaction.user}")
            print("Auto-posting disabled")
            await interaction.followup.send("✅ Auto-posting disabled.", ephemeral=True)
    except Exception as e:
        logging.error(f"Error in autopost command: {e}")
        print(f"Error in autopost command: {e}")
        await interaction.followup.send(f"Error setting auto-post channel: {str(e)}", ephemeral=True)

@tree.command(name="userdetails", description="Show Instagram user details and follower changes")
@app_commands.describe(username="The Instagram username to fetch details for (default: avamax)")
async def userdetails(interaction: discord.Interaction, username: str = "avamax"):
    """Show Instagram user details for the specified user."""
    await interaction.response.defer()
    try:
        embed, file = await userdetails_instagram(username=username)
        await interaction.followup.send(embed=embed, file=file)
        logging.info(f"Sent user details for @{username} to Discord")
        print(f"Sent user details for @{username} to Discord")
    except Exception as e:
        logging.error(f"Error in userdetails command for @{username}: {e}")
        print(f"Error in userdetails command for @{username}: {e}")
        if str(e).startswith("429"):
            await interaction.followup.send(f"Rate limit hit, trying with another account...", ephemeral=True)
            try:
                embed, file = await userdetails_instagram(username=username)
                await interaction.followup.send(embed=embed, file=file)
            except Exception as re:
                logging.error(f"Retry failed for @{username}: {re}")
                print(f"Retry failed for @{username}: {re}")
                await interaction.followup.send(f"Error fetching user details for @{username}: {str(re)}", ephemeral=True)
        else:
            await interaction.followup.send(f"Error fetching user details for @{username}: {str(e)}", ephemeral=True)

@bot.event
async def on_ready():
    """Handle bot startup."""
    try:
        print(f"Logged in as {bot.user}")
        logging.info(f"Logged in as {bot.user}")
        await tree.sync()
        logging.info("Slash commands synced")
        print("Slash commands synced")
        check_social_posts.start()
    except Exception as e:
        logging.error(f"Error in on_ready: {e}")
        print(f"Error in on_ready: {e}")

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)