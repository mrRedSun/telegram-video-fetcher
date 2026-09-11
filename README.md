# Telegram Video Fetcher

A small Go Telegram bot that replies to messages containing video-page URLs. It uses `yt-dlp` for public media, uploads results below Telegram's hosted Bot API limit, and remembers Telegram `file_id` values so repeated URLs are resent without downloading or uploading again.

## Behavior

- Handles up to five HTTP(S) URLs per message in groups and direct messages.
- Supports public pages handled by `yt-dlp`; multi-video posts return up to five
  videos together as one Telegram media album. General playlists and live
  streams remain rejected.
- Shows Telegram's native uploading-video activity indicator while uncached
  group and supergroup requests are being processed; it creates no progress
  messages and stops when processing finishes.
- Selects the highest-quality source estimated to fit, normalizes it for Telegram,
  and uses a duration-aware size-targeted transcode if the prepared file would
  otherwise exceed 49 MB. Media longer than one hour is rejected when duration
  is known.
- Uses two bounded workers and a 64-item queue, allowing two albums to be
  processed concurrently within the container's memory limit.
- Stores only a BoltDB URL-to-`file_id` cache, including ordered file-ID lists
  for albums. Downloads use a 768 MB RAM-backed temporary filesystem and are
  removed after every attempt.
- Access is unrestricted. Do not add the bot to groups where unrestricted downloading is undesirable.

## Telegram setup

Create a bot with BotFather. Disable privacy mode with `/setprivacy` if it should see ordinary group messages rather than commands and direct mentions only.

Copy `.env.example` to `.env`, replace the value with the BotFather token, and protect it:

```sh
cp .env.example .env
chmod 600 .env
docker compose up -d
```

Never commit `.env`. No cookies or authenticated downloading are supported by the default deployment.

## Limits

Telegram's hosted Bot API accepts bot uploads up to 50 MB. The bot uses 49 MB as a safety margin. Some sites do not expose reliable sizes before download; those requests may download temporarily and then be rejected. The tmpfs and timeout bound resource consumption.

Site extractors change frequently. Rebuild the image after updating the pinned `YT_DLP_VERSION` in `Dockerfile`.

## Analytics

Analytics begin when the feature is deployed. The bot stores salted hashes of
Telegram user and chat IDs plus aggregate counters; it does not store usernames,
message text, links, or raw Telegram IDs in analytics. To print the live report
without stopping the bot:

```sh
docker compose kill -s USR1 bot
docker compose logs --since=1m
```

The report includes unique users/chats, private/group split, URL messages,
submitted URLs, successful deliveries, Telegram cache hits, and silent failures.
