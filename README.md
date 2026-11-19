# Telegram Video Fetcher

A small Go Telegram bot that replies to messages containing video-page URLs. It uses `yt-dlp` for public media, uploads results below Telegram's hosted Bot API limit, and remembers Telegram `file_id` values so repeated URLs are resent without downloading or uploading again. Instagram links also deliver photos.

## Behavior

- Handles up to five HTTP(S) URLs per message in groups and direct messages.
- Supports public pages handled by `yt-dlp`; multi-video posts return up to five
  videos together as one Telegram media album. General playlists and live
  streams remain rejected.
- Instagram only (`instagram.com`, `www.instagram.com`, `instagr.am`): photo
  posts are returned as a photo, and carousels as one ordered album of up to
  five photos and videos. Photos are sent at their original resolution; one
  that exceeds Telegram's photo limits (10 MB, width + height > 10000, aspect
  ratio > 20) is sent as a document instead, and a carousel containing such a
  photo is rejected. Reels and `/tv/` links stay video-only. Every other site
  is video-only.
- Marks uncached group and supergroup requests with a temporary `👀` reaction
  while processing, briefly changes it to `👍` after delivery, and also refreshes
  Telegram's native typing action as a fallback. It creates no progress message.
- Selects the highest-quality source estimated to fit, normalizes it for Telegram,
  and uses a duration-aware size-targeted transcode if the prepared file would
  otherwise exceed 49 MB. Media longer than one hour is rejected when duration
  is known.
- Uses two bounded workers and a 64-item queue, allowing two albums to be
  processed concurrently within the container's memory limit.
- Stores only a BoltDB URL-to-`file_id` cache, including ordered file-ID lists
  for albums. Entries containing photos or documents record each item's type;
  video-only entries keep the original format. Instagram entries use a separate
  `media-v5:` key namespace, so earlier cache entries remain valid. Downloads use a 768 MB RAM-backed temporary filesystem and are
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

Instagram is accessed anonymously; no login or cookies are used. Photos come
from the image candidates in `yt-dlp` metadata (run with
`--ignore-no-formats-error` for Instagram only) and are fetched directly from
Instagram's CDN hosts (`*.cdninstagram.com`, `*.fbcdn.net`); other hosts are
refused. Private, login-gated or rate-limited posts fail silently and are
logged. To check a link without Telegram:

```sh
docker compose run --rm -e PROBE_URL='https://www.instagram.com/p/SHORTCODE/' bot
```

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
