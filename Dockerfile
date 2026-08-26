FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/bot ./cmd/bot

FROM python:3.14-alpine
ARG YT_DLP_VERSION=2026.8.19
RUN apk add --no-cache ffmpeg ca-certificates tzdata deno \
 && pip install --no-cache-dir "yt-dlp[default]==${YT_DLP_VERSION}" \
 && addgroup -S -g 10001 bot && adduser -S -D -H -u 10001 -G bot bot \
 && mkdir -p /data /tmp/downloads && chown -R bot:bot /data /tmp/downloads
COPY --from=build /out/bot /usr/local/bin/telegram-video-fetcher
USER bot
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/telegram-video-fetcher"]
