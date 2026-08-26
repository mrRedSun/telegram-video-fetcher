package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
)

const maxUpload = int64(49_000_000)

var urlRE = regexp.MustCompile(`https?://[^\s<>]+`)

type config struct {
	Token, DataDir, TempDir     string
	Workers, QueueSize, MaxURLs int
	Timeout                     time.Duration
}
type api struct {
	base   string
	client *http.Client
}
type update struct {
	UpdateID int64    `json:"update_id"`
	Message  *message `json:"message"`
}
type message struct {
	MessageID int64 `json:"message_id"`
	Chat      struct {
		ID int64 `json:"id"`
	} `json:"chat"`
	Text    string `json:"text"`
	Caption string `json:"caption"`
}
type job struct {
	ChatID, MessageID int64
	URL               string
}
type cache struct{ db *bolt.DB }
type tgResponse struct {
	OK          bool            `json:"ok"`
	Description string          `json:"description"`
	Result      json.RawMessage `json:"result"`
}
type sentMessage struct {
	Video    *tgFile `json:"video"`
	Document *tgFile `json:"document"`
}
type tgFile struct {
	FileID   string `json:"file_id"`
	UniqueID string `json:"file_unique_id"`
}

type mediaInfo struct {
	Duration  float64       `json:"duration"`
	Extractor string        `json:"extractor_key"`
	Formats   []mediaFormat `json:"formats"`
}

type mediaFormat struct {
	ID                  string  `json:"format_id"`
	Ext                 string  `json:"ext"`
	VideoCodec          string  `json:"vcodec"`
	AudioCodec          string  `json:"acodec"`
	FormatNote          string  `json:"format_note"`
	Width               float64 `json:"width"`
	Height              float64 `json:"height"`
	FPS                 float64 `json:"fps"`
	TotalBitrate        float64 `json:"tbr"`
	AudioBitrate        float64 `json:"abr"`
	FileSize            float64 `json:"filesize"`
	ApproximateFileSize float64 `json:"filesize_approx"`
	LanguagePreference  float64 `json:"language_preference"`
}

type videoMeta struct {
	Width, Height, Duration int
}

type probeResult struct {
	Streams []struct {
		CodecType         string `json:"codec_type"`
		CodecName         string `json:"codec_name"`
		Width             int    `json:"width"`
		Height            int    `json:"height"`
		SampleAspectRatio string `json:"sample_aspect_ratio"`
	} `json:"streams"`
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

type formatChoice struct {
	Selector string
	Score    float64
	Size     int64
	Known    bool
}

func main() {
	if probe := os.Getenv("PROBE_URL"); probe != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		dir, err := os.MkdirTemp("", "probe-")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(dir)
		path, err := download(ctx, dir, probe)
		if err != nil {
			panic(err)
		}
		path, meta, err := prepareTelegramVideo(ctx, dir, path)
		if err != nil {
			panic(err)
		}
		st, err := os.Stat(path)
		if err != nil {
			panic(err)
		}
		if st.Size() > maxUpload {
			panic("probe output exceeds upload limit")
		}
		slog.Info("probe succeeded", "bytes", st.Size(), "file", filepath.Base(path), "width", meta.Width, "height", meta.Height, "duration", meta.Duration)
		return
	}
	cfg, err := loadConfig()
	if err != nil {
		panic(err)
	}
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(cfg.TempDir, 0700); err != nil {
		panic(err)
	}
	db, err := bolt.Open(filepath.Join(cfg.DataDir, "cache.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	c := &cache{db: db}
	if err := c.init(); err != nil {
		panic(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	a := &api{base: "https://api.telegram.org/bot" + cfg.Token, client: &http.Client{Timeout: cfg.Timeout + time.Minute}}
	jobs := make(chan job, cfg.QueueSize)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); worker(ctx, cfg, a, c, jobs) }()
	}
	poll(ctx, cfg, a, jobs)
	close(jobs)
	wg.Wait()
}

func loadConfig() (config, error) {
	c := config{Token: os.Getenv("TELEGRAM_BOT_TOKEN"), DataDir: env("DATA_DIR", "/data"), TempDir: env("TEMP_DIR", "/tmp/downloads"), Workers: envInt("WORKERS", 3), QueueSize: envInt("QUEUE_SIZE", 64), MaxURLs: envInt("MAX_URLS_PER_MESSAGE", 5), Timeout: time.Duration(envInt("DOWNLOAD_TIMEOUT_SECONDS", 600)) * time.Second}
	if c.Token == "" {
		return c, errors.New("TELEGRAM_BOT_TOKEN is required")
	}
	return c, nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	v, e := strconv.Atoi(os.Getenv(k))
	if e == nil && v > 0 {
		return v
	}
	return d
}

func poll(ctx context.Context, cfg config, a *api, jobs chan<- job) {
	var offset int64
	for ctx.Err() == nil {
		var out []update
		err := a.call(ctx, "getUpdates", map[string]any{"offset": offset, "timeout": 30, "allowed_updates": []string{"message"}}, &out)
		if err != nil {
			slog.Error("poll failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		for _, u := range out {
			offset = u.UpdateID + 1
			if u.Message == nil {
				continue
			}
			text := u.Message.Text + " " + u.Message.Caption
			urls := extractURLs(text, cfg.MaxURLs)
			for _, raw := range urls {
				select {
				case jobs <- job{u.Message.Chat.ID, u.Message.MessageID, raw}:
				default:
					_ = a.reply(ctx, u.Message.Chat.ID, u.Message.MessageID, "Download queue is full; please try again shortly.")
				}
			}
		}
	}
}
func extractURLs(s string, max int) []string {
	matches := urlRE.FindAllString(s, -1)
	out := make([]string, 0, max)
	seen := map[string]bool{}
	for _, v := range matches {
		v = strings.TrimRight(v, ".,;:!?)]}\"'")
		u, e := url.Parse(v)
		if e != nil || !(u.Scheme == "http" || u.Scheme == "https") || u.Hostname() == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
		if len(out) == max {
			break
		}
	}
	return out
}

func worker(ctx context.Context, cfg config, a *api, c *cache, jobs <-chan job) {
	for j := range jobs {
		if ctx.Err() != nil {
			return
		}
		key := "media-v4:" + normalize(j.URL)
		if id, _ := c.get(key); id != "" {
			if err := a.sendCached(ctx, j, id); err == nil {
				continue
			}
		}
		dctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		dir, err := os.MkdirTemp(cfg.TempDir, "job-")
		if err != nil {
			cancel()
			continue
		}
		path, err := download(dctx, dir, j.URL)
		if err != nil {
			cancel()
			slog.Warn("download failed", "error", err, "host", host(j.URL))
			if !isUnsupported(err) {
				_ = a.reply(ctx, j.ChatID, j.MessageID, "Could not download a usable video from this link.")
			}
			os.RemoveAll(dir)
			continue
		}
		path, meta, err := prepareTelegramVideo(dctx, dir, path)
		cancel()
		if err != nil {
			slog.Warn("media validation failed", "error", err, "host", host(j.URL))
			_ = a.reply(ctx, j.ChatID, j.MessageID, "Downloaded media could not be prepared as a Telegram-compatible video.")
			os.RemoveAll(dir)
			continue
		}
		st, err := os.Stat(path)
		if err != nil || st.Size() > maxUpload {
			_ = a.reply(ctx, j.ChatID, j.MessageID, "The available video is too large for Telegram's 50 MB bot limit.")
			os.RemoveAll(dir)
			continue
		}
		fileID, err := a.upload(ctx, j, path, meta)
		if err != nil {
			slog.Error("upload failed", "error", err, "host", host(j.URL))
			_ = a.reply(ctx, j.ChatID, j.MessageID, "Telegram rejected the downloaded video.")
		} else {
			_ = c.put(key, fileID)
		}
		os.RemoveAll(dir)
	}
}
func download(ctx context.Context, dir, raw string) (string, error) {
	infoPath := filepath.Join(dir, "media-info.json")
	selector, err := chooseFormat(ctx, raw, infoPath)
	if err != nil {
		return "", err
	}
	slog.Info("format selected", "selector", selector, "host", host(raw))
	out := filepath.Join(dir, "video.%(ext)s")
	cmd := exec.CommandContext(ctx, "yt-dlp", "--no-warnings", "--max-filesize", "49M", "-f", selector, "--merge-output-format", "mp4", "-o", out, "--load-info-json", infoPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("yt-dlp: %w: %.300s", err, stderr.String())
	}
	files, _ := filepath.Glob(filepath.Join(dir, "video.*"))
	if len(files) != 1 {
		return "", errors.New("no single output")
	}
	return files[0], nil
}

func chooseFormat(ctx context.Context, raw, infoPath string) (string, error) {
	cmd := exec.CommandContext(ctx, "yt-dlp", "--dump-single-json", "--skip-download", "--no-playlist", "--no-warnings", "--", raw)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("metadata: %w: %.300s", err, stderr.String())
	}
	var info mediaInfo
	if err := json.Unmarshal(stdout.Bytes(), &info); err != nil {
		return "", fmt.Errorf("metadata JSON: %w", err)
	}
	if info.Duration > 3600 {
		return "", errors.New("video exceeds the one-hour duration limit")
	}
	if err := os.WriteFile(infoPath, stdout.Bytes(), 0600); err != nil {
		return "", fmt.Errorf("save metadata snapshot: %w", err)
	}
	choice, ok := selectFormat(info, 48_000_000)
	if !ok {
		return "", errors.New("no video format fits the size budget")
	}
	return choice.Selector, nil
}

func selectFormat(info mediaInfo, budget int64) (formatChoice, bool) {
	var knownBest, unknownBest formatChoice
	var haveKnown, haveUnknown bool
	consider := func(c formatChoice, width, height float64) {
		if c.Known {
			if c.Size <= budget && (!haveKnown || c.Score > knownBest.Score) {
				knownBest, haveKnown = c, true
			}
			return
		}
		// With no trustworthy size, 1080p is the highest conservative fallback.
		if portraitAware1080p(width, height) && (!haveUnknown || c.Score > unknownBest.Score) {
			unknownBest, haveUnknown = c, true
		}
	}

	var videos, audios []mediaFormat
	for _, f := range info.Formats {
		hasVideo := f.VideoCodec != "" && f.VideoCodec != "none"
		hasAudio := f.AudioCodec != "" && f.AudioCodec != "none"
		opaqueMP4 := f.Ext == "mp4" && !hasVideo && !hasAudio
		size, known := formatSize(f, info.Duration)
		switch {
		case opaqueMP4 || hasVideo && hasAudio:
			// Prefer a source-provided progressive file over DASH/HLS reconstruction
			// when quality is otherwise comparable. It preserves platform framing
			// and is much more likely to be directly Telegram-compatible.
			bonus := 1e9
			if opaqueMP4 && strings.EqualFold(info.Extractor, "Instagram") {
				// Instagram omits codec/geometry metadata for its original
				// progressive MP4; post-download ffprobe is authoritative.
				bonus = 1e17
			}
			consider(formatChoice{Selector: f.ID, Score: videoScore(f) + audioScore(f) + bonus, Size: size, Known: known}, f.Width, f.Height)
		case hasVideo:
			videos = append(videos, f)
		case hasAudio:
			audios = append(audios, f)
		}
	}
	for _, v := range videos {
		vs, vk := formatSize(v, info.Duration)
		for _, a := range audios {
			as, ak := formatSize(a, info.Duration)
			consider(formatChoice{Selector: v.ID + "+" + a.ID, Score: videoScore(v) + audioScore(a), Size: vs + as, Known: vk && ak}, v.Width, v.Height)
		}
	}
	if haveKnown {
		return knownBest, true
	}
	return unknownBest, haveUnknown
}

func portraitAware1080p(width, height float64) bool {
	if width > 0 && height > 0 {
		return min(width, height) <= 1080
	}
	return max(width, height) <= 1920
}

func formatSize(f mediaFormat, duration float64) (int64, bool) {
	if f.FileSize > 0 {
		return int64(f.FileSize), true
	}
	if f.ApproximateFileSize > 0 {
		return int64(f.ApproximateFileSize), true
	}
	if duration > 0 && f.TotalBitrate > 0 {
		return int64(duration * f.TotalBitrate * 1000 / 8 * 1.08), true
	}
	return 0, false
}

func videoScore(f mediaFormat) float64 {
	codec := strings.ToLower(f.VideoCodec)
	compatibility := float64(0)
	if strings.Contains(codec, "avc") || strings.Contains(codec, "h264") {
		compatibility += 1e12
	}
	if f.Ext == "mp4" {
		compatibility += 5e11
	}
	// Resolution and frame rate define quality. Codec/container compatibility
	// only breaks ties because incompatible winners are normalized afterward.
	pixels := f.Width * f.Height
	if pixels == 0 && f.Height > 0 {
		pixels = f.Height * f.Height
	}
	resolution := pixels * 1e7
	return resolution + f.FPS*1e8 + compatibility + f.TotalBitrate
}

func isUnsupported(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unsupported url") || strings.Contains(message, "no video formats found")
}

func audioScore(f mediaFormat) float64 {
	score := f.LanguagePreference*1e6 + f.AudioBitrate
	note := strings.ToLower(f.FormatNote)
	if strings.Contains(note, "original") || strings.Contains(note, "default") {
		score += 1e8
	}
	codec := strings.ToLower(f.AudioCodec)
	if f.Ext == "m4a" || strings.Contains(codec, "mp4a") || strings.Contains(codec, "aac") {
		score += 1e7
	}
	return score
}

func prepareTelegramVideo(ctx context.Context, dir, input string) (string, videoMeta, error) {
	before, err := probeVideo(ctx, input)
	if err != nil {
		return "", videoMeta{}, err
	}
	output := filepath.Join(dir, "telegram.mp4")
	args := []string{"-y", "-v", "error", "-i", input, "-map", "0:v:0", "-map", "0:a:0?"}
	if before.videoCodec == "h264" && (before.audioCodec == "" || before.audioCodec == "aac") && (before.sar == "" || before.sar == "1:1") {
		args = append(args, "-c", "copy")
	} else {
		args = append(args, "-c:v", "libx264", "-preset", "veryfast", "-crf", "22", "-pix_fmt", "yuv420p", "-vf", "setsar=1", "-c:a", "aac", "-b:a", "128k")
	}
	args = append(args, "-metadata:s:v:0", "rotate=0", "-movflags", "+faststart", output)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", videoMeta{}, fmt.Errorf("ffmpeg normalization: %w: %.300s", err, stderr.String())
	}
	after, err := probeVideo(ctx, output)
	if err != nil {
		return "", videoMeta{}, err
	}
	if after.width < 1 || after.height < 1 || after.duration < 1 {
		return "", videoMeta{}, errors.New("invalid normalized video geometry or duration")
	}
	return output, videoMeta{Width: after.width, Height: after.height, Duration: int(after.duration + 0.5)}, nil
}

type probedVideo struct {
	width, height               int
	duration                    float64
	videoCodec, audioCodec, sar string
}

func probeVideo(ctx context.Context, path string) (probedVideo, error) {
	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries", "stream=codec_type,codec_name,width,height,sample_aspect_ratio:format=duration", "-of", "json", path)
	out, err := cmd.Output()
	if err != nil {
		return probedVideo{}, fmt.Errorf("ffprobe: %w", err)
	}
	var result probeResult
	if err := json.Unmarshal(out, &result); err != nil {
		return probedVideo{}, err
	}
	p := probedVideo{}
	for _, stream := range result.Streams {
		switch stream.CodecType {
		case "video":
			if p.videoCodec == "" {
				p.videoCodec, p.width, p.height, p.sar = stream.CodecName, stream.Width, stream.Height, stream.SampleAspectRatio
			}
		case "audio":
			if p.audioCodec == "" {
				p.audioCodec = stream.CodecName
			}
		}
	}
	p.duration, _ = strconv.ParseFloat(result.Format.Duration, 64)
	if p.videoCodec == "" {
		return p, errors.New("no video stream")
	}
	return p, nil
}
func normalize(raw string) string {
	u, e := url.Parse(raw)
	if e != nil {
		return raw
	}
	u.Fragment = ""
	u.Host = strings.ToLower(u.Host)
	q := u.Query()
	for k := range q {
		if strings.HasPrefix(strings.ToLower(k), "utm_") || k == "fbclid" || k == "si" {
			q.Del(k)
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}
func host(raw string) string { u, _ := url.Parse(raw); return u.Hostname() }

func (c *cache) init() error {
	return c.db.Update(func(tx *bolt.Tx) error { _, e := tx.CreateBucketIfNotExists([]byte("files")); return e })
}
func (c *cache) get(k string) (string, error) {
	var v string
	e := c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("files"))
		x := b.Get([]byte(k))
		if x != nil {
			v = string(x)
		}
		return nil
	})
	return v, e
}
func (c *cache) put(k, v string) error {
	return c.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("files")).Put([]byte(k), []byte(v)) })
}

func (a *api) call(ctx context.Context, method string, payload any, result any) error {
	body, _ := json.Marshal(payload)
	req, e := http.NewRequestWithContext(ctx, "POST", a.base+"/"+method, bytes.NewReader(body))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	resp, e := a.client.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	var r tgResponse
	if e = json.Unmarshal(b, &r); e != nil {
		return e
	}
	if !r.OK {
		return errors.New(r.Description)
	}
	if result != nil {
		return json.Unmarshal(r.Result, result)
	}
	return nil
}
func (a *api) reply(ctx context.Context, chat, msg int64, text string) error {
	return a.call(ctx, "sendMessage", map[string]any{"chat_id": chat, "reply_parameters": map[string]any{"message_id": msg}, "text": text}, nil)
}
func (a *api) sendCached(ctx context.Context, j job, id string) error {
	return a.call(ctx, "sendVideo", map[string]any{"chat_id": j.ChatID, "reply_parameters": map[string]any{"message_id": j.MessageID}, "video": id}, nil)
}
func (a *api) upload(ctx context.Context, j job, path string, meta videoMeta) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	_ = w.WriteField("chat_id", strconv.FormatInt(j.ChatID, 10))
	_ = w.WriteField("reply_parameters", fmt.Sprintf(`{"message_id":%d}`, j.MessageID))
	_ = w.WriteField("width", strconv.Itoa(meta.Width))
	_ = w.WriteField("height", strconv.Itoa(meta.Height))
	_ = w.WriteField("duration", strconv.Itoa(meta.Duration))
	_ = w.WriteField("supports_streaming", "true")
	p, e := w.CreateFormFile("video", filepath.Base(path))
	if e != nil {
		return "", e
	}
	if _, e = io.Copy(p, f); e != nil {
		return "", e
	}
	w.Close()
	req, e := http.NewRequestWithContext(ctx, "POST", a.base+"/sendVideo", &b)
	if e != nil {
		return "", e
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, e := a.client.Do(req)
	if e != nil {
		return "", e
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	var r tgResponse
	if e = json.Unmarshal(raw, &r); e != nil {
		return "", e
	}
	if !r.OK {
		return "", errors.New(r.Description)
	}
	var sent sentMessage
	if e = json.Unmarshal(r.Result, &sent); e != nil || sent.Video == nil {
		return "", errors.New("missing video file_id")
	}
	return sent.Video.FileID, nil
}
