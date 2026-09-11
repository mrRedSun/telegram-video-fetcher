package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
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
const transcodeTarget = int64(47_000_000)
const maxVideosPerPost = 5

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
	From      *struct {
		ID int64 `json:"id"`
	} `json:"from"`
	Chat struct {
		ID   int64  `json:"id"`
		Type string `json:"type"`
	} `json:"chat"`
	Text    string `json:"text"`
	Caption string `json:"caption"`
}
type job struct {
	ChatID, MessageID int64
	ChatType          string
	URL               string
}
type cache struct {
	db   *bolt.DB
	salt []byte
}

type analyticsSummary struct {
	StartedAt            int64  `json:"started_at"`
	LastSeenAt           int64  `json:"last_seen_at"`
	MessagesWithURLs     uint64 `json:"messages_with_urls"`
	URLsSubmitted        uint64 `json:"urls_submitted"`
	SuccessfulDeliveries uint64 `json:"successful_deliveries"`
	CacheHits            uint64 `json:"cache_hits"`
	Failures             uint64 `json:"failures"`
}
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
	Duration  float64           `json:"duration"`
	Extractor string            `json:"extractor_key"`
	Formats   []mediaFormat     `json:"formats"`
	Entries   []json.RawMessage `json:"entries"`
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

type downloadPlan struct {
	Info     json.RawMessage
	Selector string
}

type preparedVideo struct {
	Path string
	Meta videoMeta
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
		paths, err := download(ctx, dir, probe)
		if err != nil {
			panic(err)
		}
		for i, path := range paths {
			path, meta, err := prepareTelegramVideo(ctx, filepath.Dir(path), path)
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
			slog.Info("probe item succeeded", "item", i+1, "items", len(paths), "bytes", st.Size(), "file", filepath.Base(path), "width", meta.Width, "height", meta.Height, "duration", meta.Duration)
		}
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
	if os.Getenv("ANALYTICS_REPORT") != "" {
		if err := c.reportAnalytics(os.Stdout); err != nil {
			panic(err)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	reportSignal := make(chan os.Signal, 1)
	signal.Notify(reportSignal, syscall.SIGUSR1)
	defer signal.Stop(reportSignal)
	go func() {
		for range reportSignal {
			if err := c.reportAnalytics(os.Stdout); err != nil {
				slog.Warn("analytics report failed", "error", err)
			}
		}
	}()
	a := &api{base: "https://api.telegram.org/bot" + cfg.Token, client: &http.Client{Timeout: cfg.Timeout + time.Minute}}
	jobs := make(chan job, cfg.QueueSize)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); worker(ctx, cfg, a, c, jobs) }()
	}
	poll(ctx, cfg, a, c, jobs)
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

func poll(ctx context.Context, cfg config, a *api, c *cache, jobs chan<- job) {
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
			if len(urls) > 0 {
				var userID int64
				if u.Message.From != nil {
					userID = u.Message.From.ID
				}
				if err := c.recordMessage(userID, u.Message.Chat.ID, u.Message.Chat.Type, len(urls)); err != nil {
					slog.Warn("analytics message update failed", "error", err)
				}
			}
			for _, raw := range urls {
				select {
				case jobs <- job{ChatID: u.Message.Chat.ID, MessageID: u.Message.MessageID, ChatType: u.Message.Chat.Type, URL: raw}:
				default:
					slog.Warn("download queue full", "host", host(raw))
					_ = c.recordOutcome(false, false)
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
		processJob(ctx, cfg, a, c, j)
	}
}

func processJob(ctx context.Context, cfg config, a *api, c *cache, j job) {
	key := "media-v4:" + normalize(j.URL)
	if ids, _ := c.getFileIDs(key); len(ids) > 0 {
		if err := a.sendCached(ctx, j, ids); err == nil {
			_ = c.recordOutcome(true, true)
			return
		}
	}

	progressCtx, stopProgress := context.WithCancel(ctx)
	defer stopProgress()
	if j.ChatType == "group" || j.ChatType == "supergroup" {
		go a.keepVideoProgress(progressCtx, j.ChatID)
	}

	dctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	dir, err := os.MkdirTemp(cfg.TempDir, "job-")
	if err != nil {
		_ = c.recordOutcome(false, false)
		return
	}
	defer os.RemoveAll(dir)
	paths, err := download(dctx, dir, j.URL)
	if err != nil {
		slog.Warn("download failed", "error", err, "host", host(j.URL))
		_ = c.recordOutcome(false, false)
		return
	}
	prepared := make([]preparedVideo, 0, len(paths))
	for _, source := range paths {
		path, meta, prepareErr := prepareTelegramVideo(dctx, filepath.Dir(source), source)
		if prepareErr != nil {
			err = prepareErr
			break
		}
		st, statErr := os.Stat(path)
		if statErr != nil || st.Size() > maxUpload {
			err = errors.New("prepared video exceeds upload limit")
			break
		}
		if path != source {
			_ = os.Remove(source)
		}
		prepared = append(prepared, preparedVideo{Path: path, Meta: meta})
	}
	cancel()
	if err != nil {
		slog.Warn("media validation failed", "error", err, "host", host(j.URL))
		_ = c.recordOutcome(false, false)
		return
	}
	fileIDs, err := a.upload(ctx, j, prepared)
	if err != nil {
		slog.Error("upload failed", "error", err, "host", host(j.URL))
		_ = c.recordOutcome(false, false)
		return
	}
	_ = c.putFileIDs(key, fileIDs)
	_ = c.recordOutcome(true, false)
}
func download(ctx context.Context, dir, raw string) ([]string, error) {
	plans, err := chooseFormats(ctx, raw)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(plans))
	for i, plan := range plans {
		itemDir := dir
		if len(plans) > 1 {
			itemDir = filepath.Join(dir, fmt.Sprintf("item-%d", i+1))
			if err := os.Mkdir(itemDir, 0700); err != nil {
				return nil, err
			}
		}
		infoPath := filepath.Join(itemDir, "media-info.json")
		if err := os.WriteFile(infoPath, plan.Info, 0600); err != nil {
			return nil, fmt.Errorf("save metadata snapshot: %w", err)
		}
		slog.Info("format selected", "selector", plan.Selector, "item", i+1, "items", len(plans), "host", host(raw))
		out := filepath.Join(itemDir, "video.%(ext)s")
		cmd := exec.CommandContext(ctx, "yt-dlp", "--no-warnings", "--max-filesize", "49M", "-f", plan.Selector, "--merge-output-format", "mp4", "-o", out, "--load-info-json", infoPath)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("yt-dlp item %d: %w: %.300s", i+1, err, stderr.String())
		}
		files, _ := filepath.Glob(filepath.Join(itemDir, "video.*"))
		if len(files) != 1 {
			return nil, fmt.Errorf("item %d produced no single output", i+1)
		}
		paths = append(paths, files[0])
	}
	return paths, nil
}

func chooseFormats(ctx context.Context, raw string) ([]downloadPlan, error) {
	cmd := exec.CommandContext(ctx, "yt-dlp", "--dump-single-json", "--skip-download", "--no-playlist", "--playlist-end", strconv.Itoa(maxVideosPerPost), "--no-warnings", "--", raw)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("metadata: %w: %.300s", err, stderr.String())
	}
	return plansFromMetadata(stdout.Bytes())
}

func plansFromMetadata(raw []byte) ([]downloadPlan, error) {
	var info mediaInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, fmt.Errorf("metadata JSON: %w", err)
	}
	items := []json.RawMessage{json.RawMessage(raw)}
	if len(info.Entries) > 0 {
		items = info.Entries
		if len(items) > maxVideosPerPost {
			items = items[:maxVideosPerPost]
		}
	}
	plans := make([]downloadPlan, 0, len(items))
	for i, rawInfo := range items {
		if len(rawInfo) == 0 || string(rawInfo) == "null" {
			return nil, fmt.Errorf("playlist item %d has no metadata", i+1)
		}
		var item mediaInfo
		if err := json.Unmarshal(rawInfo, &item); err != nil {
			return nil, fmt.Errorf("metadata JSON item %d: %w", i+1, err)
		}
		if item.Duration > 3600 {
			return nil, fmt.Errorf("video item %d exceeds the one-hour duration limit", i+1)
		}
		choice, ok := selectFormat(item, 48_000_000)
		if !ok {
			return nil, fmt.Errorf("no video format fits the size budget for item %d", i+1)
		}
		plans = append(plans, downloadPlan{Info: append(json.RawMessage(nil), rawInfo...), Selector: choice.Selector})
	}
	if len(plans) == 0 {
		return nil, errors.New("post has no downloadable videos")
	}
	return plans, nil
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
	compatible := before.videoCodec == "h264" && (before.audioCodec == "" || before.audioCodec == "aac") && (before.sar == "" || before.sar == "1:1")
	inputStat, err := os.Stat(input)
	if err != nil {
		return "", videoMeta{}, err
	}
	// Large AV1/VP9 sources are likely to grow past the limit when normalized to
	// H.264. Go directly to the bounded encode instead of creating a doomed CRF
	// intermediate first.
	if !compatible && inputStat.Size() > transcodeTarget/2 {
		output, err = transcodeToSize(ctx, dir, input, before)
	} else {
		args := []string{"-y", "-v", "error", "-i", input, "-map", "0:v:0", "-map", "0:a:0?"}
		if compatible {
			args = append(args, "-c", "copy")
		} else {
			args = append(args, "-c:v", "libx264", "-preset", "veryfast", "-crf", "22", "-pix_fmt", "yuv420p", "-vf", "setsar=1", "-c:a", "aac", "-b:a", "128k")
		}
		args = append(args, "-metadata:s:v:0", "rotate=0", "-movflags", "+faststart", output)
		err = runFFmpeg(ctx, "ffmpeg normalization", args)
	}
	if err != nil {
		return "", videoMeta{}, err
	}
	st, err := os.Stat(output)
	if err != nil {
		return "", videoMeta{}, err
	}
	if st.Size() > maxUpload {
		// The temporary filesystem is deliberately small. Remove the disposable
		// oversized encode before producing its size-constrained replacement.
		if err := os.Remove(output); err != nil {
			return "", videoMeta{}, fmt.Errorf("remove oversized intermediate: %w", err)
		}
		output, err = transcodeToSize(ctx, dir, input, before)
		if err != nil {
			return "", videoMeta{}, err
		}
	}
	after, err := probeVideo(ctx, output)
	if err != nil {
		return "", videoMeta{}, err
	}
	if after.width < 1 || after.height < 1 || after.duration < 1 {
		return "", videoMeta{}, errors.New("invalid normalized video geometry or duration")
	}
	st, err = os.Stat(output)
	if err != nil {
		return "", videoMeta{}, err
	}
	if st.Size() > maxUpload {
		return "", videoMeta{}, errors.New("size-targeted video exceeds upload limit")
	}
	return output, videoMeta{Width: after.width, Height: after.height, Duration: int(after.duration + 0.5)}, nil
}

func transcodeToSize(ctx context.Context, dir, input string, media probedVideo) (string, error) {
	videoRate, audioRate, err := transcodeBitrates(media.duration, media.audioCodec != "")
	if err != nil {
		return "", err
	}
	output := filepath.Join(dir, "telegram-sized.mp4")
	passlog := filepath.Join(dir, "ffmpeg-pass")
	videoArgs := []string{
		"-c:v", "libx264", "-preset", "veryfast", "-b:v", strconv.FormatInt(videoRate, 10),
		"-pix_fmt", "yuv420p", "-vf", "setsar=1", "-passlogfile", passlog,
	}
	firstPass := append([]string{"-y", "-v", "error", "-i", input, "-map", "0:v:0"}, videoArgs...)
	firstPass = append(firstPass, "-pass", "1", "-an", "-f", "null", os.DevNull)
	if err := runFFmpeg(ctx, "ffmpeg size pass 1", firstPass); err != nil {
		return "", err
	}
	secondPass := append([]string{"-y", "-v", "error", "-i", input, "-map", "0:v:0", "-map", "0:a:0?"}, videoArgs...)
	secondPass = append(secondPass, "-pass", "2")
	if media.audioCodec != "" {
		secondPass = append(secondPass, "-c:a", "aac", "-b:a", strconv.FormatInt(audioRate, 10))
	}
	secondPass = append(secondPass, "-metadata:s:v:0", "rotate=0", "-movflags", "+faststart", output)
	if err := runFFmpeg(ctx, "ffmpeg size pass 2", secondPass); err != nil {
		return "", err
	}
	return output, nil
}

func transcodeBitrates(duration float64, hasAudio bool) (int64, int64, error) {
	if duration <= 0 {
		return 0, 0, errors.New("cannot size transcode without duration")
	}
	// Reserve two percent for MP4 container overhead. Keep audio at 128 kbps
	// when possible, but reduce it for long videos so video retains most of the
	// fixed Telegram upload budget.
	totalRate := int64(float64(transcodeTarget*8) / duration * 0.98)
	audioRate := int64(0)
	if hasAudio {
		audioRate = min(int64(128_000), max(int64(32_000), totalRate/6))
	}
	videoRate := totalRate - audioRate
	if videoRate < 50_000 {
		return 0, 0, errors.New("video is too long for the upload size budget")
	}
	return videoRate, audioRate, nil
}

func runFFmpeg(ctx context.Context, label string, args []string) error {
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w: %.300s", label, err, stderr.String())
	}
	return nil
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
	return c.db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"files", "analytics_users", "analytics_chats", "analytics_meta", "analytics_summary"} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		meta := tx.Bucket([]byte("analytics_meta"))
		salt := meta.Get([]byte("salt"))
		if len(salt) == 0 {
			salt = make([]byte, 32)
			if _, err := rand.Read(salt); err != nil {
				return err
			}
			if err := meta.Put([]byte("salt"), salt); err != nil {
				return err
			}
		}
		c.salt = append([]byte(nil), salt...)
		return nil
	})
}

func (c *cache) anonymousID(kind byte, id int64) []byte {
	mac := hmac.New(sha256.New, c.salt)
	mac.Write([]byte{kind})
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], uint64(id))
	mac.Write(raw[:])
	return mac.Sum(nil)
}

func (c *cache) recordMessage(userID, chatID int64, chatType string, urlCount int) error {
	now := time.Now().Unix()
	return c.db.Update(func(tx *bolt.Tx) error {
		if userID != 0 {
			if err := tx.Bucket([]byte("analytics_users")).Put(c.anonymousID('u', userID), []byte(strconv.FormatInt(now, 10))); err != nil {
				return err
			}
		}
		if err := tx.Bucket([]byte("analytics_chats")).Put(c.anonymousID('c', chatID), []byte(chatType)); err != nil {
			return err
		}
		s, err := readSummary(tx)
		if err != nil {
			return err
		}
		if s.StartedAt == 0 {
			s.StartedAt = now
		}
		s.LastSeenAt = now
		s.MessagesWithURLs++
		s.URLsSubmitted += uint64(urlCount)
		return writeSummary(tx, s)
	})
}

func (c *cache) recordOutcome(success, cacheHit bool) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		s, err := readSummary(tx)
		if err != nil {
			return err
		}
		if success {
			s.SuccessfulDeliveries++
			if cacheHit {
				s.CacheHits++
			}
		} else {
			s.Failures++
		}
		return writeSummary(tx, s)
	})
}

func readSummary(tx *bolt.Tx) (analyticsSummary, error) {
	var s analyticsSummary
	raw := tx.Bucket([]byte("analytics_summary")).Get([]byte("totals"))
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &s); err != nil {
			return s, err
		}
	}
	return s, nil
}

func writeSummary(tx *bolt.Tx, s analyticsSummary) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return tx.Bucket([]byte("analytics_summary")).Put([]byte("totals"), raw)
}

func (c *cache) reportAnalytics(w io.Writer) error {
	return c.db.View(func(tx *bolt.Tx) error {
		s, err := readSummary(tx)
		if err != nil {
			return err
		}
		users := tx.Bucket([]byte("analytics_users")).Stats().KeyN
		chats := tx.Bucket([]byte("analytics_chats"))
		private, groups := 0, 0
		_ = chats.ForEach(func(_, value []byte) error {
			if string(value) == "private" {
				private++
			} else {
				groups++
			}
			return nil
		})
		fmt.Fprintln(w, "METRIC\tVALUE")
		fmt.Fprintf(w, "unique_users\t%d\nunique_chats\t%d\nprivate_chats\t%d\ngroup_chats\t%d\n", users, chats.Stats().KeyN, private, groups)
		fmt.Fprintf(w, "messages_with_urls\t%d\nurls_submitted\t%d\nsuccessful_deliveries\t%d\ncache_hits\t%d\nfailures\t%d\n", s.MessagesWithURLs, s.URLsSubmitted, s.SuccessfulDeliveries, s.CacheHits, s.Failures)
		if s.StartedAt > 0 {
			fmt.Fprintf(w, "tracking_since_utc\t%s\nlast_activity_utc\t%s\n", time.Unix(s.StartedAt, 0).UTC().Format(time.RFC3339), time.Unix(s.LastSeenAt, 0).UTC().Format(time.RFC3339))
		}
		return nil
	})
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

func (c *cache) getFileIDs(k string) ([]string, error) {
	raw, err := c.get(k)
	if err != nil || raw == "" {
		return nil, err
	}
	if !strings.HasPrefix(raw, "[") {
		return []string{raw}, nil
	}
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

func (c *cache) putFileIDs(k string, ids []string) error {
	if len(ids) == 1 {
		return c.put(k, ids[0])
	}
	raw, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	return c.put(k, string(raw))
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

func (a *api) keepVideoProgress(ctx context.Context, chatID int64) {
	ticker := time.NewTicker(4 * time.Second)
	defer ticker.Stop()
	for {
		_ = a.call(ctx, "sendChatAction", map[string]any{"chat_id": chatID, "action": "upload_video"}, nil)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *api) sendCached(ctx context.Context, j job, ids []string) error {
	if len(ids) == 1 {
		return a.call(ctx, "sendVideo", map[string]any{"chat_id": j.ChatID, "reply_parameters": map[string]any{"message_id": j.MessageID}, "video": ids[0]}, nil)
	}
	media := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		media = append(media, map[string]any{"type": "video", "media": id, "supports_streaming": true})
	}
	return a.call(ctx, "sendMediaGroup", map[string]any{"chat_id": j.ChatID, "reply_parameters": map[string]any{"message_id": j.MessageID}, "media": media}, nil)
}

func (a *api) upload(ctx context.Context, j job, videos []preparedVideo) ([]string, error) {
	if len(videos) == 1 {
		id, err := a.uploadOne(ctx, j, videos[0])
		if err != nil {
			return nil, err
		}
		return []string{id}, nil
	}
	return a.uploadAlbum(ctx, j, videos)
}

func (a *api) uploadOne(ctx context.Context, j job, video preparedVideo) (string, error) {
	f, e := os.Open(video.Path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	_ = w.WriteField("chat_id", strconv.FormatInt(j.ChatID, 10))
	_ = w.WriteField("reply_parameters", fmt.Sprintf(`{"message_id":%d}`, j.MessageID))
	_ = w.WriteField("width", strconv.Itoa(video.Meta.Width))
	_ = w.WriteField("height", strconv.Itoa(video.Meta.Height))
	_ = w.WriteField("duration", strconv.Itoa(video.Meta.Duration))
	_ = w.WriteField("supports_streaming", "true")
	p, e := w.CreateFormFile("video", filepath.Base(video.Path))
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

func (a *api) uploadAlbum(ctx context.Context, j job, videos []preparedVideo) ([]string, error) {
	if len(videos) < 2 || len(videos) > maxVideosPerPost {
		return nil, fmt.Errorf("invalid album size %d", len(videos))
	}
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	_ = w.WriteField("chat_id", strconv.FormatInt(j.ChatID, 10))
	_ = w.WriteField("reply_parameters", fmt.Sprintf(`{"message_id":%d}`, j.MessageID))
	media := make([]map[string]any, 0, len(videos))
	for i, video := range videos {
		name := fmt.Sprintf("video%d", i)
		media = append(media, map[string]any{
			"type":               "video",
			"media":              "attach://" + name,
			"width":              video.Meta.Width,
			"height":             video.Meta.Height,
			"duration":           video.Meta.Duration,
			"supports_streaming": true,
		})
		f, err := os.Open(video.Path)
		if err != nil {
			return nil, err
		}
		part, err := w.CreateFormFile(name, filepath.Base(video.Path))
		if err == nil {
			_, err = io.Copy(part, f)
		}
		closeErr := f.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	mediaJSON, err := json.Marshal(media)
	if err != nil {
		return nil, err
	}
	if err := w.WriteField("media", string(mediaJSON)); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", a.base+"/sendMediaGroup", &b)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	var response tgResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	if !response.OK {
		return nil, errors.New(response.Description)
	}
	var sent []sentMessage
	if err := json.Unmarshal(response.Result, &sent); err != nil {
		return nil, err
	}
	if len(sent) != len(videos) {
		return nil, errors.New("Telegram returned an incomplete media group")
	}
	ids := make([]string, 0, len(sent))
	for _, message := range sent {
		if message.Video == nil || message.Video.FileID == "" {
			return nil, errors.New("missing video file_id in media group")
		}
		ids = append(ids, message.Video.FileID)
	}
	return ids, nil
}
