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

func main() {
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
		key := normalize(j.URL)
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
		cancel()
		if err != nil {
			_ = a.reply(ctx, j.ChatID, j.MessageID, "Could not download this video under the 50 MB limit.")
			os.RemoveAll(dir)
			continue
		}
		st, err := os.Stat(path)
		if err != nil || st.Size() > maxUpload {
			_ = a.reply(ctx, j.ChatID, j.MessageID, "The available video is too large for Telegram's 50 MB bot limit.")
			os.RemoveAll(dir)
			continue
		}
		fileID, err := a.upload(ctx, j, path)
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
	out := filepath.Join(dir, "video.%(ext)s")
	cmd := exec.CommandContext(ctx, "yt-dlp", "--no-playlist", "--no-warnings", "--max-filesize", "49M", "--match-filter", "!is_live & duration <=? 3600", "-f", "b[ext=mp4][filesize<49M]/b[filesize<49M]", "-o", out, "--", raw)
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
func (a *api) upload(ctx context.Context, j job, path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	_ = w.WriteField("chat_id", strconv.FormatInt(j.ChatID, 10))
	_ = w.WriteField("reply_parameters", fmt.Sprintf(`{"message_id":%d}`, j.MessageID))
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
