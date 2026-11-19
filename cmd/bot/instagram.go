package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Telegram's sendPhoto limits. Larger or more extreme images are sent as
// documents so they keep their original quality.
const (
	maxPhotoBytes     = int64(10_000_000)
	maxPhotoDimension = 10_000 // width + height
	maxPhotoRatio     = 20
)

// instagramSource reports whether raw is an Instagram link eligible for photo
// delivery and returns the URL handed to yt-dlp. Only the canonical web hosts
// and the official short domain qualify; every other host stays video-only.
// The short domain is rewritten because yt-dlp's Instagram extractor matches
// only instagram.com and instagr.am itself is not reliably reachable.
func instagramSource(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return raw, false
	}
	if u.Port() != "" && u.Port() != "80" && u.Port() != "443" {
		return raw, false
	}
	switch strings.TrimSuffix(strings.ToLower(u.Hostname()), ".") {
	case "instagram.com", "www.instagram.com", "instagr.am":
	default:
		return raw, false
	}
	canonical := url.URL{Scheme: "https", Host: "www.instagram.com", Path: u.Path, RawPath: u.RawPath, RawQuery: u.RawQuery}
	return canonical.String(), true
}

// instagramAllowsPhotos reports whether a canonical Instagram URL may yield
// photos. Reels and IGTV stay on the video-only path, so a Reel whose video
// cannot be resolved fails instead of silently delivering its cover image.
func instagramAllowsPhotos(source string) bool {
	u, err := url.Parse(source)
	if err != nil {
		return false
	}
	for _, segment := range strings.Split(strings.ToLower(u.Path), "/") {
		if segment == "reel" || segment == "reels" || segment == "tv" {
			return false
		}
	}
	return true
}

// allowedPhotoHost restricts photo downloads to Instagram's media CDNs so a
// manipulated metadata response cannot make the bot fetch arbitrary URLs.
func allowedPhotoHost(u *url.URL) bool {
	if u == nil || u.Scheme != "https" || u.User != nil || (u.Port() != "" && u.Port() != "443") {
		return false
	}
	h := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	return h == "cdninstagram.com" || strings.HasSuffix(h, ".cdninstagram.com") || h == "fbcdn.net" || strings.HasSuffix(h, ".fbcdn.net")
}

var photoClient = &http.Client{
	Timeout: 2 * time.Minute,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 || !allowedPhotoHost(req.URL) {
			return errors.New("photo redirect rejected")
		}
		return nil
	},
}

// downloadPhoto stores one Instagram image in dir, bounded by the upload limit.
func downloadPhoto(ctx context.Context, client *http.Client, dir, raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || !allowedPhotoHost(u) {
		return "", errors.New("photo URL host is not an Instagram CDN")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	req.Header.Set("Referer", "https://www.instagram.com/")
	resp, err := client.Do(req)
	if err != nil {
		// The URL is signed; never include it in errors that reach the logs.
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", errors.New("photo request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("photo download HTTP %d", resp.StatusCode)
	}
	prefix := make([]byte, 512)
	n, err := io.ReadFull(resp.Body, prefix)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return "", errors.New("photo read failed")
	}
	prefix = prefix[:n]
	var ext string
	switch http.DetectContentType(prefix) {
	case "image/jpeg":
		ext = ".jpg"
	case "image/png":
		ext = ".png"
	default:
		return "", errors.New("photo is not JPEG or PNG")
	}
	path := filepath.Join(dir, "photo"+ext)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	written, copyErr := io.Copy(f, io.MultiReader(bytes.NewReader(prefix), io.LimitReader(resp.Body, maxUpload+1-int64(len(prefix)))))
	closeErr := f.Close()
	if copyErr != nil {
		return "", errors.New("photo read failed")
	}
	if closeErr != nil {
		return "", closeErr
	}
	if written > maxUpload {
		return "", errors.New("photo exceeds upload limit")
	}
	return path, nil
}

// preparePhoto validates a downloaded image and picks the Telegram media type
// that preserves it: a photo when within sendPhoto limits, otherwise a document.
func preparePhoto(path string) (preparedMedia, error) {
	f, err := os.Open(path)
	if err != nil {
		return preparedMedia{}, err
	}
	defer f.Close()
	cfg, _, err := image.DecodeConfig(f)
	if err != nil {
		return preparedMedia{}, fmt.Errorf("decode photo header: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		return preparedMedia{}, err
	}
	if cfg.Width < 1 || cfg.Height < 1 {
		return preparedMedia{}, errors.New("invalid photo geometry")
	}
	if st.Size() > maxUpload {
		return preparedMedia{}, errors.New("photo exceeds upload limit")
	}
	kind := kindPhoto
	if !fitsPhotoLimits(cfg.Width, cfg.Height, st.Size()) {
		kind = kindDocument
	}
	return preparedMedia{Kind: kind, Path: path, Meta: videoMeta{Width: cfg.Width, Height: cfg.Height}}, nil
}

func fitsPhotoLimits(width, height int, size int64) bool {
	long, short := max(width, height), min(width, height)
	return size <= maxPhotoBytes && width+height <= maxPhotoDimension && short > 0 && long <= short*maxPhotoRatio
}
