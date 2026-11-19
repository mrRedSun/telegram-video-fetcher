package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"image"
	"image/jpeg"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestInstagramSourceGatesHosts(t *testing.T) {
	cases := []struct {
		raw, want string
		ok        bool
	}{
		{"https://www.instagram.com/p/ABC123/", "https://www.instagram.com/p/ABC123/", true},
		{"https://instagram.com/p/ABC123/?img_index=2", "https://www.instagram.com/p/ABC123/?img_index=2", true},
		{"http://WWW.Instagram.COM/reel/XYZ987/", "https://www.instagram.com/reel/XYZ987/", true},
		{"https://instagr.am/p/ABC123/", "https://www.instagram.com/p/ABC123/", true},
		{"https://www.instagram.com./p/ABC123/#frag", "https://www.instagram.com/p/ABC123/", true},
		{"https://m.instagram.com/p/ABC123/", "", false},
		{"https://ddinstagram.com/p/ABC123/", "", false},
		{"https://instagram.com.evil.example/p/ABC123/", "", false},
		{"https://evil.example/instagram.com/p/ABC123/", "", false},
		{"https://user@instagram.com/p/ABC123/", "", false},
		{"https://instagram.com:8443/p/ABC123/", "", false},
		{"https://www.youtube.com/watch?v=abc", "", false},
		{"ftp://instagram.com/p/ABC123/", "", false},
	}
	for _, tc := range cases {
		got, ok := instagramSource(tc.raw)
		if ok != tc.ok {
			t.Errorf("%s: ok = %v, want %v", tc.raw, ok, tc.ok)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.raw, got, tc.want)
		}
		if !ok && got != tc.raw {
			t.Errorf("%s: non-Instagram URL was rewritten to %q", tc.raw, got)
		}
	}
}

func TestInstagramReelsStayVideoOnly(t *testing.T) {
	for raw, want := range map[string]bool{
		"https://www.instagram.com/p/ABC123/":          true,
		"https://www.instagram.com/someone/p/ABC123/":  true,
		"https://www.instagram.com/reel/ABC123/":       false,
		"https://www.instagram.com/reels/ABC123/":      false,
		"https://www.instagram.com/someone/reel/ABC1/": false,
		"https://www.instagram.com/tv/ABC123/":         false,
	} {
		if got := instagramAllowsPhotos(raw); got != want {
			t.Errorf("%s: got %v, want %v", raw, got, want)
		}
	}
}

func instagramEntry(t *testing.T, video bool, id string) json.RawMessage {
	t.Helper()
	item := map[string]any{
		"id":         id,
		"thumbnail":  "https://scontent.cdninstagram.com/" + id + "-full.jpg",
		"thumbnails": []map[string]string{{"url": "https://scontent.cdninstagram.com/" + id + "-small.jpg"}},
		"formats":    []any{},
	}
	if video {
		item["extractor_key"] = "Instagram"
		item["formats"] = []map[string]any{{"format_id": id + "-mp4", "ext": "mp4"}}
	}
	raw, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func instagramPost(t *testing.T, entries ...json.RawMessage) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"_type": "playlist", "entries": entries})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestPlansMixedCarouselInOrder(t *testing.T) {
	raw := instagramPost(t, instagramEntry(t, false, "a"), instagramEntry(t, true, "b"), instagramEntry(t, false, "c"))
	plans, err := plansFromMetadata(raw, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 3 {
		t.Fatalf("got %d plans", len(plans))
	}
	if plans[0].Kind != kindPhoto || plans[0].PhotoURL != "https://scontent.cdninstagram.com/a-full.jpg" {
		t.Errorf("item 1: %#v", plans[0])
	}
	if plans[1].Kind != kindVideo || plans[1].Selector != "b-mp4" || len(plans[1].Info) == 0 {
		t.Errorf("item 2: %#v", plans[1])
	}
	if plans[2].Kind != kindPhoto || plans[2].PhotoURL != "https://scontent.cdninstagram.com/c-full.jpg" {
		t.Errorf("item 3: %#v", plans[2])
	}
}

func TestPlansSinglePhotoPostAndThumbnailFallback(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{
		"formats":    []any{},
		"thumbnails": []map[string]string{{"url": "https://x.cdninstagram.com/small.jpg"}, {"url": "https://x.cdninstagram.com/best.jpg"}},
	})
	plans, err := plansFromMetadata(raw, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 || plans[0].Kind != kindPhoto || plans[0].PhotoURL != "https://x.cdninstagram.com/best.jpg" {
		t.Fatalf("unexpected plans: %#v", plans)
	}
	if _, err := plansFromMetadata([]byte(`{"formats":[]}`), true); err == nil {
		t.Fatal("item without formats or image was accepted")
	}
}

func TestPhotoEntriesStayRejectedOutsideInstagram(t *testing.T) {
	raw := instagramPost(t, instagramEntry(t, false, "a"), instagramEntry(t, true, "b"))
	if _, err := plansFromMetadata(raw, false); err == nil {
		t.Fatal("photo entry was planned for a non-Instagram source")
	}
}

func TestMixedCarouselCappedAtFiveItems(t *testing.T) {
	entries := make([]json.RawMessage, 0, 8)
	for i := 0; i < 8; i++ {
		entries = append(entries, instagramEntry(t, i%3 == 1, string(rune('a'+i))))
	}
	plans, err := plansFromMetadata(instagramPost(t, entries...), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != maxAlbumItems {
		t.Fatalf("got %d plans, want %d", len(plans), maxAlbumItems)
	}
	want := []mediaKind{kindPhoto, kindVideo, kindPhoto, kindPhoto, kindVideo}
	for i, plan := range plans {
		if plan.Kind != want[i] {
			t.Errorf("item %d kind %s, want %s", i+1, plan.Kind, want[i])
		}
	}
	if plans[4].Selector != "e-mp4" {
		t.Errorf("ordering lost: %#v", plans[4])
	}
}

func TestUploadMixedAlbumMultipart(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sendMediaGroup" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatal(err)
		}
		var media []map[string]any
		if err := json.Unmarshal([]byte(r.FormValue("media")), &media); err != nil {
			t.Fatal(err)
		}
		want := []struct{ typ, attach, body string }{
			{"photo", "attach://photo0", "jpeg-0"},
			{"video", "attach://video1", "mp4-1"},
			{"photo", "attach://photo2", "jpeg-2"},
		}
		if len(media) != len(want) {
			t.Fatalf("media = %#v", media)
		}
		for i, w := range want {
			if media[i]["type"] != w.typ || media[i]["media"] != w.attach {
				t.Errorf("item %d = %#v", i, media[i])
			}
			f, _, err := r.FormFile(strings.TrimPrefix(w.attach, "attach://"))
			if err != nil {
				t.Fatalf("attachment %d: %v", i, err)
			}
			body, _ := io.ReadAll(f)
			if string(body) != w.body {
				t.Errorf("attachment %d body %q", i, body)
			}
		}
		if _, ok := media[0]["duration"]; ok {
			t.Error("photo item carries video fields")
		}
		if media[1]["supports_streaming"] != true || media[1]["duration"] != float64(6) {
			t.Errorf("video item fields: %#v", media[1])
		}
		_, _ = w.Write([]byte(`{"ok":true,"result":[
			{"photo":[{"file_id":"p0-small"},{"file_id":"p0-large"}]},
			{"video":{"file_id":"v1"}},
			{"photo":[{"file_id":"p2-small"},{"file_id":"p2-large"}]}]}`))
	}))
	defer server.Close()

	dir := t.TempDir()
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	items := []preparedMedia{
		{Kind: kindPhoto, Path: write("0.jpg", "jpeg-0"), Meta: videoMeta{Width: 1080, Height: 1350}},
		{Kind: kindVideo, Path: write("1.mp4", "mp4-1"), Meta: videoMeta{Width: 720, Height: 1280, Duration: 6}},
		{Kind: kindPhoto, Path: write("2.jpg", "jpeg-2"), Meta: videoMeta{Width: 1080, Height: 1080}},
	}
	a := &api{base: server.URL, client: server.Client()}
	got, err := a.uploadAlbum(t.Context(), job{ChatID: 1, MessageID: 2}, items)
	if err != nil {
		t.Fatal(err)
	}
	want := []cachedMedia{{kindPhoto, "p0-large"}, {kindVideo, "v1"}, {kindPhoto, "p2-large"}}
	if len(got) != len(want) {
		t.Fatalf("got %#v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("item %d = %#v, want %#v", i, got[i], want[i])
		}
	}
}

func TestUploadAlbumRejectsDocumentsAndMissingPhotoIDs(t *testing.T) {
	a := &api{base: "http://127.0.0.1:1", client: http.DefaultClient}
	items := []preparedMedia{{Kind: kindPhoto, Path: "x"}, {Kind: kindDocument, Path: "y"}}
	if _, err := a.uploadAlbum(t.Context(), job{}, items); err == nil {
		t.Fatal("document accepted in album")
	}
	if _, ok := sentFileID(sentMessage{Video: &tgFile{FileID: "v"}}, kindPhoto); ok {
		t.Fatal("video file_id accepted for photo item")
	}
}

func TestUploadOneUsesPhotoAndDocumentMethods(t *testing.T) {
	for _, tc := range []struct {
		kind           mediaKind
		path, response string
	}{
		{kindPhoto, "/sendPhoto", `{"ok":true,"result":{"photo":[{"file_id":"s"},{"file_id":"l"}]}}`},
		{kindDocument, "/sendDocument", `{"ok":true,"result":{"document":{"file_id":"l"}}}`},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != tc.path {
				t.Errorf("path = %q, want %q", r.URL.Path, tc.path)
			}
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Fatal(err)
			}
			if _, _, err := r.FormFile(string(tc.kind)); err != nil {
				t.Errorf("missing %s file: %v", tc.kind, err)
			}
			if r.FormValue("duration") != "" {
				t.Error("non-video upload carries video fields")
			}
			_, _ = w.Write([]byte(tc.response))
		}))
		path := filepath.Join(t.TempDir(), "photo.jpg")
		if err := os.WriteFile(path, []byte("jpeg"), 0600); err != nil {
			t.Fatal(err)
		}
		a := &api{base: server.URL, client: server.Client()}
		got, err := a.upload(t.Context(), job{ChatID: 1, MessageID: 2}, []preparedMedia{{Kind: tc.kind, Path: path}})
		server.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0] != (cachedMedia{tc.kind, "l"}) {
			t.Fatalf("%s: got %#v", tc.kind, got)
		}
	}
}

func TestSendCachedUsesStoredTypes(t *testing.T) {
	requests := make(chan struct {
		path    string
		payload map[string]any
	}, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		requests <- struct {
			path    string
			payload map[string]any
		}{r.URL.Path, payload}
		_, _ = w.Write([]byte(`{"ok":true,"result":{}}`))
	}))
	defer server.Close()
	a := &api{base: server.URL, client: server.Client()}

	if err := a.sendCached(t.Context(), job{ChatID: 1, MessageID: 2}, []cachedMedia{{kindPhoto, "p"}}); err != nil {
		t.Fatal(err)
	}
	got := <-requests
	if got.path != "/sendPhoto" || got.payload["photo"] != "p" {
		t.Fatalf("single photo: %#v", got)
	}
	if err := a.sendCached(t.Context(), job{ChatID: 1, MessageID: 2}, []cachedMedia{{kindDocument, "d"}}); err != nil {
		t.Fatal(err)
	}
	got = <-requests
	if got.path != "/sendDocument" || got.payload["document"] != "d" {
		t.Fatalf("single document: %#v", got)
	}
	if err := a.sendCached(t.Context(), job{ChatID: 1, MessageID: 2}, []cachedMedia{{kindPhoto, "p"}, {kindVideo, "v"}}); err != nil {
		t.Fatal(err)
	}
	got = <-requests
	media, _ := got.payload["media"].([]any)
	if got.path != "/sendMediaGroup" || len(media) != 2 {
		t.Fatalf("album: %#v", got)
	}
	first, second := media[0].(map[string]any), media[1].(map[string]any)
	if first["type"] != "photo" || first["media"] != "p" || second["type"] != "video" || second["media"] != "v" {
		t.Fatalf("album items: %#v", media)
	}
}

func TestCachedMediaTypeRoundTrip(t *testing.T) {
	db, err := bolt.Open(filepath.Join(t.TempDir(), "cache.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := &cache{db: db}
	if err := c.init(); err != nil {
		t.Fatal(err)
	}
	mixed := []cachedMedia{{kindPhoto, "p1"}, {kindVideo, "v2"}, {kindPhoto, "p3"}}
	if err := c.putMedia("media-v5:a", mixed); err != nil {
		t.Fatal(err)
	}
	got, err := c.getMedia("media-v5:a")
	if err != nil || len(got) != 3 || got[0] != mixed[0] || got[1] != mixed[1] || got[2] != mixed[2] {
		t.Fatalf("mixed round trip: %#v, %v", got, err)
	}

	// Values written before photo support must still decode as videos.
	if err := c.put("media-v4:legacy-one", "fid"); err != nil {
		t.Fatal(err)
	}
	if err := c.put("media-v4:legacy-album", `["a","b"]`); err != nil {
		t.Fatal(err)
	}
	got, err = c.getMedia("media-v4:legacy-one")
	if err != nil || len(got) != 1 || got[0] != (cachedMedia{kindVideo, "fid"}) {
		t.Fatalf("legacy single: %#v, %v", got, err)
	}
	got, err = c.getMedia("media-v4:legacy-album")
	if err != nil || len(got) != 2 || got[1] != (cachedMedia{kindVideo, "b"}) {
		t.Fatalf("legacy album: %#v, %v", got, err)
	}

	// Video-only results keep the legacy encoding byte for byte.
	for _, tc := range []struct {
		items []cachedMedia
		want  string
	}{
		{[]cachedMedia{{kindVideo, "one"}}, "one"},
		{[]cachedMedia{{kindVideo, "a"}, {kindVideo, "b"}}, `["a","b"]`},
		{[]cachedMedia{{kindDocument, "d"}}, `[{"type":"document","file_id":"d"}]`},
	} {
		if raw, err := encodeCachedMedia(tc.items); err != nil || raw != tc.want {
			t.Errorf("encode %#v = %q, %v; want %q", tc.items, raw, err, tc.want)
		}
	}
	if _, err := decodeCachedMedia(`[{"type":"sticker","file_id":"x"}]`); err == nil {
		t.Error("unknown cached type accepted")
	}
	if _, err := encodeCachedMedia(nil); err == nil {
		t.Error("empty media list encoded")
	}
}

func jpegBytes(t *testing.T, width, height int) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := jpeg.Encode(&b, image.NewRGBA(image.Rect(0, 0, width, height)), nil); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestPreparePhotoChoosesPhotoOrDocument(t *testing.T) {
	dir := t.TempDir()
	normal := filepath.Join(dir, "normal.jpg")
	tall := filepath.Join(dir, "tall.jpg")
	if err := os.WriteFile(normal, jpegBytes(t, 108, 135), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tall, jpegBytes(t, 10, 250), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := preparePhoto(normal)
	if err != nil || got.Kind != kindPhoto || got.Meta.Width != 108 || got.Meta.Height != 135 {
		t.Fatalf("normal photo: %#v, %v", got, err)
	}
	got, err = preparePhoto(tall)
	if err != nil || got.Kind != kindDocument {
		t.Fatalf("over-ratio photo: %#v, %v", got, err)
	}
	if fitsPhotoLimits(1080, 1350, maxPhotoBytes+1) || fitsPhotoLimits(6000, 4001, 1) || !fitsPhotoLimits(1440, 1800, maxPhotoBytes) {
		t.Fatal("photo limit boundaries wrong")
	}
	if _, err := prepareMedia(t.Context(), []downloadedMedia{{kindPhoto, normal}, {kindPhoto, tall}}); err == nil {
		t.Fatal("album with over-limit photo accepted")
	}
	broken := filepath.Join(dir, "broken.jpg")
	if err := os.WriteFile(broken, []byte("not an image"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := preparePhoto(broken); err == nil {
		t.Fatal("undecodable photo accepted")
	}
}

// cdnClient routes every request to server regardless of host, so tests can
// use realistic Instagram CDN hostnames.
func cdnClient(server *httptest.Server) *http.Client {
	addr := server.Listener.Addr().String()
	client := *photoClient
	client.Transport = &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	return &client
}

func TestDownloadPhotoBoundsHostsAndContent(t *testing.T) {
	photo := jpegBytes(t, 40, 50)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok.jpg":
			if r.Header.Get("Referer") != "https://www.instagram.com/" {
				t.Errorf("referer = %q", r.Header.Get("Referer"))
			}
			_, _ = w.Write(photo)
		case "/redirect-out":
			http.Redirect(w, r, "https://evil.example/x.jpg", http.StatusFound)
		case "/html":
			_, _ = w.Write([]byte("<html><body>login</body></html>"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := cdnClient(server)
	dir := t.TempDir()

	path, err := downloadPhoto(t.Context(), client, dir, "https://scontent-waw2-1.cdninstagram.com/ok.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if body, _ := os.ReadFile(path); !bytes.Equal(body, photo) || filepath.Ext(path) != ".jpg" {
		t.Fatalf("stored %s with %d bytes", path, len(body))
	}
	for _, raw := range []string{
		"https://evil.example/ok.jpg",
		"http://scontent.cdninstagram.com/ok.jpg",
		"https://cdninstagram.com.evil.example/ok.jpg",
		"https://scontent.cdninstagram.com/redirect-out",
		"https://scontent.cdninstagram.com/html",
		"https://scontent.cdninstagram.com/missing",
	} {
		if _, err := downloadPhoto(t.Context(), client, t.TempDir(), raw); err == nil {
			t.Errorf("%s: accepted", raw)
		} else if strings.Contains(err.Error(), "cdninstagram") || strings.Contains(err.Error(), "evil") {
			t.Errorf("%s: error leaks URL: %v", raw, err)
		}
	}
}

func TestProcessJobReplaysTypedInstagramCache(t *testing.T) {
	db, err := bolt.Open(filepath.Join(t.TempDir(), "cache.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := &cache{db: db}
	if err := c.init(); err != nil {
		t.Fatal(err)
	}
	// instagr.am and instagram.com share the canonical v5 key.
	if err := c.putMedia("media-v5:https://www.instagram.com/p/ABC123/", []cachedMedia{{kindPhoto, "p"}, {kindVideo, "v"}}); err != nil {
		t.Fatal(err)
	}
	paths := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		_, _ = w.Write([]byte(`{"ok":true,"result":[]}`))
	}))
	defer server.Close()
	a := &api{base: server.URL, client: server.Client()}
	processJob(t.Context(), config{TempDir: t.TempDir(), Timeout: time.Second}, a, c, job{ChatID: 1, MessageID: 2, ChatType: "private", URL: "https://instagr.am/p/ABC123/"})
	if got := <-paths; got != "/sendMediaGroup" {
		t.Fatalf("path = %q", got)
	}
	var out bytes.Buffer
	if err := c.reportAnalytics(&out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "cache_hits\t1") || !strings.Contains(out.String(), "successful_deliveries\t1") {
		t.Fatalf("analytics:\n%s", out.String())
	}
}
