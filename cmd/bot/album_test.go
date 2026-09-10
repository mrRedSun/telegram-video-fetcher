package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestUploadAlbumUsesOneOrderedMediaGroup(t *testing.T) {
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
		if len(media) != 2 || media[0]["media"] != "attach://video0" || media[1]["media"] != "attach://video1" {
			t.Fatalf("unexpected media: %#v", media)
		}
		for i := range media {
			if _, _, err := r.FormFile(fmt.Sprintf("video%d", i)); err != nil {
				t.Fatalf("attachment %d: %v", i, err)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":[{"video":{"file_id":"first"}},{"video":{"file_id":"second"}}]}`))
	}))
	defer server.Close()

	dir := t.TempDir()
	videos := make([]preparedVideo, 2)
	for i := range videos {
		path := filepath.Join(dir, fmt.Sprintf("video-%d.mp4", i))
		if err := os.WriteFile(path, []byte(fmt.Sprintf("video-%d", i)), 0600); err != nil {
			t.Fatal(err)
		}
		videos[i] = preparedVideo{Path: path, Meta: videoMeta{Width: 1920, Height: 1080, Duration: 6}}
	}
	a := &api{base: server.URL, client: server.Client()}
	ids, err := a.uploadAlbum(t.Context(), job{ChatID: 1, MessageID: 2}, videos)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "first" || ids[1] != "second" {
		t.Fatalf("unexpected file IDs: %#v", ids)
	}
}
