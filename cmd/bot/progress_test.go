package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestVideoProgressUsesNativeChatAction(t *testing.T) {
	received := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sendChatAction" {
			t.Errorf("path = %q", r.URL.Path)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		} else {
			received <- payload
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(t.Context())
	a := &api{base: server.URL, client: server.Client()}
	done := make(chan struct{})
	go func() {
		a.keepVideoProgress(ctx, 123)
		close(done)
	}()
	select {
	case payload := <-received:
		if payload["chat_id"] != float64(123) || payload["action"] != "upload_video" {
			t.Fatalf("unexpected payload: %#v", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("no chat action received")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("progress loop did not stop")
	}
}
