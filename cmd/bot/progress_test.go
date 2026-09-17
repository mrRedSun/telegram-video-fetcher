package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestVideoProgressUsesNativeTypingAction(t *testing.T) {
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

	a := &api{base: server.URL, client: server.Client()}
	if err := a.sendVideoProgress(t.Context(), 123); err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-received:
		if payload["chat_id"] != float64(123) || payload["action"] != "typing" {
			t.Fatalf("unexpected payload: %#v", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("no chat action received")
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		a.keepVideoProgress(ctx, 123)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("progress loop did not stop")
	}
}

func TestMessageProgressReactionCanBeSetAndRemoved(t *testing.T) {
	received := make(chan map[string]any, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/setMessageReaction" {
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

	a := &api{base: server.URL, client: server.Client()}
	if err := a.setMessageReaction(t.Context(), 123, 456, "👀"); err != nil {
		t.Fatal(err)
	}
	if err := a.setMessageReaction(t.Context(), 123, 456, ""); err != nil {
		t.Fatal(err)
	}
	setPayload := <-received
	removePayload := <-received
	setReactions, ok := setPayload["reaction"].([]any)
	if !ok || len(setReactions) != 1 {
		t.Fatalf("unexpected set payload: %#v", setPayload)
	}
	reaction, ok := setReactions[0].(map[string]any)
	if !ok || reaction["emoji"] != "👀" || setPayload["message_id"] != float64(456) {
		t.Fatalf("unexpected reaction: %#v", setPayload)
	}
	removeReactions, ok := removePayload["reaction"].([]any)
	if !ok || len(removeReactions) != 0 {
		t.Fatalf("unexpected remove payload: %#v", removePayload)
	}
}
