package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestAnalyticsAggregatesWithoutRawIDs(t *testing.T) {
	db, err := bolt.Open(filepath.Join(t.TempDir(), "analytics.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := &cache{db: db}
	if err := c.init(); err != nil {
		t.Fatal(err)
	}
	if err := c.recordMessage(123456789, -987654321, "supergroup", 2); err != nil {
		t.Fatal(err)
	}
	if err := c.recordMessage(123456789, 555, "private", 1); err != nil {
		t.Fatal(err)
	}
	if err := c.recordOutcome(true, false); err != nil {
		t.Fatal(err)
	}
	if err := c.recordOutcome(true, true); err != nil {
		t.Fatal(err)
	}
	if err := c.recordOutcome(false, false); err != nil {
		t.Fatal(err)
	}
	var report bytes.Buffer
	if err := c.reportAnalytics(&report); err != nil {
		t.Fatal(err)
	}
	text := report.String()
	for _, want := range []string{"unique_users\t1", "unique_chats\t2", "private_chats\t1", "group_chats\t1", "urls_submitted\t3", "successful_deliveries\t2", "cache_hits\t1", "failures\t1"} {
		if !strings.Contains(text, want) {
			t.Fatalf("report missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "123456789") || strings.Contains(text, "987654321") {
		t.Fatal("report exposed a raw Telegram ID")
	}
}
