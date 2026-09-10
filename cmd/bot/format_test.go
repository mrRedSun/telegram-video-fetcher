package main

import (
	"encoding/json"
	"testing"
)

func TestSelectsBestCompatibleSeparateStreams(t *testing.T) {
	info := mediaInfo{Duration: 10, Formats: []mediaFormat{
		{ID: "low", Ext: "mp4", VideoCodec: "avc1", Height: 720, FileSize: 2_000_000},
		{ID: "best", Ext: "mp4", VideoCodec: "avc1", Height: 1080, FileSize: 10_000_000},
		{ID: "oversize", Ext: "mp4", VideoCodec: "avc1", Height: 2160, FileSize: 49_000_000},
		{ID: "audio", Ext: "m4a", AudioCodec: "mp4a", FileSize: 600_000, FormatNote: "original (default)"},
	}}
	got, ok := selectFormat(info, 48_000_000)
	if !ok || got.Selector != "best+audio" {
		t.Fatalf("got %#v, ok=%v", got, ok)
	}
}

func TestUnknownSizeUsesConservative1080pFallback(t *testing.T) {
	info := mediaInfo{Formats: []mediaFormat{
		{ID: "4k", Ext: "mp4", VideoCodec: "avc1", Width: 2160, Height: 3840},
		{ID: "1080", Ext: "mp4", VideoCodec: "avc1", Width: 1080, Height: 1920},
		{ID: "audio", Ext: "m4a", AudioCodec: "mp4a", FormatNote: "original"},
	}}
	got, ok := selectFormat(info, 48_000_000)
	if !ok || got.Selector != "1080+audio" {
		t.Fatalf("got %#v, ok=%v", got, ok)
	}
}

func TestCombinedFormatIsSupported(t *testing.T) {
	info := mediaInfo{Formats: []mediaFormat{
		{ID: "combined", Ext: "mp4", VideoCodec: "avc1", AudioCodec: "mp4a", Height: 720, FileSize: 5_000_000},
	}}
	got, ok := selectFormat(info, 48_000_000)
	if !ok || got.Selector != "combined" {
		t.Fatalf("got %#v, ok=%v", got, ok)
	}
}

func TestProgressiveCombinedBeatsUnknownDASHPair(t *testing.T) {
	info := mediaInfo{Extractor: "Instagram", Formats: []mediaFormat{
		{ID: "progressive", Ext: "mp4"},
		{ID: "dash-video", Ext: "mp4", VideoCodec: "vp9", Width: 720, Height: 1280},
		{ID: "dash-audio", Ext: "m4a", AudioCodec: "mp4a"},
	}}
	got, ok := selectFormat(info, 48_000_000)
	if !ok || got.Selector != "progressive" {
		t.Fatalf("got %#v, ok=%v", got, ok)
	}
}

func TestResolutionBeatsLowQualityCombinedFormat(t *testing.T) {
	info := mediaInfo{Formats: []mediaFormat{
		{ID: "combined-360", Ext: "mp4", VideoCodec: "avc1", AudioCodec: "mp4a", Width: 640, Height: 360, FileSize: 4_000_000},
		{ID: "video-4k", Ext: "mp4", VideoCodec: "av01", Width: 2160, Height: 3840, FileSize: 20_000_000},
		{ID: "audio", Ext: "m4a", AudioCodec: "mp4a", FileSize: 1_000_000},
	}}
	got, ok := selectFormat(info, 48_000_000)
	if !ok || got.Selector != "video-4k+audio" {
		t.Fatalf("got %#v, ok=%v", got, ok)
	}
}

func TestTranscodeBitratesFitTarget(t *testing.T) {
	video, audio, err := transcodeBitrates(110.333, true)
	if err != nil {
		t.Fatal(err)
	}
	if audio != 128_000 {
		t.Fatalf("audio bitrate = %d, want 128000", audio)
	}
	projected := float64(video+audio) * 110.333 / 8
	if projected >= float64(transcodeTarget) || projected < 45_000_000 {
		t.Fatalf("projected output = %.0f bytes", projected)
	}
}

func TestTranscodeBitratesReduceAudioForLongVideo(t *testing.T) {
	video, audio, err := transcodeBitrates(3600, true)
	if err != nil {
		t.Fatal(err)
	}
	if video < 50_000 || audio != 32_000 {
		t.Fatalf("video=%d audio=%d", video, audio)
	}
}

func TestPlansEveryVideoInMultiVideoPost(t *testing.T) {
	entry := func(id string) json.RawMessage {
		raw, err := json.Marshal(mediaInfo{Duration: 5, Formats: []mediaFormat{{ID: id, Ext: "mp4", VideoCodec: "avc1", AudioCodec: "aac", Width: 1920, Height: 1080, FileSize: 2_000_000}}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	raw, err := json.Marshal(mediaInfo{Entries: []json.RawMessage{entry("first"), entry("second")}})
	if err != nil {
		t.Fatal(err)
	}
	plans, err := plansFromMetadata(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 2 || plans[0].Selector != "first" || plans[1].Selector != "second" {
		t.Fatalf("unexpected plans: %#v", plans)
	}
}

func TestMultiVideoPostIsCappedAtFiveItems(t *testing.T) {
	entries := make([]json.RawMessage, 0, 7)
	for i := 0; i < 7; i++ {
		raw, err := json.Marshal(mediaInfo{Duration: 5, Formats: []mediaFormat{{ID: "video", Ext: "mp4", VideoCodec: "avc1", AudioCodec: "aac", FileSize: 1_000_000}}})
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, raw)
	}
	raw, err := json.Marshal(mediaInfo{Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	plans, err := plansFromMetadata(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != maxVideosPerPost {
		t.Fatalf("got %d plans, want %d", len(plans), maxVideosPerPost)
	}
}
