package main

import "testing"

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
		{ID: "4k", Ext: "mp4", VideoCodec: "avc1", Height: 2160},
		{ID: "1080", Ext: "mp4", VideoCodec: "avc1", Height: 1080},
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
