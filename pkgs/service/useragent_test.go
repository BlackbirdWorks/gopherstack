package service_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func TestMatchesUserAgentMarker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		header  http.Header
		name    string
		markers []string
		want    bool
	}{
		{
			name: "native SDK sets User-Agent, no X-Amz-User-Agent",
			header: http.Header{
				"User-Agent": []string{"aws-sdk-go-v2/1.0 api/docdb#1.0"},
			},
			markers: []string{"api/docdb"},
			want:    true,
		},
		{
			name: "browser SDK sets only X-Amz-User-Agent, real browser User-Agent present",
			header: http.Header{
				"User-Agent":       []string{"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"},
				"X-Amz-User-Agent": []string{"aws-sdk-js/3.1094.0 ua/2.1 os/linux lang/js api/DocDB/3.1094.0"},
			},
			markers: []string{"api/docdb"},
			want:    true,
		},
		{
			name: "case differs between Go SDK marker and JS SDK marker",
			header: http.Header{
				"X-Amz-User-Agent": []string{"api/Neptune/3.1094.0"},
			},
			markers: []string{"api/neptune"},
			want:    true,
		},
		{
			name: "second marker needed for JS SDK's hyphenated MediaStore Data serviceId",
			header: http.Header{
				"X-Amz-User-Agent": []string{"aws-sdk-js/3.1094.0 api/MediaStore-Data/3.1094.0"},
			},
			markers: []string{"mediastoredata", "mediastore-data"},
			want:    true,
		},
		{
			name: "Go SDK's unhyphenated MediaStore Data marker still matches",
			header: http.Header{
				"User-Agent": []string{"aws-sdk-go-v2/1.0.0 mediastoredata/1.29.18"},
			},
			markers: []string{"mediastoredata", "mediastore-data"},
			want:    true,
		},
		{
			name: "unrelated service does not match",
			header: http.Header{
				"X-Amz-User-Agent": []string{"aws-sdk-js/3.1094.0 api/AppSync/3.1094.0"},
			},
			markers: []string{"api/docdb"},
			want:    false,
		},
		{
			name:    "no headers at all",
			header:  http.Header{},
			markers: []string{"api/docdb"},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := service.MatchesUserAgentMarker(tt.header, tt.markers...)
			assert.Equal(t, tt.want, got)
		})
	}
}
