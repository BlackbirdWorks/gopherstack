package azureblob_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func TestParseRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		header    string
		size      int64
		wantStart int64
		wantEnd   int64
		wantOK    bool
	}{
		{name: "start_end", header: "bytes=2-5", size: 10, wantStart: 2, wantEnd: 5, wantOK: true},
		{name: "open_ended", header: "bytes=7-", size: 10, wantStart: 7, wantEnd: 9, wantOK: true},
		{name: "suffix", header: "bytes=-3", size: 10, wantStart: 7, wantEnd: 9, wantOK: true},
		{name: "suffix_larger_than_size", header: "bytes=-100", size: 10, wantStart: 0, wantEnd: 9, wantOK: true},
		{name: "end_beyond_size_clamped", header: "bytes=5-1000", size: 10, wantStart: 5, wantEnd: 9, wantOK: true},
		{name: "no_bytes_prefix", header: "items=0-1", size: 10, wantOK: false},
		{name: "empty_header", header: "", size: 10, wantOK: false},
		{name: "multi_range_rejected", header: "bytes=0-1,3-4", size: 10, wantOK: false},
		{name: "start_beyond_size", header: "bytes=100-200", size: 10, wantOK: false},
		{name: "end_before_start", header: "bytes=5-2", size: 10, wantOK: false},
		{name: "malformed_no_dash", header: "bytes=abc", size: 10, wantOK: false},
		{name: "zero_size_suffix", header: "bytes=-5", size: 0, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			start, end, ok := azureblob.ParseRange(tt.header, tt.size)

			assert.Equal(t, tt.wantOK, ok, tt.name)
			if tt.wantOK {
				assert.Equal(t, tt.wantStart, start, tt.name)
				assert.Equal(t, tt.wantEnd, end, tt.name)
			}
		})
	}
}

func TestSplitPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		path          string
		wantAccount   string
		wantContainer string
		wantBlob      string
	}{
		{name: "empty", path: "", wantAccount: "", wantContainer: "", wantBlob: ""},
		{name: "account_only", path: "/devstoreaccount1", wantAccount: "devstoreaccount1"},
		{
			name: "account_and_container", path: "/devstoreaccount1/mycontainer",
			wantAccount: "devstoreaccount1", wantContainer: "mycontainer",
		},
		{
			name: "account_container_blob", path: "/devstoreaccount1/mycontainer/myblob.txt",
			wantAccount: "devstoreaccount1", wantContainer: "mycontainer", wantBlob: "myblob.txt",
		},
		{
			name: "blob_name_with_slashes", path: "/devstoreaccount1/mycontainer/dir/sub/file.txt",
			wantAccount: "devstoreaccount1", wantContainer: "mycontainer", wantBlob: "dir/sub/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			account, container, blob := azureblob.SplitPath(tt.path)

			assert.Equal(t, tt.wantAccount, account, tt.name)
			assert.Equal(t, tt.wantContainer, container, tt.name)
			assert.Equal(t, tt.wantBlob, blob, tt.name)
		})
	}
}
