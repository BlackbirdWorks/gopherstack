package wfinterp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

func TestParseJSONTrailingData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{name: "single", in: `{"a":1}`},
		{name: "trailing_space", in: "{\"a\":1} \n"},
		{name: "second_value", in: `{"a":1} {"b":2}`, wantErr: true},
		{name: "garbage", in: `{"a":1} x`, wantErr: true},
		{name: "invalid", in: `{"a":`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := wfinterp.ParseJSON([]byte(tt.in))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			assert.NoError(t, err)
		})
	}
}
