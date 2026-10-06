package s3

import (
	"bytes"
	"encoding/xml"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

func TestEscapeXMLString_MatchesEncoder(t *testing.T) {
	t.Parallel()

	var allBytes strings.Builder
	for i := range 256 {
		allBytes.WriteByte(byte(i))
	}

	tests := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"plain_ascii", "photos/2024/cat-01.jpg"},
		{"specials", `a&b<c>d"e'f`},
		{"all_bytes", allBytes.String()},
		{"del_and_controls", "a\x7fb\x1fc\x00d"},
		{"unicode", "日本語/é-😀"},
		{"invalid_utf8", invalidUTF8AndIllegalRunesKey},
		{"trailing_special", "abc&"},
		{"leading_special", "<abc"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			type payload struct {
				XMLName xml.Name `xml:"R"`
				K       string   `xml:"K"`
			}

			want, err := xml.Marshal(payload{K: tc.in})
			require.NoError(t, err)
			wantInner := strings.TrimSuffix(strings.TrimPrefix(string(want), "<R>"), "</R>")

			var got bytes.Buffer
			writeXMLElem(&got, "K", tc.in)

			require.Equal(t, wantInner, got.String())
		})
	}
}

func TestTimestampArena_MatchesFormat(t *testing.T) {
	t.Parallel()

	zone := time.FixedZone("x", 5*3600+1800)
	stamps := []time.Time{
		time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
		time.Date(2024, 1, 2, 3, 4, 5, 987654321, zone),
		{},
		time.Date(1999, 12, 31, 23, 59, 59, 0, time.FixedZone("neg", -8*3600)),
	}

	tests := []struct {
		name string
		n    int
	}{
		{"none", 0},
		{"one", 1},
		{"all", len(stamps)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			objects := make([]types.Object, tc.n)
			for i := range objects {
				objects[i].LastModified = aws.Time(stamps[i])
			}

			arena := newTimestampArena(objects)
			for i := range objects {
				require.Equal(t, stamps[i].Format(time.RFC3339), arena.at(i))
			}
		})
	}
}
