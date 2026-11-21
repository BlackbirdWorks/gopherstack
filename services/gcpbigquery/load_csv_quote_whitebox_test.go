package gcpbigquery

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitQuotedCSV(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("é,", 3000)

	tests := []struct {
		name    string
		in      string
		wantErr string
		want    [][]string
		delim   rune
		quote   rune
	}{
		{"ascii", "1,'a,b'\n2,'it''s'\n", "", [][]string{{"1", "a,b"}, {"2", "it's"}}, ',', '\''},
		{"multibyte quote", "1,«a,b«\n2,«x««y«\n", "", [][]string{{"1", "a,b"}, {"2", "x«y"}}, ',', '«'},
		{"multibyte quote doubled", "»»»»\n", "", [][]string{{"»"}}, ',', '»'},
		{"multibyte delim", "aéb\n", "", [][]string{{"a", "b"}}, 'é', '\''},
		{"crlf in quotes", "'a\r\nb'\r\nc\r\n", "", [][]string{{"a\r\nb"}, {"c"}}, ',', '\''},
		{"unclosed", "'abc", "unclosed", nil, ',', '\''},
		{"junk after quote", "'a'b\n", "between close quote", nil, ',', '\''},
		{"invalid utf8 preserved", "a\xffb,'c\xfe'\n", "", [][]string{{"a�b", "c�"}}, ',', '\''},
		{"quote past 4k boundary", strings.Repeat("x", 4095) + ",'é''é'\n", "",
			[][]string{{strings.Repeat("x", 4095), "é'é"}}, ',', '\''},
		{"large multibyte fields", long + "\n", "", [][]string{strings.Split(long, ",")}, ',', '\''},
		{"no trailing newline", "'a',b", "", [][]string{{"a", "b"}}, ',', '\''},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := splitQuotedCSV(tt.in, tt.delim, tt.quote)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
