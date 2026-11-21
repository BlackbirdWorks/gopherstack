package gcpbigquery

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type oracleCSV struct {
	recs   [][]string
	rec    []string
	field  strings.Builder
	delim  rune
	quote  rune
	quoted bool
	wasQ   bool
	start  bool
}

func oracleSplit(text string, delim, q rune) ([][]string, error) {
	p := &oracleCSV{delim: delim, quote: q, start: true}
	rs := []rune(text)

	for i := 0; i < len(rs); i++ {
		skip, err := p.step(rs, i)
		if err != nil {
			return nil, err
		}

		i += skip
	}

	if p.quoted {
		return nil, fmt.Errorf("%w: unclosed quoted field", errLoad)
	}

	if len(p.rec) > 0 || p.field.Len() > 0 || p.wasQ {
		p.flush()

		p.recs = append(p.recs, p.rec)
	}

	return p.recs, nil
}

func (p *oracleCSV) flush() {
	p.rec = append(p.rec, p.field.String())
	p.field.Reset()
	p.wasQ, p.start = false, true
}

func (p *oracleCSV) step(rs []rune, i int) (int, error) {
	c := rs[i]

	var next rune
	if i+1 < len(rs) {
		next = rs[i+1]
	}

	if p.quoted {
		return p.inQuotes(c, next), nil
	}

	switch {
	case p.start && c == p.quote:
		p.quoted, p.wasQ, p.start = true, true, false
	case c == p.delim:
		p.flush()
	case c == '\n' || c == '\r' && next == '\n':
		p.flush()

		p.recs, p.rec = append(p.recs, p.rec), nil

		if c == '\r' {
			return 1, nil
		}
	case p.wasQ:
		return 0, fmt.Errorf("%w: data between close quote and field separator", errLoad)
	default:
		p.field.WriteRune(c)

		p.start = false
	}

	return 0, nil
}

func (p *oracleCSV) inQuotes(c, next rune) int {
	switch {
	case c == p.quote && next == p.quote:
		p.field.WriteRune(p.quote)

		return 1
	case c == p.quote:
		p.quoted = false
	default:
		p.field.WriteRune(c)
	}

	return 0
}

func TestSplitQuotedCSVMatchesRuneOracle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		delim rune
		quote rune
		seed  uint64
	}{
		{name: "ascii", delim: ',', quote: '\'', seed: 1},
		{name: "multibyte quote", delim: ',', quote: '«', seed: 2},
		{name: "multibyte delim", delim: 'é', quote: '\'', seed: 3},
		{name: "multibyte both", delim: '€', quote: '«', seed: 4},
		{name: "delim equals quote", delim: '\'', quote: '\'', seed: 5},
		{name: "replacement quote", delim: ',', quote: '�', seed: 6},
		{name: "tab delim", delim: '\t', quote: '"', seed: 7},
	}

	alphabet := []string{
		"a", "b", "é", "€", "«", "'", "\"", ",", "\t", "\n", "\r", "\r\n",
		"\xff", "\xc3", "\xe2\x82", " ", "\uFFFD",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewPCG(tt.seed, tt.seed))
			q := string(tt.quote)
			alpha := append([]string{string(tt.delim), q, q + q}, alphabet...)

			for range 4000 {
				var sb strings.Builder

				for range rng.IntN(24) {
					sb.WriteString(alpha[rng.IntN(len(alpha))])
				}

				in := sb.String()
				want, wantErr := oracleSplit(in, tt.delim, tt.quote)
				got, gotErr := splitQuotedCSV(in, tt.delim, tt.quote)

				if wantErr != nil {
					require.Error(t, gotErr, "input %q", in)
					require.ErrorIs(t, gotErr, errLoad)
					assert.Equal(t, wantErr.Error(), gotErr.Error(), "input %q", in)

					continue
				}

				require.NoError(t, gotErr, "input %q", in)
				require.Equal(t, want, got, "input %q", in)
			}
		})
	}
}
