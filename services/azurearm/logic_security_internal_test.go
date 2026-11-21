package azurearm

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogicLocaleTableDecodes(t *testing.T) {
	t.Parallel()

	got, err := decodeLogicLocales(logicLocaleTable)
	require.NoError(t, err)
	assert.Len(t, got, 19)

	for key, l := range got {
		assert.NotEmpty(t, l.am, key)
		assert.NotEmpty(t, l.pm, key)
		assert.Len(t, l.std, len(logicLocaleStd), key)
	}
}

func TestLogicLocaleTableRejectsMalformedRows(t *testing.T) {
	t.Parallel()

	good := strings.Split(strings.Split(logicLocaleTable, "\n")[1], "\t")
	require.Len(t, good, logicLocaleCols)

	tests := []struct {
		mutate func(c []string)
		name   string
		cut    bool
	}{
		{name: "missing column", cut: true},
		{name: "short month list", mutate: func(c []string) { c[1] = "a|b" }},
		{name: "empty month name", mutate: func(c []string) { c[2] = strings.Replace(c[2], "Jan", "", 1) }},
		{name: "short day list", mutate: func(c []string) { c[3] = "x" }},
		{name: "short standard patterns", mutate: func(c []string) { c[7] = "d|D" }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			row := append([]string(nil), good...)
			if tc.cut {
				row = row[:len(row)-1]
			} else {
				tc.mutate(row)
			}

			_, err := decodeLogicLocales("# header\n" + strings.Join(row, "\t") + "\n")
			require.ErrorIs(t, err, errLogicLocaleRow)
			assert.Contains(t, err.Error(), "line 2")
		})
	}
}

func TestLogicLocaleNameMatchKeepsRuneBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		names   []string
		wantPos int
		wantIdx int
		wantOK  bool
	}{
		{"kelvin sign", "\u212aa", []string{"ka"}, len("\u212aa"), 0, true},
		{"dotted capital i is not an i", "İx", []string{"i"}, 0, 0, false},
		{"umlaut upper", "MÄRZ 2024", []string{"Januar", "März"}, len("MÄRZ"), 1, true},
		{"longest wins", "Juni", []string{"Jun", "Juni"}, len("Juni"), 1, true},
		{"cyrillic upper", "МАРТА 5", []string{"марта"}, len("МАРТА"), 0, true},
		{"no match", "xyz", []string{"abc"}, 0, 0, false},
		{"truncated input", "ma", []string{"mars"}, 0, 0, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := &timeScanner{s: tc.input}
			idx, ok := sc.name(tc.names)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantIdx, idx)
			assert.Equal(t, tc.wantPos, sc.pos)
			assert.True(t, utf8.ValidString(tc.input[sc.pos:]), "the cursor stays on a rune boundary")
		})
	}
}

func TestLogicLocaleMeridiemFoldsCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  string
		am, pm string
		wantPM bool
		wantOK bool
		pos    int
	}{
		{"upper pm", "PM 1", "am", "pm", true, true, 2},
		{"korean", "오후 2", "오전", "오후", true, true, len("오후")},
		{"russian upper", "ДП 1", "дп", "пп", false, true, len("ДП")},
		{"none", "xx", "am", "pm", false, false, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := &timeScanner{s: tc.input, loc: logicLocale{am: tc.am, pm: tc.pm}}
			pt := &parsedTime{}
			assert.Equal(t, tc.wantOK, sc.meridiem(pt))
			assert.Equal(t, tc.wantPM, pt.pm)
			assert.Equal(t, tc.pos, sc.pos)
		})
	}
}

func TestLogicLocalHostName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		host string
		want bool
	}{
		{"127.0.0.1", true},
		{"::1", true},
		{"localhost", true},
		{"LOCALHOST", true},
		{"gopherstack", true},
		{"evil.example.com", false},
		{"203.0.113.9", false},
		{"", false},
	}

	for _, tc := range tests {
		t.Run(tc.host, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, localHostName(tc.host))
		})
	}
}

func TestLogicSecretTracePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path string
		want bool
	}{
		{"inputs.authentication", true},
		{"inputs.Authentication.pfx", true},
		{"inputs.headers.Authorization", true},
		{"inputs.headers.cookie", true},
		{"inputs.cookie", true},
		{"inputs.headers.X-Keep", false},
		{"inputs.body", false},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, secretTracePath(tc.path))
		})
	}
}

func TestLogicRunBudget(t *testing.T) {
	t.Parallel()

	var b logicRunBudget

	assert.True(t, b.takeBody(logicMaxRunBodyBytes))
	assert.False(t, b.takeBody(1))

	for range logicMaxRunRequests {
		require.True(t, b.takeRequest())
	}

	assert.False(t, b.takeRequest())
	assert.Equal(t, 5, b.takeTraces(5))
	assert.Equal(t, logicMaxRunTraces-5, b.takeTraces(logicMaxRunTraces))
	assert.Zero(t, b.takeTraces(1))
}
