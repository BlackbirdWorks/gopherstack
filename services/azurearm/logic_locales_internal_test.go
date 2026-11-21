package azurearm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogicLocaleStandardFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want   map[string]string
		locale string
	}{
		{
			map[string]string{
				"d": "05/03/2024", "D": "Tuesday, 5 March 2024", "g": "05/03/2024 14:07", "T": "14:07:09",
			},
			"en-GB",
		},
		{
			map[string]string{"d": "05.03.2024", "D": "Dienstag, 5. März 2024", "f": "Dienstag, 5. März 2024 14:07"},
			"de-DE",
		},
		{map[string]string{"d": "05/03/2024", "D": "martedì 5 marzo 2024", "t": "14:07"}, "it-IT"},
		{map[string]string{"d": "05/03/2024", "D": "terça-feira, 5 de março de 2024"}, "pt-BR"},
		{map[string]string{"d": "05/03/2024", "D": "terça-feira, 5 de março de 2024"}, "pt-PT"},
		{map[string]string{"d": "05-03-2024", "D": "dinsdag 5 maart 2024"}, "nl-NL"},
		{map[string]string{"d": "2024-03-05", "D": "tisdag 5 mars 2024"}, "sv-SE"},
		{map[string]string{"d": "05/03/2024", "D": "tirsdag den 5. marts 2024"}, "da-DK"},
		{map[string]string{"d": "05.03.2024", "D": "tirsdag 5. mars 2024"}, "nb-NO"},
		{map[string]string{"d": "5.3.2024", "D": "tiistaina 5. maaliskuuta 2024", "t": "14.07"}, "fi-FI"},
		{map[string]string{"d": "05.03.2024", "D": "wtorek, 5 marca 2024"}, "pl-PL"},
		{map[string]string{"d": "2024/03/05", "D": "2024年3月5日火曜日", "t": "14:07"}, "ja-JP"},
		{map[string]string{"d": "2024/3/5", "D": "2024年3月5日星期二", "t": "下午2:07"}, "zh-CN"},
		{map[string]string{"d": "2024. 3. 5.", "D": "2024년 3월 5일 화요일", "t": "오후 2:07"}, "ko-KR"},
		{map[string]string{"d": "05.03.2024", "D": "вторник, 5 марта 2024 г."}, "ru-RU"},
		{map[string]string{"d": "5.03.2024", "D": "5 Mart 2024 Salı"}, "tr-TR"},
		{map[string]string{"d": "05/03/2024", "D": "mardi 5 mars 2024"}, "fr-FR"},
		{map[string]string{"d": "5/3/2024", "D": "martes, 5 de marzo de 2024"}, "es-ES"},
		{
			map[string]string{"d": "3/5/2024", "g": "3/5/2024 2:07 PM", "G": "3/5/2024 2:07:09 PM", "t": "2:07 PM"},
			"en-US",
		},
	}

	for _, tc := range tests {
		t.Run(tc.locale, func(t *testing.T) {
			t.Parallel()

			for spec, want := range tc.want {
				got, err := formatLogicTime(stampLogicTS(t), spec, tc.locale)
				require.NoError(t, err)
				assert.Equal(t, want, got, spec)
			}
		})
	}
}

const logicTestStamp = "2024-03-05T14:07:09Z"

func stampLogicTS(t *testing.T) logicTS {
	t.Helper()

	ts, err := parseLogicTime(logicTestStamp)
	require.NoError(t, err)

	return ts
}

func TestLogicLocaleInvariantFormats(t *testing.T) {
	t.Parallel()

	ts := stampLogicTS(t)

	tests := []struct {
		spec, want string
	}{
		{"R", "Tue, 05 Mar 2024 14:07:09 GMT"},
		{"r", "Tue, 05 Mar 2024 14:07:09 GMT"},
		{"s", "2024-03-05T14:07:09"},
		{"u", "2024-03-05 14:07:09Z"},
		{"o", "2024-03-05T14:07:09.0000000Z"},
	}

	for _, loc := range []string{"de-DE", "ja-JP", "tr-TR"} {
		for _, tc := range tests {
			t.Run(loc+"/"+tc.spec, func(t *testing.T) {
				t.Parallel()

				got, err := formatLogicTime(ts, tc.spec, loc)
				require.NoError(t, err)
				assert.Equal(t, tc.want, got, "the %s specifier ignores the culture", tc.spec)
			})
		}
	}
}

func TestLogicLocaleRoundTrip(t *testing.T) {
	t.Parallel()

	locales := []string{
		"en-GB", "fr-FR", "es-ES", "de-DE", "it-IT", "pt-BR", "pt-PT", "nl-NL", "sv-SE", "da-DK", "nb-NO", "fi-FI",
		"pl-PL", "ja-JP", "zh-CN", "ko-KR", "ru-RU", "tr-TR",
	}

	for _, loc := range locales {
		for _, spec := range []string{"d", "D", "g", "G", "f", "F"} {
			t.Run(loc+"/"+spec, func(t *testing.T) {
				t.Parallel()

				text, err := formatLogicTime(stampLogicTS(t), spec, loc)
				require.NoError(t, err)

				ts, ok := parseWithFormat(text, spec, logicLocales[lower(loc)])
				require.True(t, ok, "%q must parse back under %s", text, spec)

				want := stampLogicTS(t).t
				switch spec {
				case "d", "D":
					want = want.Truncate(24 * 60 * 60 * 1e9)
				case "g", "f":
					want = want.Add(-9 * 1e9)
				}

				assert.Equal(t, want, ts.t)
			})
		}
	}
}

func lower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}

	return string(b)
}

func TestLogicLocaleExpressions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		want string
	}{
		{"de month", "@formatDateTime('2024-03-05', 'MMMM', 'de-DE')", "März"},
		{"de abbreviated month", "@formatDateTime('2024-03-05', 'MMM', 'de-DE')", "März"},
		{"fr abbreviated month", "@formatDateTime('2024-07-05', 'MMM', 'fr-FR')", "juil."},
		{"ru day", "@formatDateTime('2024-03-05', 'dddd', 'ru-RU')", "вторник"},
		{"ja meridiem", "@formatDateTime('2024-03-05T14:00:00', 'tt h', 'ja-JP')", "午後 2"},
		{"zh meridiem", "@formatDateTime('2024-03-05T09:00:00', 'tt h', 'zh-CN')", "上午 9"},
		{"en-GB lowercase", "@formatDateTime('2024-03-05T14:00:00', 'tt', 'en-GB')", "pm"},
		{"non ascii literal", "@formatDateTime('2024-03-05', 'yyyy年M月', 'ja-JP')", "2024年3月"},
		{"parse de", "@parseDateTime('5. März 2024', 'de-DE')", "2024-03-05T00:00:00.0000000"},
		{"parse ja", "@parseDateTime('2024/03/05', 'ja-JP')", "2024-03-05T00:00:00.0000000"},
		{"parse ru standard", "@parseDateTime('05.03.2024 14:07', 'ru-RU', 'g')", "2024-03-05T14:07:00.0000000"},
		{"parse ko meridiem", "@parseDateTime('오후 2:07', 'ko-KR', 'tt h:mm')", "0001-01-01T14:07:00.0000000"},
		{"R format", "@formatDateTime('2024-03-05T14:07:09Z', 'R')", "Tue, 05 Mar 2024 14:07:09 GMT"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalLogicTemplate(tc.expr, logicTestCtx())
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	for _, expr := range []string{
		"@formatDateTime('2024-03-05', 'd', 'xx-XX')",
		"@parseDateTime('05.03.2024', 'th-TH')",
	} {
		_, err := evalLogicTemplate(expr, logicTestCtx())
		require.Error(t, err, "locales outside the table keep being rejected")
	}
}
