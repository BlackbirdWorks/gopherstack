package opencypher_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func TestExtendedDatetime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		params  map[string]any
		want    string
		wantErr string
		enabled bool
	}{
		{
			name: "off keeps seconds", query: `RETURN datetime('2024-04-01T23:59:59Z') AS d`,
			want: `[{"d":"2024-04-01T23:59:59Z"}]`,
		},
		{
			name: "on adds milliseconds", enabled: true, query: `RETURN datetime('2024-04-01T23:59:59Z') AS d`,
			want: `[{"d":"2024-04-01T23:59:59.000Z"}]`,
		},
		{
			name: "on keeps given millis", enabled: true, query: `RETURN datetime('2024-04-01T23:59:59.123') AS d`,
			want: `[{"d":"2024-04-01T23:59:59.123Z"}]`,
		},
		{
			name: "on keeps time zone", enabled: true, query: `RETURN datetime('2024-04-01T10:00:00+05:30') AS d`,
			want: `[{"d":"2024-04-01T10:00:00.000+05:30"}]`,
		},
		{
			name: "on hour-only zone", enabled: true, query: `RETURN datetime('2024-04-01T10:00:00-08') AS d`,
			want: `[{"d":"2024-04-01T10:00:00.000-08:00"}]`,
		},
		{
			name: "on yyyyMM date", enabled: true, query: `RETURN datetime('202404T10') AS d`,
			want: `[{"d":"2024-04-01T10:00:00.000Z"}]`,
		},
		{
			name:    "on over stored property",
			enabled: true,
			query:   `CREATE (:event {time: '2024-04-01T23:59:59Z'}) WITH 1 AS x MATCH (n:event) RETURN datetime(n.time) AS d`,
			want:    `[{"d":"2024-04-01T23:59:59.000Z"}]`,
		},
		{
			name: "on over parameter", enabled: true, params: map[string]any{"list": []any{"2023-01-01T00:00:00Z"}},
			query: `UNWIND $list AS d RETURN datetime(d) AS d`,
			want:  `[{"d":"2023-01-01T00:00:00.000Z"}]`,
		},
		{
			name:    "non-utc compact date rejected",
			enabled: true,
			query:   `RETURN datetime('20240401T10:00:00+01:00') AS d`,
			wantErr: "Unable to parse",
		},
		{
			name:    "non-utc minutes-only time rejected",
			enabled: true,
			query:   `RETURN datetime('2024-04-01T10:00+01:00') AS d`,
			wantErr: "Unable to parse",
		},
		{
			name: "zone out of range", enabled: true, query: `RETURN datetime('2024-04-01T10:00:00+14:30') AS d`,
			wantErr: "Unable to parse",
		},
		{
			name: "zone max ok", enabled: true, query: `RETURN datetime('2024-04-01T10:00:00+14:00') AS d`,
			want: `[{"d":"2024-04-01T10:00:00.000+14:00"}]`,
		},
		{
			name: "before 1400", enabled: true, query: `RETURN datetime('1399-12-31') AS d`,
			wantErr: "Unable to parse",
		},
		{
			name: "epochMillis over extended", enabled: true,
			query: `RETURN epochMillis(datetime('2022-01-01T00:01:00.250+01:00')) AS e`,
			want:  `[{"e":1640991660250}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := opencypher.NewGraph()

			g.SetExtendedDatetime(tt.enabled)

			got, err := run(t, g, tt.query, tt.params)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.JSONEq(t, tt.want, got)
		})
	}
}
