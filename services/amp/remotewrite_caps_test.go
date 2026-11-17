package amp_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amp"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

func TestDecodeWriteRequestBoundsCounts(t *testing.T) {
	t.Parallel()

	labels := func(n int) promql.Labels {
		l := promql.Labels{}
		for i := range n {
			l["l"+strconv.Itoa(i)] = "v"
		}

		return l
	}

	tests := []struct {
		name    string
		series  []amp.WriteSeries
		wantErr bool
	}{
		{name: "150 labels", series: []amp.WriteSeries{{Labels: labels(150)}}},
		{name: "151 labels", series: []amp.WriteSeries{{Labels: labels(151)}}, wantErr: true},
		{
			name: "too many samples",
			series: []amp.WriteSeries{{
				Labels: labels(1), Samples: make([]promql.Sample, (1<<21)+1),
			}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := amp.DecodeWriteRequest(amp.EncodeWriteRequest(tt.series))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			assert.NoError(t, err)
		})
	}
}
