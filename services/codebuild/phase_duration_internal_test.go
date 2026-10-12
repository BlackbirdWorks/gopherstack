package codebuild

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordSuccessfulPhasesDurationsAreWholeSeconds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		elapsed float64
	}{
		{name: "fractional", elapsed: 1.0},
		{name: "sub-second", elapsed: 0.1},
		{name: "whole", elapsed: 9.0},
	}

	whole := regexp.MustCompile(`"durationInSeconds":\d+[,}]`)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			build := &Build{StartTime: 1000}
			recordSuccessfulPhases(build, build.StartTime+tt.elapsed)

			for _, p := range build.Phases {
				raw, err := json.Marshal(p)
				require.NoError(t, err)
				assert.NotRegexp(t, `"durationInSeconds":\d*\.`, string(raw))

				if p.DurationInSeconds != 0 {
					assert.Regexp(t, whole, string(raw))
				}
			}
		})
	}
}
