package dmsengine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
)

func TestAddColumnOutOfRangeArguments(t *testing.T) {
	t.Parallel()

	row := dmsengine.Row{"7", "Tech", "20500.50", "2002-01-15", nil}

	tests := []struct {
		name string
		expr string
		want string
	}{
		{"substr start past end", `substr($dept,99999999999,2)`, ""},
		{"substr start before begin", `substr($dept,-99999999999,2)`, ""},
		{"substr length past end", `substr($dept,1,99999999999)`, "Tech"},
		{"zeroblob huge", `substr(hex(zeroblob(9999999999999)),1,4)`, "0000"},
		{"zeroblob negative", `hex(zeroblob(-5))`, ""},
		{"randomblob huge", `substr(hex(randomblob(9999999999999)),1,2)`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := dmsengine.ParseMappings(transformDoc(addColumn("2", "added", tt.expr, `{"type":"string"}`)))
			require.NoError(t, err)

			plan, err := m.Plan(employeeTable(), dmsengine.FamilyMySQL, "")
			require.NoError(t, err)

			out := plan.ProjectWith(row, dmsengine.ProjectCtx{Full: true, Now: time.Unix(0, 0).UTC()})
			require.Len(t, out, 6)

			if tt.name == "randomblob huge" {
				assert.Len(t, out[5], 2)

				return
			}

			assert.Equal(t, tt.want, out[5])
		})
	}
}
