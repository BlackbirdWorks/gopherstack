package azuretable_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"
)

func TestQueryTables_HidesMetricsTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		table  string
		listed bool
	}{
		{name: "metrics table hidden", table: "$MetricsHourPrimaryTransactionsFile", listed: false},
		{name: "user table listed", table: "plain", listed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.CreateTable(tt.table))
			_, err := h.Backend.InsertEntity(tt.table, "p", "r",
				map[string]odatatable.EntityProperty{"N": {Value: int64(1), Type: odatatable.EdmInt64}})
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/Tables", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.listed, contains(rec.Body.String(), tt.table))
		})
	}
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}
