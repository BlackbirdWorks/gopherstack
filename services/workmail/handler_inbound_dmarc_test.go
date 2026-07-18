package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Inbound DMARC Settings ----

func TestInboundDmarcSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		enforced bool
	}{
		{name: "enable enforcement", enforced: true},
		{name: "disable enforcement", enforced: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			orgID := createTestOrg(t, h, "dmarc-org")

			// Describe default (false)
			rec := doOp(t, h, "DescribeInboundDmarcSettings", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusOK, rec.Code)
			m := decodeJSON(t, rec)
			assert.Equal(t, false, m["Enforced"])

			// Put
			rec = doOp(t, h, "PutInboundDmarcSettings", fmt.Sprintf(
				`{"OrganizationId":%q,"Enforced":%v}`, orgID, tc.enforced,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe
			rec = doOp(t, h, "DescribeInboundDmarcSettings", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			m = decodeJSON(t, rec)
			assert.Equal(t, tc.enforced, m["Enforced"])
		})
	}
}
