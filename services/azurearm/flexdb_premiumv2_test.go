package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPGPremiumSSDv2Storage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		storage string
		status  int
	}{
		{name: "v2 any size", storage: `"type":"PremiumV2_LRS","storageSizeGB":100,"iops":3000,"throughput":125`,
			status: http.StatusAccepted},
		{name: "v2 max size", storage: `"type":"PremiumV2_LRS","storageSizeGB":65536,"iops":80000,"throughput":1200`,
			status: http.StatusAccepted},
		{name: "v2 too big", storage: `"type":"PremiumV2_LRS","storageSizeGB":65537,"iops":3000,"throughput":125`,
			status: http.StatusBadRequest},
		{name: "v2 no iops", storage: `"type":"PremiumV2_LRS","storageSizeGB":64,"throughput":125`,
			status: http.StatusBadRequest},
		{name: "v2 no throughput", storage: `"type":"PremiumV2_LRS","storageSizeGB":64,"iops":3000`,
			status: http.StatusBadRequest},
		{name: "v2 iops low", storage: `"type":"PremiumV2_LRS","storageSizeGB":64,"iops":2999,"throughput":125`,
			status: http.StatusBadRequest},
		{name: "v2 throughput high", storage: `"type":"PremiumV2_LRS","storageSizeGB":64,"iops":3000,"throughput":1201`,
			status: http.StatusBadRequest},
		{name: "premium odd size", storage: `"type":"Premium_LRS","storageSizeGB":100`, status: http.StatusBadRequest},
		{name: "unknown type", storage: `"type":"Standard_LRS","storageSizeGB":32`, status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newFlexStack(t, nil)
			status, resp := s.raw(t, http.MethodPut, flexServerPath(pgNS, "pgv2"),
				pgReq(pgLoginPW+`,"storage":{`+tt.storage+`}`), flexAPIPG)
			require.Equal(t, tt.status, status, resp)

			if tt.status != http.StatusAccepted {
				return
			}

			status, resp = s.raw(t, http.MethodGet, flexServerPath(pgNS, "pgv2"), "", flexAPIPG)
			require.Equal(t, http.StatusOK, status, resp)

			props, _ := resp["properties"].(map[string]any)
			st, _ := props["storage"].(map[string]any)
			assert.Equal(t, "PremiumV2_LRS", st["type"])
			assert.NotZero(t, st["iops"])
			assert.NotZero(t, st["throughput"])
		})
	}
}

func TestMySQLRejectsStorageType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		storage string
	}{
		{name: "premium", storage: `"type":"Premium_LRS","storageSizeGB":20`},
		{name: "premium v2", storage: `"type":"PremiumV2_LRS","storageSizeGB":20,"iops":3000,"throughput":125`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newFlexStack(t, nil)
			status, resp := s.raw(t, http.MethodPut, flexServerPath(myNS, "myv2"),
				`{"location":"x","sku":{"name":"Standard_B1ms","tier":"Burstable"},"properties":{`+
					`"administratorLogin":"a1","administratorLoginPassword":"`+flexPassword+
					`","storage":{`+tt.storage+`}}}`, flexAPIMy)
			assert.Equal(t, http.StatusBadRequest, status, resp)
		})
	}
}

func TestPGPremiumSSDv2Persistence(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	path := flexServerPath(pgNS, "pgv2snap")
	status, resp := s.raw(t, http.MethodPut, path, pgReq(pgLoginPW+
		`,"storage":{"type":"PremiumV2_LRS","storageSizeGB":512,"iops":12000,"throughput":500}`), flexAPIPG)
	require.Equal(t, http.StatusAccepted, status, resp)

	s2 := newFlexStack(t, nil)
	require.NoError(t, s2.handler.Restore(t.Context(), s.handler.Snapshot(t.Context())))

	status, resp = s2.raw(t, http.MethodGet, path, "", flexAPIPG)
	require.Equal(t, http.StatusOK, status, resp)

	props, _ := resp["properties"].(map[string]any)
	st, _ := props["storage"].(map[string]any)
	assert.Equal(t, "PremiumV2_LRS", st["type"])
	assert.InDelta(t, 12000, st["iops"], 0)
	assert.InDelta(t, 500, st["throughput"], 0)
}

func TestPGStorageTypeImmutable(t *testing.T) {
	t.Parallel()

	const (
		v2 = `"type":"PremiumV2_LRS","storageSizeGB":64,"iops":3000,"throughput":125`
		v1 = `"type":"Premium_LRS","storageSizeGB":64`
	)

	tests := []struct {
		name     string
		create   string
		update   string
		wantCode int
	}{
		{name: "v1 to v2", create: v1, update: v2, wantCode: http.StatusBadRequest},
		{name: "v2 to v1", create: v2, update: v1, wantCode: http.StatusBadRequest},
		{name: "v2 same", create: v2, update: v2, wantCode: http.StatusOK},
		{name: "v1 same", create: v1, update: v1, wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newFlexStack(t, nil)
			path := flexServerPath(pgNS, "pgtype")

			status, resp := s.raw(t, http.MethodPut, path, pgReq(pgLoginPW+`,"storage":{`+tt.create+`}`), flexAPIPG)
			require.Equal(t, http.StatusAccepted, status, resp)

			status, resp = s.raw(t, http.MethodPut, path, pgReq(pgLoginPW+`,"storage":{`+tt.update+`}`), flexAPIPG)
			assert.Equal(t, tt.wantCode, status, resp)
		})
	}
}
