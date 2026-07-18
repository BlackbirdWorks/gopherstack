package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConditionalForwarder_Lifecycle(t *testing.T) {
	t.Parallel()

	t.Run("create duplicate forwarder returns EntityAlreadyExistsException", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "CreateConditionalForwarder", map[string]any{
			"DirectoryId":      dirID,
			"RemoteDomainName": "remote.example.com",
			"DnsIpAddrs":       []string{"10.0.0.1"},
		})

		rec := doRequest(t, h, "CreateConditionalForwarder", map[string]any{
			"DirectoryId":      dirID,
			"RemoteDomainName": "remote.example.com",
			"DnsIpAddrs":       []string{"10.0.0.2"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		body := respBody(t, rec)
		assert.Equal(t, "EntityAlreadyExistsException", body["__type"])
	})

	t.Run("update changes DNS IPs", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "CreateConditionalForwarder", map[string]any{
			"DirectoryId":      dirID,
			"RemoteDomainName": "remote.example.com",
			"DnsIpAddrs":       []string{"10.0.0.1"},
		})

		doRequest(t, h, "UpdateConditionalForwarder", map[string]any{
			"DirectoryId":      dirID,
			"RemoteDomainName": "remote.example.com",
			"DnsIpAddrs":       []string{"10.0.0.2", "10.0.0.3"},
		})

		rec := doRequest(t, h, "DescribeConditionalForwarders", map[string]any{
			"DirectoryId":       dirID,
			"RemoteDomainNames": []string{"remote.example.com"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		fwds, _ := body["ConditionalForwarders"].([]any)
		require.Len(t, fwds, 1)
		fwd := fwds[0].(map[string]any)
		dnsIPs, _ := fwd["DnsIpAddrs"].([]any)
		assert.Len(t, dnsIPs, 2)
	})

	t.Run("delete non-existent forwarder returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		rec := doRequest(t, h, "DeleteConditionalForwarder", map[string]any{
			"DirectoryId":      dirID,
			"RemoteDomainName": "nonexistent.example.com",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// --- GetDirectoryLimits accuracy ---

func TestConditionalForwarders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create describe update delete cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Create
			rec1 := doRequest(t, h, "CreateConditionalForwarder", map[string]any{
				"DirectoryId":      dirID,
				"RemoteDomainName": "partner.example.com",
				"DnsIpAddrs":       []any{"8.8.8.8"},
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Describe
			rec2 := doRequest(t, h, "DescribeConditionalForwarders", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			fwds, _ := r2["ConditionalForwarders"].([]any)
			require.Len(t, fwds, 1)
			fwd := fwds[0].(map[string]any)
			assert.Equal(t, "partner.example.com", fwd["RemoteDomainName"])

			// Update
			rec3 := doRequest(t, h, "UpdateConditionalForwarder", map[string]any{
				"DirectoryId":      dirID,
				"RemoteDomainName": "partner.example.com",
				"DnsIpAddrs":       []any{"1.1.1.1"},
			})
			assert.Equal(t, http.StatusOK, rec3.Code)

			// Describe after update
			rec4 := doRequest(t, h, "DescribeConditionalForwarders", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			fwds2, _ := r4["ConditionalForwarders"].([]any)
			require.Len(t, fwds2, 1)
			fwd2 := fwds2[0].(map[string]any)
			dns, _ := fwd2["DnsIpAddrs"].([]any)
			assert.Equal(t, "1.1.1.1", dns[0].(string))

			// Delete
			rec5 := doRequest(t, h, "DeleteConditionalForwarder", map[string]any{
				"DirectoryId":      dirID,
				"RemoteDomainName": "partner.example.com",
			})
			assert.Equal(t, http.StatusOK, rec5.Code)

			// Describe after delete
			rec6 := doRequest(t, h, "DescribeConditionalForwarders", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec6.Code)
			var r6 map[string]any
			require.NoError(t, json.Unmarshal(rec6.Body.Bytes(), &r6))
			fwds3, _ := r6["ConditionalForwarders"].([]any)
			assert.Empty(t, fwds3)

			_ = tc
		})
	}
}
