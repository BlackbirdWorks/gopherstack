package route53_test

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoute53_ReusableDelegationSets(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// CreateReusableDelegationSet
	rec := send(
		t,
		h,
		http.MethodPost,
		"/2013-04-01/delegationset",
		`<CreateReusableDelegationSetRequest>`+
			`<CallerReference>ref-ds-1</CallerReference>`+
			`</CreateReusableDelegationSetRequest>`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	// ListReusableDelegationSets
	rec = send(t, h, http.MethodGet, "/2013-04-01/delegationset", "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestExtractOperation_CreateDelegationSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_delegation_set",
			path:   "/2013-04-01/delegationset",
			method: http.MethodPost,
			wantOp: "CreateReusableDelegationSet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestDelegationSetResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantIDInResp bool
		wantCode     int
	}{
		{
			name:         "response_includes_id",
			wantIDInResp: true,
			wantCode:     http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateReusableDelegationSetRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ds-ref-test</CallerReference>
</CreateReusableDelegationSetRequest>`

			rec := send(t, h, http.MethodPost, "/2013-04-01/delegationset", body)
			require.Equal(t, tt.wantCode, rec.Code)

			type dsResp struct {
				DelegationSet struct {
					ID          string   `xml:"Id"`
					NameServers []string `xml:"NameServers>NameServer"`
				} `xml:"DelegationSet"`
			}

			var resp dsResp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			if tt.wantIDInResp {
				assert.NotEmpty(t, resp.DelegationSet.ID)
				assert.Contains(t, resp.DelegationSet.ID, "delegationset")
				assert.NotEmpty(t, resp.DelegationSet.NameServers)
			}
		})
	}
}

func TestDelegationSet_CRUD(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateReusableDelegationSetRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ds-ref-1</CallerReference>
</CreateReusableDelegationSetRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/delegationset", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	type dsResp struct {
		DelegationSet struct {
			ID string `xml:"Id"`
		} `xml:"DelegationSet"`
	}

	var cr dsResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &cr))
	dsID := strings.TrimPrefix(cr.DelegationSet.ID, "/delegationset/")

	// Get delegation set.
	rec = send(t, h, http.MethodGet, "/2013-04-01/delegationset/"+dsID, "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetReusableDelegationSetResponse")

	// List delegation sets.
	rec = send(t, h, http.MethodGet, "/2013-04-01/delegationset", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListReusableDelegationSetsResponse")

	// Delete delegation set.
	rec = send(t, h, http.MethodDelete, "/2013-04-01/delegationset/"+dsID, "")
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestRoute53_CreateReusableDelegationSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "create_rds_success",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateReusableDelegationSetRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ds-ref-1</CallerReference>
</CreateReusableDelegationSetRequest>`,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateReusableDelegationSetResponse", "NameServer"},
		},
		{
			name: "create_rds_missing_caller_ref",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateReusableDelegationSetRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
</CreateReusableDelegationSetRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create_rds_invalid_xml",
			body:     "not-xml",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, http.MethodPost, "/2013-04-01/delegationset", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}
