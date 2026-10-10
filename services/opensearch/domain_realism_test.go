package opensearch_test

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenSearchHandler_CreateDomainClusterValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cluster map[string]any
		ebs     map[string]any
		name    string
		want    int
	}{
		{
			name:    "bad_instance_type",
			cluster: map[string]any{"InstanceType": "foo.large", "InstanceCount": 1},
			want:    400,
		},
		{name: "bad_master_type", cluster: map[string]any{"DedicatedMasterType": "large"}, want: 400},
		{
			name:    "count_too_high",
			cluster: map[string]any{"InstanceType": "t3.small.search", "InstanceCount": 81},
			want:    400,
		},
		{name: "ebs_too_small", ebs: map[string]any{"EBSEnabled": true, "VolumeSize": 1}, want: 400},
		{
			name:    "valid",
			cluster: map[string]any{"InstanceType": "t3.small.search", "InstanceCount": 2},
			ebs:     map[string]any{"EBSEnabled": true, "VolumeSize": 10},
			want:    200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := map[string]any{"DomainName": "realism"}
			if tt.cluster != nil {
				body["ClusterConfig"] = tt.cluster
			}

			if tt.ebs != nil {
				body["EBSOptions"] = tt.ebs
			}

			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", body)
			defer resp.Body.Close()
			assert.Equal(t, tt.want, resp.StatusCode)
		})
	}
}

func TestOpenSearchHandler_TagsTrailingSlashAndUpgradeCheck(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	arn := createDomainAndGetARN(t, h, "slashdom")

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/tags/", map[string]any{
		"ARN": arn, "TagList": []map[string]string{{"Key": "k", "Value": "v"}},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, h, http.MethodGet, "/2021-01-01/tags/?arn="+arn, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(raw), `"Key":"k"`)

	upgrade := func(target string, checkOnly bool) (int, string) {
		r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/upgradeDomain", map[string]any{
			"DomainName": "slashdom", "TargetVersion": target, "PerformCheckOnly": checkOnly,
		})
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)

		return r.StatusCode, string(b)
	}
	engineVersion := func() string {
		r := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/slashdom", nil)
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)

		return string(b)
	}

	code, msg := upgrade("OpenSearch_1.0", false)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.NotContains(t, msg, "ValidationException:")

	code, _ = upgrade("Elasticsearch_7.10", false)
	assert.Equal(t, http.StatusBadRequest, code)

	code, _ = upgrade("OpenSearch_2.13", true)
	assert.Equal(t, http.StatusOK, code)
	assert.NotContains(t, engineVersion(), "OpenSearch_2.13")

	code, _ = upgrade("OpenSearch_2.13", false)
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, engineVersion(), `"EngineVersion":"OpenSearch_2.13"`)
}
