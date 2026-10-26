package servicequotas_test

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/json"
	"io"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqsdk "github.com/aws/aws-sdk-go-v2/service/servicequotas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicequotas"
)

//go:embed docs_quotas.json.gz
var docsQuotasGZ []byte

func TestDocsCatalog_Provenance(t *testing.T) {
	t.Parallel()

	zr, err := gzip.NewReader(bytes.NewReader(docsQuotasGZ))
	require.NoError(t, err)

	docsQuotas, err := io.ReadAll(zr)
	require.NoError(t, err)

	var svcs []servicequotas.ServiceDef
	require.NoError(t, json.Unmarshal(docsQuotas, &svcs))

	codeRe := regexp.MustCompile(`^L-[0-9A-F]{8}$`)
	seen := map[string]bool{}

	for _, s := range svcs {
		t.Run(s.Code, func(t *testing.T) {
			t.Parallel()

			assert.NotEmpty(t, s.Name)
			assert.Regexp(t, `^https://docs\.aws\.amazon\.com/general/latest/gr/[a-z0-9_-]+\.html$`, s.Source)
			assert.NotEmpty(t, s.Quotas)

			for _, q := range s.Quotas {
				assert.Regexp(t, codeRe, q.Code)
				assert.NotEmpty(t, q.Name)
				assert.NotEmpty(t, q.Unit)
				assert.True(t, q.Adjustable, "only adjustable quotas carry a documented code")
			}
		})

		assert.False(t, seen[s.Code], "duplicate service %s", s.Code)
		seen[s.Code] = true
	}

	canon, err := json.MarshalIndent(svcs, "", " ")
	require.NoError(t, err)
	assert.True(t, bytes.Equal(append(canon, '\n'), docsQuotas), "docs_quotas.json.gz is not in generator output form")
}

func TestDocsCatalog_ViaSDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		service  string
		quota    string
		wantName string
		want     float64
	}{
		{
			name:     "lambda_concurrency",
			service:  "lambda",
			quota:    "L-B99A9384",
			wantName: "Concurrent executions",
			want:     1000,
		},
		{
			name:     "kafka_kraft_brokers",
			service:  "kafka",
			quota:    "L-03FD6822",
			wantName: "Number of brokers per KRaft cluster",
			want:     60,
		},
		{
			name:     "dynamodb_control_plane",
			service:  "dynamodb",
			quota:    "L-1BB77E89",
			wantName: "Concurrent control plane operations",
			want:     500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)

			got, err := f.client.GetServiceQuota(t.Context(), &sqsdk.GetServiceQuotaInput{
				ServiceCode: aws.String(tt.service), QuotaCode: aws.String(tt.quota),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, aws.ToString(got.Quota.QuotaName))
			assert.InDelta(t, tt.want, aws.ToFloat64(got.Quota.Value), 0)

			def, err := f.client.GetAWSDefaultServiceQuota(t.Context(), &sqsdk.GetAWSDefaultServiceQuotaInput{
				ServiceCode: aws.String(tt.service), QuotaCode: aws.String(tt.quota),
			})
			require.NoError(t, err)
			assert.InDelta(t, tt.want, aws.ToFloat64(def.Quota.Value), 0)
		})
	}

	t.Run("list_services_breadth", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)

		var n int

		var token *string

		for {
			out, err := f.client.ListServices(t.Context(), &sqsdk.ListServicesInput{NextToken: token})
			require.NoError(t, err)

			n += len(out.Services)

			if token = out.NextToken; token == nil {
				break
			}
		}

		assert.GreaterOrEqual(t, n, 100)
	})
}
