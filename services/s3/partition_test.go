package s3_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

type regionPartition struct {
	region    string
	partition string
}

func partitionRegions() []regionPartition {
	return []regionPartition{
		{"us-east-1", "aws"},
		{"cn-north-1", "aws-cn"},
		{"us-gov-west-1", "aws-us-gov"},
	}
}

func partitionPolicy(partition string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*",` +
		`"Action":"s3:GetObject","Resource":"arn:` + partition + `:s3:::pbkt/*"}]}`
}

func putPolicyInRegion(h *s3.S3Handler, region, policy string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPut, "/pbkt?policy", strings.NewReader(policy))
	req = req.WithContext(awsmeta.WithRegion(req.Context(), region))
	rec := httptest.NewRecorder()
	serveInternal(h, rec, req)

	return rec
}

func newPartitionHandler(t *testing.T, region string) (*s3.S3Handler, *s3.InMemoryBackend) {
	t.Helper()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	_, err := backend.CreateBucket(awsmeta.WithRegion(context.Background(), region), &awss3.CreateBucketInput{
		Bucket: aws.String("pbkt"),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		},
	})
	require.NoError(t, err)

	return s3.NewHandler(backend), backend
}

func TestBucketPolicyPartitionEvaluation(t *testing.T) {
	t.Parallel()

	for _, tt := range partitionRegions() {
		t.Run(tt.region, func(t *testing.T) {
			t.Parallel()

			h, backend := newPartitionHandler(t, tt.region)
			require.NoError(t, backend.PutBucketPolicy(context.Background(), "pbkt", partitionPolicy(tt.partition)))

			got := h.EvaluateBucketPolicyForTest(context.Background(), "pbkt", "k", s3.ActionGetObject, true)
			assert.Equal(t, s3.EffectDeny, got)
		})
	}
}

func TestPutBucketPolicyPartitionStrict(t *testing.T) {
	t.Parallel()

	for _, tt := range partitionRegions() {
		for _, other := range partitionRegions() {
			t.Run(tt.region+"_with_"+other.partition, func(t *testing.T) {
				t.Parallel()

				h, _ := newPartitionHandler(t, tt.region)
				rec := putPolicyInRegion(h, tt.region, partitionPolicy(other.partition))

				if tt.partition == other.partition {
					assert.Equal(t, http.StatusNoContent, rec.Code)

					return
				}

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "MalformedPolicy")
			})
		}
	}
}

func TestPutBucketPolicyPartitionNonStrict(t *testing.T) {
	t.Setenv(arn.EnvPartitionStrict, "false")

	h, _ := newPartitionHandler(t, "cn-north-1")
	rec := putPolicyInRegion(h, "cn-north-1", partitionPolicy("aws"))
	assert.Equal(t, http.StatusNoContent, rec.Code)
}
