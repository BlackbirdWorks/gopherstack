package awsmeta_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

func TestGetSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		set  *awsmeta.Metadata
		want awsmeta.Metadata
	}{
		{
			name: "nil-set-returns-defaults",
			set:  nil,
			want: awsmeta.Metadata{
				Account:   awsmeta.DefaultAccount,
				Partition: awsmeta.DefaultPartition,
			},
		},
		{
			name: "explicit-region-and-account",
			set: &awsmeta.Metadata{
				Account:   "123456789012",
				Region:    "us-west-2",
				Partition: "aws",
				RequestID: "req-1",
			},
			want: awsmeta.Metadata{
				Account:   "123456789012",
				Region:    "us-west-2",
				Partition: "aws",
				RequestID: "req-1",
			},
		},
		{
			name: "govcloud-partition",
			set: &awsmeta.Metadata{
				Account:   "999999999999",
				Region:    "us-gov-west-1",
				Partition: "aws-us-gov",
			},
			want: awsmeta.Metadata{
				Account:   "999999999999",
				Region:    "us-gov-west-1",
				Partition: "aws-us-gov",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := awsmeta.Set(context.Background(), tc.set)
			got := awsmeta.Get(ctx)
			assert.Equal(t, tc.want.Account, got.Account)
			assert.Equal(t, tc.want.Region, got.Region)
			assert.Equal(t, tc.want.Partition, got.Partition)
			assert.Equal(t, tc.want.RequestID, got.RequestID)
		})
	}
}

func TestExtractors(t *testing.T) {
	t.Parallel()

	type metaArgs struct {
		setupCtx func(t *testing.T) context.Context
	}

	type metaWant struct {
		region      string
		account     string
		partition   string
		accessKeyID string
		service     string
	}

	tests := []struct {
		name string
		args metaArgs
		want metaWant
	}{
		{
			name: "nil-context-returns-defaults",
			args: metaArgs{
				setupCtx: func(t *testing.T) context.Context {
					t.Helper()

					return nil
				},
			},
			want: metaWant{
				region:      "",
				account:     awsmeta.DefaultAccount,
				partition:   awsmeta.DefaultPartition,
				accessKeyID: "",
				service:     "",
			},
		},
		{
			name: "empty-context-returns-defaults",
			args: metaArgs{
				setupCtx: func(t *testing.T) context.Context {
					t.Helper()

					return t.Context()
				},
			},
			want: metaWant{
				region:      "",
				account:     awsmeta.DefaultAccount,
				partition:   awsmeta.DefaultPartition,
				accessKeyID: "",
				service:     "",
			},
		},
		{
			name: "populated-context",
			args: metaArgs{
				setupCtx: func(t *testing.T) context.Context {
					t.Helper()

					return awsmeta.Set(t.Context(), &awsmeta.Metadata{
						Account:     "111111111111",
						Region:      "eu-west-1",
						Partition:   "aws",
						AccessKeyID: "AKIAIOSFODNN7EXAMPLE",
						Service:     "dynamodb",
					})
				},
			},
			want: metaWant{
				region:      "eu-west-1",
				account:     "111111111111",
				partition:   "aws",
				accessKeyID: "AKIAIOSFODNN7EXAMPLE",
				service:     "dynamodb",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := tc.args.setupCtx(t)
			assert.Equal(t, tc.want.region, awsmeta.Region(ctx))
			assert.Equal(t, tc.want.account, awsmeta.Account(ctx))
			assert.Equal(t, tc.want.partition, awsmeta.Partition(ctx))
			assert.Equal(t, tc.want.accessKeyID, awsmeta.AccessKeyID(ctx))
			assert.Equal(t, tc.want.service, awsmeta.Service(ctx))
		})
	}
}

func TestFromRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		buildRequest  func() *http.Request
		defaultRegion string
		wantRegion    string
		wantAccount   string
		wantRequestID string
		wantAKID      string
		wantService   string
	}{
		{
			name: "nil-request-uses-default-region",
			buildRequest: func() *http.Request {
				return nil
			},
			defaultRegion: "us-east-1",
			wantRegion:    "us-east-1",
			wantAccount:   awsmeta.DefaultAccount,
		},
		{
			name: "sigv4-scope-overrides-default",
			buildRequest: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/", nil)
				r.Header.Set("Authorization",
					"AWS4-HMAC-SHA256 Credential=AKIA/20260606/eu-central-1/s3/aws4_request")

				return r
			},
			defaultRegion: "us-east-1",
			wantRegion:    "eu-central-1",
			wantAccount:   awsmeta.DefaultAccount,
			wantAKID:      "AKIA",
			wantService:   "s3",
		},
		{
			name: "request-id-and-account-headers",
			buildRequest: func() *http.Request {
				r := httptest.NewRequest(
					http.MethodPost,
					"/?X-Amz-Credential=MYAKID/20260606/us-east-2/dynamodb/aws4_request",
					nil,
				)
				r.Header.Set("X-Amz-Account-Id", "222222222222")
				r.Header.Set("X-Amz-Request-Id", "abc-123")

				return r
			},
			defaultRegion: "us-east-2",
			wantRegion:    "us-east-2",
			wantAccount:   "222222222222",
			wantRequestID: "abc-123",
			wantAKID:      "MYAKID",
			wantService:   "dynamodb",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := awsmeta.FromRequest(tc.buildRequest(), tc.defaultRegion)
			assert.Equal(t, tc.wantRegion, m.Region)
			assert.Equal(t, tc.wantAccount, m.Account)
			assert.Equal(t, tc.wantRequestID, m.RequestID)
			assert.Equal(t, tc.wantAKID, m.AccessKeyID)
			assert.Equal(t, tc.wantService, m.Service)
			assert.Equal(t, awsmeta.DefaultPartition, m.Partition)
		})
	}
}
