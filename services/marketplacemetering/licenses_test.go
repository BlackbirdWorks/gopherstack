package marketplacemetering_test

import (
	"bytes"
	"encoding/json/v2"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/marketplacemetering"
	"github.com/aws/aws-sdk-go-v2/service/marketplacemetering/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	mm "github.com/blackbirdworks/gopherstack/services/marketplacemetering"
)

func TestBatchMeterUsageLicenses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutate   func(f *fixture, r *types.UsageRecord, lic *mm.License)
		wantCode string
		wantOK   bool
	}{
		{name: "valid", mutate: func(*fixture, *types.UsageRecord, *mm.License) {}, wantOK: true},
		{name: "unknown arn", mutate: func(_ *fixture, r *types.UsageRecord, _ *mm.License) {
			r.LicenseArn = aws.String("arn:aws:license-manager::000000000000:license:l-missing")
		}, wantCode: "InvalidLicenseException"},
		{name: "other customer", mutate: func(_ *fixture, r *types.UsageRecord, _ *mm.License) {
			r.CustomerAWSAccountId = aws.String(otherAccount)
		}, wantCode: "InvalidLicenseException"},
		{name: "before activation", mutate: func(f *fixture, r *types.UsageRecord, lic *mm.License) {
			r.Timestamp = aws.Time(lic.ActivatedAt.Add(-time.Minute))
			_ = f
		}, wantCode: "InvalidLicenseException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			require.NoError(t, f.backend.RegisterProduct(mm.ProductSpec{
				Code: "lic-product", Dimensions: []mm.Dimension{{Name: "Users"}},
			}))

			from := awstime.NewEpochTime(f.now().Add(-time.Hour))
			lic, err := f.backend.RegisterLicense(mm.LicenseSpec{
				ProductCode: "lic-product", CustomerAWSAccountID: testAccount, ActivatedAt: &from,
			})
			require.NoError(t, err)

			rec := types.UsageRecord{
				Dimension: aws.String("Users"), Quantity: new(int32(1)), Timestamp: aws.Time(f.now()),
				LicenseArn: aws.String(lic.Arn),
			}
			tt.mutate(f, &rec, lic)

			out, err := f.client.BatchMeterUsage(t.Context(), &sdk.BatchMeterUsageInput{
				UsageRecords: []types.UsageRecord{rec},
			})
			if !tt.wantOK {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			require.Len(t, out.Results, 1)
			assert.Equal(t, types.UsageRecordResultStatusSuccess, out.Results[0].Status)
			assert.Equal(t, lic.Arn, aws.ToString(out.Results[0].UsageRecord.LicenseArn))
			assert.Len(t, f.backend.UsageRecords("lic-product"), 1)
		})
	}
}

func TestSeedEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string
		body     map[string]any
		wantKey  string
	}{
		{name: "product", resource: "products", body: map[string]any{"code": "seed-prod"}, wantKey: "code"},
		{
			name: "subscription", resource: "subscriptions",
			body:    map[string]any{"productCode": "seed-prod", "accountId": testAccount},
			wantKey: "customerIdentifier",
		},
		{
			name: "token", resource: "registration-tokens",
			body:    map[string]any{"productCode": "seed-prod", "accountId": testAccount},
			wantKey: "registrationToken",
		},
		{
			name: "license", resource: "licenses",
			body:    map[string]any{"productCode": "seed-prod", "customerAwsAccountId": testAccount},
			wantKey: "arn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			raw, err := json.Marshal(tt.body, jsonx.V1())
			require.NoError(t, err)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
				f.url+"/_gopherstack/marketplace/"+tt.resource, bytes.NewReader(raw))
			require.NoError(t, err)

			resp, err := httpclient.New(10 * time.Second).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var got map[string]any
			require.NoError(t, json.UnmarshalRead(resp.Body, &got, jsonx.V1()))
			assert.NotEmpty(t, got[tt.wantKey])
		})
	}
}

func TestSeedEndpointsRejectBadInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string
		body     string
	}{
		{name: "malformed json", resource: "products", body: "{"},
		{name: "empty product", resource: "products", body: "{}"},
		{
			name: "duplicate dimension", resource: "products",
			body: `{"code":"p","dimensions":[{"name":"a"},{"name":"a"}]}`,
		},
		{name: "bad account", resource: "subscriptions", body: `{"productCode":"p","accountId":"x"}`},
		{name: "bad token account", resource: "registration-tokens", body: `{"productCode":"p","accountId":"x"}`},
		{name: "no product", resource: "registration-tokens", body: `{"accountId":"123456789012"}`},
		{name: "license bad account", resource: "licenses", body: `{"productCode":"p","customerAwsAccountId":"1"}`},
		{
			name: "license window", resource: "licenses",
			body: `{"productCode":"p","customerAwsAccountId":"123456789012","activatedAt":10,"expiresAt":5}`,
		},
		{name: "license bad time", resource: "licenses", body: `{"productCode":"p","activatedAt":"nope"}`},
		{name: "wrong type", resource: "licenses", body: `[1]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
				f.url+"/_gopherstack/marketplace/"+tt.resource, strings.NewReader(tt.body))
			require.NoError(t, err)

			resp, err := httpclient.New(10 * time.Second).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}
