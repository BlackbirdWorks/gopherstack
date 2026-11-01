package marketplaceentitlementservice_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/marketplaceentitlementservice"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	ent "github.com/blackbirdworks/gopherstack/services/marketplaceentitlementservice"
)

const (
	testAccount = "000000000000"
	product     = "prod-abcdefghijklm"
)

type fixture struct {
	backend *ent.InMemoryBackend
	handler *ent.Handler
	client  *sdk.Client
	url     string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	b := ent.NewInMemoryBackend(testAccount, "us-east-1")
	h := ent.NewHandler(b)
	url := sdktest.Serve(t, h)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testAccount, "test", "")),
	)
	require.NoError(t, err)

	return &fixture{
		backend: b, handler: h, url: url,
		client: sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(url) }),
	}
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func intValue(v int32) *ent.Value { return &ent.Value{IntegerValue: &v} }

func seed(t *testing.T, f *fixture) {
	t.Helper()

	exp := awstime.NewEpochTime(time.Date(2027, 6, 1, 0, 0, 0, 0, time.UTC))
	str := "gold"
	flag := true
	ratio := 0.5

	for _, e := range []ent.Entitlement{
		{ProductCode: product, CustomerIdentifier: "cust-a", CustomerAWSAccountID: "111111111111", Dimension: "Seats",
			Value: intValue(10), ExpirationDate: &exp},
		{ProductCode: product, CustomerIdentifier: "cust-a", CustomerAWSAccountID: "111111111111", Dimension: "Tier",
			Value: &ent.Value{StringValue: &str}},
		{ProductCode: product, CustomerIdentifier: "cust-b", CustomerAWSAccountID: "222222222222", Dimension: "Seats",
			Value: intValue(3)},
		{ProductCode: product, CustomerIdentifier: "cust-b", CustomerAWSAccountID: "222222222222", Dimension: "Beta",
			Value: &ent.Value{BooleanValue: &flag}},
		{ProductCode: product, CustomerIdentifier: "cust-b", CustomerAWSAccountID: "222222222222", Dimension: "Ratio",
			Value: &ent.Value{DoubleValue: &ratio}},
		{ProductCode: "other-product", CustomerIdentifier: "cust-a", CustomerAWSAccountID: "111111111111",
			Dimension: "Seats", Value: intValue(1)},
	} {
		require.NoError(t, f.backend.GrantEntitlement(e))
	}
}

func TestGetEntitlementsFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter   map[string][]string
		name     string
		product  string
		wantCode string
		want     int
	}{
		{name: "no filter", product: product, want: 5},
		{name: "other product", product: "other-product", want: 1},
		{name: "unknown product", product: "nope", want: 0},
		{
			name:    "by customer",
			product: product,
			filter:  map[string][]string{"CUSTOMER_IDENTIFIER": {"cust-a"}},
			want:    2,
		},
		{
			name:    "customer union",
			product: product,
			filter:  map[string][]string{"CUSTOMER_IDENTIFIER": {"cust-a", "cust-b"}},
			want:    5,
		},
		{name: "by dimension", product: product, filter: map[string][]string{"DIMENSION": {"Seats"}}, want: 2},
		{
			name: "customer and dimension", product: product, want: 1,
			filter: map[string][]string{"CUSTOMER_IDENTIFIER": {"cust-b"}, "DIMENSION": {"Seats", "Tier"}},
		},
		{
			name:    "by account",
			product: product,
			filter:  map[string][]string{"CUSTOMER_AWS_ACCOUNT_ID": {"222222222222"}},
			want:    3,
		},
		{name: "no match", product: product, filter: map[string][]string{"DIMENSION": {"Missing"}}, want: 0},
		{
			name: "mutually exclusive", product: product, wantCode: "InvalidParameterException",
			filter: map[string][]string{"CUSTOMER_IDENTIFIER": {"a"}, "CUSTOMER_AWS_ACCOUNT_ID": {"1"}},
		},
		{
			name:     "unknown filter",
			product:  product,
			filter:   map[string][]string{"COLOR": {"red"}},
			wantCode: "InvalidParameterException",
		},
		{
			name:     "empty values",
			product:  product,
			filter:   map[string][]string{"DIMENSION": {}},
			wantCode: "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			seed(t, f)

			out, err := f.client.GetEntitlements(t.Context(), &sdk.GetEntitlementsInput{
				ProductCode: aws.String(tt.product), Filter: tt.filter,
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Entitlements, tt.want)
		})
	}
}

func TestGetEntitlementsValues(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	seed(t, f)

	out, err := f.client.GetEntitlements(t.Context(), &sdk.GetEntitlementsInput{
		ProductCode: aws.String(product),
		Filter:      map[string][]string{"CUSTOMER_IDENTIFIER": {"cust-a"}},
	})
	require.NoError(t, err)
	require.Len(t, out.Entitlements, 2)

	seats := out.Entitlements[0]
	assert.Equal(t, "Seats", aws.ToString(seats.Dimension))
	assert.Equal(t, "111111111111", aws.ToString(seats.CustomerAWSAccountId))
	assert.Equal(t, int32(10), aws.ToInt32(seats.Value.IntegerValue))
	require.NotNil(t, seats.ExpirationDate)
	assert.True(t, seats.ExpirationDate.Equal(time.Date(2027, 6, 1, 0, 0, 0, 0, time.UTC)))

	tier := out.Entitlements[1]
	assert.Equal(t, "gold", aws.ToString(tier.Value.StringValue))
	assert.Nil(t, tier.ExpirationDate)

	rest, err := f.client.GetEntitlements(t.Context(), &sdk.GetEntitlementsInput{
		ProductCode: aws.String(product), Filter: map[string][]string{"CUSTOMER_IDENTIFIER": {"cust-b"}},
	})
	require.NoError(t, err)

	got := map[string]bool{}

	for _, e := range rest.Entitlements {
		switch aws.ToString(e.Dimension) {
		case "Beta":
			got["Beta"] = aws.ToBool(e.Value.BooleanValue)
		case "Ratio":
			got["Ratio"] = aws.ToFloat64(e.Value.DoubleValue) == 0.5
		}
	}

	assert.Equal(t, map[string]bool{"Beta": true, "Ratio": true}, got)
}

func TestGetEntitlementsPagination(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	seed(t, f)

	var dims []string

	token := (*string)(nil)

	for range 10 {
		out, err := f.client.GetEntitlements(t.Context(), &sdk.GetEntitlementsInput{
			ProductCode: aws.String(product), MaxResults: aws.Int32(2), NextToken: token,
		})
		require.NoError(t, err)
		assert.LessOrEqual(t, len(out.Entitlements), 2)

		for _, e := range out.Entitlements {
			dims = append(dims, aws.ToString(e.CustomerIdentifier)+"/"+aws.ToString(e.Dimension))
		}

		if out.NextToken == nil {
			break
		}

		token = out.NextToken
	}

	assert.Len(t, dims, 5)
	assert.ElementsMatch(t, []string{
		"cust-a/Seats", "cust-a/Tier", "cust-b/Seats", "cust-b/Beta", "cust-b/Ratio",
	}, dims)
}

func TestGetEntitlementsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   *sdk.GetEntitlementsInput
		name string
	}{
		{name: "empty product", in: &sdk.GetEntitlementsInput{ProductCode: aws.String("")}},
		{name: "zero page", in: &sdk.GetEntitlementsInput{ProductCode: aws.String(product), MaxResults: aws.Int32(0)}},
		{
			name: "huge page",
			in:   &sdk.GetEntitlementsInput{ProductCode: aws.String(product), MaxResults: aws.Int32(5000)},
		},
		{
			name: "bad token",
			in:   &sdk.GetEntitlementsInput{ProductCode: aws.String(product), NextToken: aws.String("!!")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newFixture(t).client.GetEntitlements(t.Context(), tt.in)
			assert.Equal(t, "InvalidParameterException", apiCode(err))
		})
	}
}

func TestLegacyJSONTarget(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	seed(t, f)

	tests := []struct {
		name   string
		target string
		body   string
		want   string
		status int
	}{
		{
			name: "ok", target: "AWSMPEntitlementService.GetEntitlements", status: http.StatusOK,
			body: `{"ProductCode":"` + product + `","Filter":{"DIMENSION":["Tier"]}}`,
			want: `"Dimension":"Tier"`,
		},
		{
			name: "expiry is an epoch number", target: "AWSMPEntitlementService.GetEntitlements", status: http.StatusOK,
			body: `{"ProductCode":"` + product + `","Filter":{"DIMENSION":["Seats"],"CUSTOMER_IDENTIFIER":["cust-a"]}}`,
			want: `"ExpirationDate":1811808000`,
		},
		{
			name: "bad filter", target: "AWSMPEntitlementService.GetEntitlements", status: http.StatusBadRequest,
			body: `{"ProductCode":"` + product + `","Filter":{"X":["y"]}}`, want: "InvalidParameterException",
		},
		{
			name: "unknown op", target: "AWSMPEntitlementService.Nope", status: http.StatusBadRequest,
			body: `{}`, want: "InvalidParameterException",
		},
		{
			name: "malformed", target: "AWSMPEntitlementService.GetEntitlements", status: http.StatusBadRequest,
			body: `{`, want: "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, f.url+"/", strings.NewReader(tt.body))
			require.NoError(t, err)
			req.Header.Set("X-Amz-Target", tt.target)
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.status, resp.StatusCode)
			assert.Contains(t, string(raw), tt.want)
		})
	}
}

func TestGrantAndRevoke(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	require.Error(t, f.backend.GrantEntitlement(ent.Entitlement{ProductCode: product}))
	require.Error(t, f.backend.GrantEntitlement(ent.Entitlement{
		ProductCode: product, CustomerIdentifier: "c", Dimension: "d",
	}))

	seed(t, f)
	f.backend.RevokeEntitlements(product, "cust-a")

	out, err := f.client.GetEntitlements(t.Context(), &sdk.GetEntitlementsInput{ProductCode: aws.String(product)})
	require.NoError(t, err)
	assert.Len(t, out.Entitlements, 3)
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newFixture(t)
	seed(t, src)

	snap := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newFixture(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	out, err := dst.client.GetEntitlements(t.Context(), &sdk.GetEntitlementsInput{ProductCode: aws.String(product)})
	require.NoError(t, err)
	assert.Len(t, out.Entitlements, 5)

	dst.handler.Reset()

	out, err = dst.client.GetEntitlements(t.Context(), &sdk.GetEntitlementsInput{ProductCode: aws.String(product)})
	require.NoError(t, err)
	assert.Empty(t, out.Entitlements)

	require.Error(t, dst.handler.Restore(t.Context(), []byte("{")))
}
