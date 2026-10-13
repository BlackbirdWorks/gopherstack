package cloudtrail_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

func newValidationClient(t *testing.T) *cloudtrailsdk.Client {
	t.Helper()

	return newTestCloudTrailClient(
		t, cloudtrail.NewHandler(cloudtrail.NewInMemoryBackend("123456789012", "us-east-1")),
	)
}

func wantAPIError(t *testing.T, err error, code string) smithy.APIError {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, code, apiErr.ErrorCode())
	assert.NotContains(t, apiErr.ErrorMessage(), code, "message must not repeat the code")

	return apiErr
}

func TestCreateTrail_NameRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		trail string
		valid bool
	}{
		{name: "plain", trail: "my-trail", valid: true},
		{name: "dots_and_underscores", trail: "my.trail_1", valid: true},
		{name: "too_short", trail: "ab"},
		{name: "space", trail: "bad name"},
		{name: "leading_dash", trail: "-trail"},
		{name: "trailing_dot", trail: "trail."},
		{name: "adjacent_dashes", trail: "my--trail"},
		{name: "ip_address", trail: "10.0.0.1"},
		{name: "too_long", trail: string(make([]byte, 129))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newValidationClient(t)

			_, err := c.CreateTrail(t.Context(), &cloudtrailsdk.CreateTrailInput{
				Name: aws.String(tt.trail), S3BucketName: aws.String("bucket"),
			})
			if tt.valid {
				require.NoError(t, err)

				return
			}

			wantAPIError(t, err, "InvalidTrailNameException")
		})
	}
}

func TestCreateEventDataStore_Limits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		store     string
		wantCode  string
		retention int32
	}{
		{name: "defaults", store: "lake-1"},
		{name: "min_retention", store: "lake-1", retention: 7},
		{name: "retention_too_low", store: "lake-1", retention: 3, wantCode: "InvalidParameterException"},
		{name: "retention_too_high", store: "lake-1", retention: 4000, wantCode: "InvalidParameterException"},
		{name: "name_too_short", store: "ab", wantCode: "InvalidParameterException"},
		{name: "name_bad_chars", store: "lake store!", wantCode: "InvalidParameterException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newValidationClient(t)

			in := &cloudtrailsdk.CreateEventDataStoreInput{Name: aws.String(tt.store)}
			if tt.retention != 0 {
				in.RetentionPeriod = aws.Int32(tt.retention)
			}

			_, err := c.CreateEventDataStore(t.Context(), in)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			wantAPIError(t, err, tt.wantCode)
		})
	}
}

func TestLookupEvents_ArgumentValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		attrs    []cttypes.LookupAttribute
		max      int32
	}{
		{name: "ok", max: 50},
		{name: "max_too_big", max: 51, wantCode: "InvalidMaxResultsException"},
		{
			name: "bad_key",
			attrs: []cttypes.LookupAttribute{
				{AttributeKey: cttypes.LookupAttributeKey("Bogus"), AttributeValue: aws.String("x")},
			},
			wantCode: "InvalidLookupAttributesException",
		},
		{
			name: "good_key",
			attrs: []cttypes.LookupAttribute{
				{AttributeKey: cttypes.LookupAttributeKeyEventName, AttributeValue: aws.String("x")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newValidationClient(t)

			in := &cloudtrailsdk.LookupEventsInput{LookupAttributes: tt.attrs}
			if tt.max != 0 {
				in.MaxResults = aws.Int32(tt.max)
			}

			_, err := c.LookupEvents(t.Context(), in)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			wantAPIError(t, err, tt.wantCode)
		})
	}
}

func TestPutEventSelectors_ReadWriteType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rwType   cttypes.ReadWriteType
		wantCode string
	}{
		{name: "all", rwType: cttypes.ReadWriteTypeAll},
		{name: "read_only", rwType: cttypes.ReadWriteTypeReadOnly},
		{name: "bogus", rwType: cttypes.ReadWriteType("Bogus"), wantCode: "InvalidEventSelectorsException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newValidationClient(t)

			_, err := c.CreateTrail(t.Context(), &cloudtrailsdk.CreateTrailInput{
				Name: aws.String("sel-trail"), S3BucketName: aws.String("bucket"),
			})
			require.NoError(t, err)

			_, err = c.PutEventSelectors(t.Context(), &cloudtrailsdk.PutEventSelectorsInput{
				TrailName:      aws.String("sel-trail"),
				EventSelectors: []cttypes.EventSelector{{ReadWriteType: tt.rwType}},
			})
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			wantAPIError(t, err, tt.wantCode)
		})
	}
}

func TestErrorMessages_DropCodePrefix(t *testing.T) {
	t.Parallel()

	c := newValidationClient(t)
	in := &cloudtrailsdk.CreateTrailInput{Name: aws.String("dup-trail"), S3BucketName: aws.String("bucket")}

	_, err := c.CreateTrail(t.Context(), in)
	require.NoError(t, err)

	_, err = c.CreateTrail(t.Context(), in)
	apiErr := wantAPIError(t, err, "TrailAlreadyExistsException")
	assert.Contains(t, apiErr.ErrorMessage(), "dup-trail")
}

func TestStartQuery_IDIsUUID(t *testing.T) {
	t.Parallel()

	c := newValidationClient(t)

	out, err := c.StartQuery(t.Context(), &cloudtrailsdk.StartQueryInput{QueryStatement: aws.String("SELECT 1")})
	require.NoError(t, err)

	_, parseErr := uuid.Parse(aws.ToString(out.QueryId))
	require.NoError(t, parseErr)

	desc, err := c.DescribeQuery(t.Context(), &cloudtrailsdk.DescribeQueryInput{QueryId: out.QueryId})
	require.NoError(t, err)
	assert.Equal(t, out.QueryId, desc.QueryId)
}

func TestRouteMatcher_AcceptsBotocoreTargetNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{name: "go_sdk", target: "CloudTrail_20131101.DescribeTrails", want: true},
		{name: "botocore", target: "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.DescribeTrails", want: true},
		{name: "other_service", target: "com.amazonaws.kinesis.v20131202.Kinesis_20131202.ListStreams"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := cloudtrail.NewHandler(cloudtrail.NewInMemoryBackend("123456789012", "us-east-1"))
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
			req.Header.Set("X-Amz-Target", tt.target)
			c := echo.New().NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.RouteMatcher()(c))

			if tt.want {
				assert.Equal(t, "DescribeTrails", h.ExtractOperation(c))
			}
		})
	}
}
