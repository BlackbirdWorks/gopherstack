package ec2

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const allowedTestAccount = "111122223333"

func daysAgo(n int) time.Time { return time.Now().UTC().Add(-time.Duration(n) * 24 * time.Hour) }

func newAllowedBackend(t *testing.T, imgs ...*AMIStub) *InMemoryBackend {
	t.Helper()

	b := NewInMemoryBackend(allowedTestAccount, "us-east-1")
	for _, img := range imgs {
		b.images.Put(img)
	}

	_, err := b.EnableAllowedImagesSettings(allowedImagesStateEnabled)
	require.NoError(t, err)

	return b
}

func TestAllowedImages_Evaluation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		img      *AMIStub
		criteria []ImageCriterion
		want     bool
	}{
		{"provider or", &AMIStub{ImageID: "ami-1", OwnerID: "444455556666"},
			[]ImageCriterion{{ImageProviders: []string{"amazon", "444455556666"}}}, true},
		{"provider miss", &AMIStub{ImageID: "ami-1", OwnerID: "999999999999"},
			[]ImageCriterion{{ImageProviders: []string{"amazon"}}}, false},
		{"own account exempt", &AMIStub{ImageID: "ami-1", OwnerID: allowedTestAccount},
			[]ImageCriterion{{ImageProviders: []string{"none"}}}, true},
		{"none excludes shared", &AMIStub{ImageID: "ami-1", OwnerID: "amazon"},
			[]ImageCriterion{{ImageProviders: []string{"none"}}}, false},
		{"and within criterion fails", &AMIStub{ImageID: "ami-1", OwnerID: "amazon", Name: "other"},
			[]ImageCriterion{{ImageProviders: []string{"amazon"}, ImageNames: []string{"golden-*"}}}, false},
		{"and within criterion passes", &AMIStub{ImageID: "ami-1", OwnerID: "amazon", Name: "golden-1"},
			[]ImageCriterion{{ImageProviders: []string{"amazon"}, ImageNames: []string{"golden-?"}}}, true},
		{"or across criteria", &AMIStub{ImageID: "ami-1", OwnerID: "amazon", Name: "x"},
			[]ImageCriterion{{ImageProviders: []string{"999999999999"}}, {ImageNames: []string{"x"}}}, true},
		{"no criteria", &AMIStub{ImageID: "ami-1", OwnerID: "amazon"}, nil, false},
		{"product code", &AMIStub{ImageID: "ami-1", OwnerID: "aws-marketplace", ProductCodes: []string{"abc123"}},
			[]ImageCriterion{{MarketplaceProductCodes: []string{"zzz", "abc123"}}}, true},
		{"product code miss", &AMIStub{ImageID: "ami-1", OwnerID: "aws-marketplace"},
			[]ImageCriterion{{MarketplaceProductCodes: []string{"abc123"}}}, false},
		{"young enough", &AMIStub{ImageID: "ami-1", OwnerID: "amazon", CreationTime: daysAgo(10)},
			[]ImageCriterion{{CreationDateCondition: &CreationDateCondition{MaximumDaysSinceCreated: 300}}}, true},
		{"too old", &AMIStub{ImageID: "ami-1", OwnerID: "amazon", CreationTime: daysAgo(400)},
			[]ImageCriterion{{CreationDateCondition: &CreationDateCondition{MaximumDaysSinceCreated: 300}}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAllowedBackend(t, tt.img)
			require.NoError(t, b.ReplaceImageCriteriaInAllowedImagesSettings(tt.criteria))

			state, verdicts := b.EvaluateAllowedImages([]string{"ami-1"})
			assert.Equal(t, allowedImagesStateEnabled, state)
			assert.Equal(t, tt.want, verdicts["ami-1"])
		})
	}
}

func TestAllowedImages_Deprecation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		deprecate time.Time
		name      string
		maxDays   int32
		want      bool
	}{
		{time.Time{}, "not deprecated", 0, true},
		{time.Now().Add(48 * time.Hour), "future deprecation", 0, true},
		{daysAgo(5), "deprecated with zero", 0, false},
		{daysAgo(5), "within window", 30, true},
		{daysAgo(60), "beyond window", 30, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAllowedBackend(t, &AMIStub{ImageID: "ami-1", OwnerID: "amazon"})
			if !tt.deprecate.IsZero() {
				require.NoError(t, b.EnableImageDeprecation("ami-1", tt.deprecate.Format(time.RFC3339)))
			}

			require.NoError(t, b.ReplaceImageCriteriaInAllowedImagesSettings([]ImageCriterion{{
				ImageProviders:           []string{"amazon"},
				DeprecationTimeCondition: &DeprecationTimeCondition{MaximumDaysSinceDeprecated: tt.maxDays},
			}}))

			_, verdicts := b.EvaluateAllowedImages([]string{"ami-1"})
			assert.Equal(t, tt.want, verdicts["ami-1"])
		})
	}
}

func TestAllowedImages_Watermarks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		attached time.Time
		filter   ImageWatermarkFilter
		want     bool
	}{
		{"key match", time.Now(), ImageWatermarkFilter{WatermarkKey: allowedTestAccount + ":prod-*"}, true},
		{"key miss", time.Now(), ImageWatermarkFilter{WatermarkKey: "other:prod"}, false},
		{"no watermark", time.Time{}, ImageWatermarkFilter{WatermarkKey: "*"}, false},
		{"recent", daysAgo(10),
			ImageWatermarkFilter{WatermarkKey: "*", MaximumDaysSinceWatermarkCreated: new(int32(90))}, true},
		{"stale", daysAgo(100),
			ImageWatermarkFilter{WatermarkKey: "*", MaximumDaysSinceWatermarkCreated: new(int32(90))}, false},
		{"region match", time.Now(), ImageWatermarkFilter{SourceImageRegion: "us-*"}, true},
		{"region miss", time.Now(), ImageWatermarkFilter{SourceImageRegion: "eu-west-1"}, false},
		{
			"source too old", time.Now(),
			ImageWatermarkFilter{MaximumDaysSinceSourceImageCreated: new(int32(5))}, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAllowedBackend(t, &AMIStub{ImageID: "ami-1", OwnerID: "amazon", CreationTime: daysAgo(30)})
			if !tt.attached.IsZero() {
				key, err := b.AttachImageWatermark("ami-1", "prod-baseline")
				require.NoError(t, err)

				b.imageWatermarkTimes["ami-1"][key] = tt.attached
			}

			require.NoError(t, b.ReplaceImageCriteriaInAllowedImagesSettings(
				[]ImageCriterion{{ImageWatermarks: []ImageWatermarkFilter{tt.filter}}}))

			_, verdicts := b.EvaluateAllowedImages([]string{"ami-1"})
			assert.Equal(t, tt.want, verdicts["ami-1"])
		})
	}
}

func TestAllowedImages_LaunchEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		state   string
		wantErr bool
	}{
		{"enabled blocks", allowedImagesStateEnabled, true},
		{"audit allows", allowedImagesStateAudit, false},
		{"disabled allows", allowedImagesStateDisabled, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAllowedBackend(t, &AMIStub{ImageID: "ami-shared", OwnerID: "999999999999"})
			require.NoError(t, b.ReplaceImageCriteriaInAllowedImagesSettings(
				[]ImageCriterion{{ImageProviders: []string{"amazon"}}}))

			switch tt.state {
			case allowedImagesStateDisabled:
				b.DisableAllowedImagesSettings()
			default:
				_, err := b.EnableAllowedImagesSettings(tt.state)
				require.NoError(t, err)
			}

			_, err := b.RunInstances("ami-shared", "t3.micro", "", 1)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrImageNotFound)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestAllowedImages_ReplaceValidation(t *testing.T) {
	t.Parallel()

	many := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = "x"
		}

		return out
	}

	tests := []struct {
		name     string
		criteria []ImageCriterion
		wantErr  bool
	}{
		{"valid", []ImageCriterion{{ImageProviders: []string{"amazon"}}}, false},
		{"too many criteria", make([]ImageCriterion, 11), true},
		{"too many providers", []ImageCriterion{{ImageProviders: many(201)}}, true},
		{"too many names", []ImageCriterion{{ImageNames: many(51)}}, true},
		{"none exclusive", []ImageCriterion{{ImageProviders: []string{"none", "amazon"}}}, true},
		{
			"negative days",
			[]ImageCriterion{{CreationDateCondition: &CreationDateCondition{MaximumDaysSinceCreated: -1}}}, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend(allowedTestAccount, "us-east-1")
			err := b.ReplaceImageCriteriaInAllowedImagesSettings(tt.criteria)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestAllowedImages_HandlerWire(t *testing.T) {
	t.Parallel()

	b := newAllowedBackend(t,
		&AMIStub{ImageID: "ami-shared", OwnerID: "999999999999", Name: "shared"},
		&AMIStub{ImageID: "ami-mine", OwnerID: allowedTestAccount, Name: "mine", CreationTime: daysAgo(1)},
	)
	h := NewHandler(b)
	h.AccountID = allowedTestAccount
	h.Region = "us-east-1"

	post := func(vals url.Values) string {
		vals.Set("Version", "2016-11-15")

		return postFormInternal(t, h, vals.Encode())
	}

	replace := url.Values{"Action": {"ReplaceImageCriteriaInAllowedImagesSettings"}}
	replace.Set("ImageCriterion.1.ImageWatermark.1.WatermarkKey", "1:wm")
	replace.Set("ImageCriterion.1.ImageWatermark.1.MaximumDaysSinceWatermarkCreated", "7")
	replace.Set("ImageCriterion.1.ImageWatermark.1.SourceImageRegion", "us-east-1")
	assert.Contains(t, post(replace), "<return>true</return>")

	got := post(url.Values{"Action": {"GetAllowedImagesSettings"}})
	assert.Contains(t, got, "<watermarkKey>1:wm</watermarkKey>")
	assert.Contains(t, got, "<maximumDaysSinceWatermarkCreated>7</maximumDaysSinceWatermarkCreated>")
	assert.Contains(t, got, "<sourceImageRegion>us-east-1</sourceImageRegion>")

	enabled := post(url.Values{"Action": {"DescribeImages"}, "Owner.1": {"self"}, "Owner.2": {"999999999999"}})
	assert.NotContains(t, enabled, "ami-shared")
	assert.Contains(t, enabled, "<imageAllowed>true</imageAllowed>")
	assert.Contains(t, enabled, "<creationDate>")

	missing := post(url.Values{"Action": {"DescribeImages"}, "ImageId.1": {"ami-shared"}})
	assert.Contains(t, missing, "InvalidAMIID.NotFound")

	_, err := b.EnableAllowedImagesSettings(allowedImagesStateAudit)
	require.NoError(t, err)

	auditVals := url.Values{"Action": {"DescribeImages"}, "Filter.1.Name": {"image-allowed"}}
	auditVals.Set("Filter.1.Value.1", "false")
	audit := post(auditVals)
	assert.Contains(t, audit, "ami-shared")
	assert.NotContains(t, audit, "ami-mine")
	assert.Contains(t, audit, "<imageAllowed>false</imageAllowed>")
}

func postFormInternal(t *testing.T, h *Handler, body string) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec.Body.String()
}

func TestRegionalNatGateway_RouteTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		attachIGW  bool
		wantRoutes int
	}{
		{"igw attached before", true, 1},
		{"no igw", false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend(allowedTestAccount, "us-east-1")
			vpc, err := b.CreateVpc("10.9.0.0/16", "")
			require.NoError(t, err)

			var igwID string
			if tt.attachIGW {
				igw, igwErr := b.CreateInternetGateway()
				require.NoError(t, igwErr)
				require.NoError(t, b.AttachInternetGateway(igw.ID, vpc.ID))

				igwID = igw.ID
			}

			ngw, err := b.CreateRegionalNatGateway(vpc.ID, nil, nil)
			require.NoError(t, err)
			require.NotEmpty(t, ngw.RouteTableID)

			rts, rtErr := b.DescribeRouteTables([]string{ngw.RouteTableID})
			require.NoError(t, rtErr)
			require.Len(t, rts, 1)
			assert.Len(t, rts[0].Routes, tt.wantRoutes)

			if tt.wantRoutes > 0 {
				assert.Equal(t, igwID, rts[0].Routes[0].GatewayID)
			}

			require.NoError(t, b.DeleteNatGateway(ngw.ID))
			_, ok := b.routeTables.Get(ngw.RouteTableID)
			assert.False(t, ok)
		})
	}
}

func TestRegionalNatGateway_RouteFollowsIGWAttach(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend(allowedTestAccount, "us-east-1")
	vpc, err := b.CreateVpc("10.9.0.0/16", "")
	require.NoError(t, err)

	ngw, err := b.CreateRegionalNatGateway(vpc.ID, nil, nil)
	require.NoError(t, err)

	igw, err := b.CreateInternetGateway()
	require.NoError(t, err)
	require.NoError(t, b.AttachInternetGateway(igw.ID, vpc.ID))

	rt, _ := b.routeTables.Get(ngw.RouteTableID)
	require.Len(t, rt.Routes, 1)

	require.NoError(t, b.DetachInternetGateway(igw.ID, vpc.ID))
	assert.Empty(t, rt.Routes)
}
