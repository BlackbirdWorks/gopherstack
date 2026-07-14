package s3control_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3control "github.com/blackbirdworks/gopherstack/services/s3control"
)

// ---- Object Lambda AP ----

func TestObjectLambdaAP(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	ap := b.CreateAccessPointForObjectLambda("000000000000", "my-olap")

	t.Run("get OLAP", func(t *testing.T) {
		t.Parallel()
		got, err := b.GetAccessPointForObjectLambda("000000000000", ap.Name)
		require.NoError(t, err)
		assert.Equal(t, "my-olap", got.Name)
	})

	t.Run("list OLAPs", func(t *testing.T) {
		t.Parallel()
		aps := b.ListAccessPointsForObjectLambda("000000000000")
		assert.NotEmpty(t, aps)
	})

	t.Run("policy CRUD", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		a := b2.CreateAccessPointForObjectLambda("000000000000", "test-olap")
		require.NoError(
			t,
			b2.PutAccessPointPolicyForObjectLambda(
				"000000000000",
				a.Name,
				`{"Version":"2012-10-17"}`,
			),
		)
		policy, err := b2.GetAccessPointPolicyForObjectLambda("000000000000", a.Name)
		require.NoError(t, err)
		assert.Contains(t, policy, "Version")
		isPublic, _ := b2.GetAccessPointPolicyStatusForObjectLambda("000000000000", a.Name)
		assert.True(t, isPublic)
		require.NoError(t, b2.DeleteAccessPointPolicyForObjectLambda("000000000000", a.Name))
	})

	t.Run("config CRUD", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		a := b2.CreateAccessPointForObjectLambda("000000000000", "config-olap")
		require.NoError(
			t,
			b2.PutAccessPointConfigurationForObjectLambda("000000000000", a.Name, "<Config/>"),
		)
		cfg, err := b2.GetAccessPointConfigurationForObjectLambda("000000000000", a.Name)
		require.NoError(t, err)
		assert.Contains(t, cfg, "Config")
	})

	t.Run("delete OLAP", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		a := b2.CreateAccessPointForObjectLambda("000000000000", "del-olap")
		require.NoError(t, b2.DeleteAccessPointForObjectLambda("000000000000", a.Name))
	})
}

func TestHTTP_ListAccessPointsForObjectLambda(t *testing.T) {
	t.Parallel()
	h := s3control.NewHandler(s3control.NewInMemoryBackend())

	resp := doS3ControlNewOpRequest(
		t,
		h,
		http.MethodGet,
		"/v20180820/accesspointforobjectlambda",
		"000000000000",
		"",
	)
	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestCreateAccessPointForObjectLambda(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		accountID        string
		apName           string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:             "creates_object_lambda_access_point",
			accountID:        "123456789012",
			apName:           "my-lambda-ap",
			wantStatus:       http.StatusOK,
			wantBodyContains: "ObjectLambdaAccessPointArn",
		},
		{
			name:             "creates_object_lambda_access_point_different_account",
			accountID:        "000000000000",
			apName:           "another-lambda-ap",
			wantStatus:       http.StatusOK,
			wantBodyContains: "s3-object-lambda",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			path := "/v20180820/accesspointforobjectlambda/" + tt.apName
			rec := doS3ControlNewOpRequest(t, h, http.MethodPut, path, tt.accountID, "")

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}
		})
	}
}

func TestListAccessPointsForObjectLambda_Pagination(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	for i := range 4 {
		b.CreateAccessPointForObjectLambda("acct1", fmt.Sprintf("olap-%d", i))
	}
	h := s3control.NewHandler(b)

	tests := []struct {
		path          string
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          "/v20180820/accesspointforobjectlambda",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v20180820/accesspointforobjectlambda?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName                     xml.Name `xml:"ListAccessPointsForObjectLambdaResult"`
				NextToken                   string   `xml:"NextToken"`
				ObjectLambdaAccessPointList []struct {
					Name string `xml:"Name"`
				} `xml:"ObjectLambdaAccessPointList>ObjectLambdaAccessPoint"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.ObjectLambdaAccessPointList, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}
