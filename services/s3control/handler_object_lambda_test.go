package s3control_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
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

	// TestObjectLambdaAP/delete_OLAP_cascade_cleans_state locks in the
	// ghost-map-row fix: DeleteAccessPointForObjectLambda previously only
	// removed the OLAP row itself, leaving its policy, configuration, and
	// generic resource tags behind forever -- resurfacing on a
	// delete/recreate cycle under the same name.
	t.Run("delete OLAP cascade cleans state", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		a := b2.CreateAccessPointForObjectLambda("000000000000", "cascade-olap")
		require.NoError(t, b2.PutAccessPointPolicyForObjectLambda("000000000000", a.Name, `{"p":1}`))
		require.NoError(t, b2.PutAccessPointConfigurationForObjectLambda("000000000000", a.Name, "<Config/>"))
		b2.TagResource(a.ObjectLambdaAccessPointArn, map[string]string{"env": "test"})

		require.NoError(t, b2.DeleteAccessPointForObjectLambda("000000000000", a.Name))

		b2.CreateAccessPointForObjectLambda("000000000000", a.Name)

		policy, err := b2.GetAccessPointPolicyForObjectLambda("000000000000", a.Name)
		require.NoError(t, err)
		assert.Empty(t, policy, "policy must not survive delete")

		cfg, err := b2.GetAccessPointConfigurationForObjectLambda("000000000000", a.Name)
		require.NoError(t, err)
		assert.Empty(t, cfg, "config must not survive delete")

		assert.Empty(t, b2.ListTagsForResource(a.ObjectLambdaAccessPointArn), "tags must not survive delete")
	})
}

func TestHTTP_ListAccessPointsForObjectLambda(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, b *s3control.InMemoryBackend)
		name       string
		accountID  string
		wantAlias  bool
		wantStatus int
		wantCount  int
	}{
		{
			name:       "empty_list",
			accountID:  "000000000000",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:      "list_with_alias",
			accountID: "123456789012",
			setup: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				b.CreateAccessPointForObjectLambda("123456789012", "my-olap")
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
			wantAlias:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}
			h := s3control.NewHandler(b)

			resp := doS3ControlNewOpRequest(
				t,
				h,
				http.MethodGet,
				"/v20180820/accesspointforobjectlambda",
				tt.accountID,
				"",
			)
			var listResp struct {
				XMLName                     xml.Name `xml:"ListAccessPointsForObjectLambdaResult"`
				NextToken                   string   `xml:"NextToken"`
				ObjectLambdaAccessPointList []struct {
					Name                       string `xml:"Name"`
					ObjectLambdaAccessPointArn string `xml:"ObjectLambdaAccessPointArn"`
					Alias                      struct {
						Status string `xml:"Status"`
						Value  string `xml:"Value"`
					} `xml:"Alias"`
				} `xml:"ObjectLambdaAccessPointList>ObjectLambdaAccessPoint"`
			}
			require.NoError(t, xml.Unmarshal(resp.Body.Bytes(), &listResp))
			require.Len(t, listResp.ObjectLambdaAccessPointList, tt.wantCount)

			if tt.wantAlias {
				require.NotEmpty(t, listResp.ObjectLambdaAccessPointList)
				alias := listResp.ObjectLambdaAccessPointList[0].Alias
				assert.Equal(t, "READY", alias.Status)
				assert.True(t, strings.HasSuffix(alias.Value, "--ol-s3"))
				assert.LessOrEqual(t, len(alias.Value), 63)
			}
		})
	}
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

// TestGetAccessPointForObjectLambda_NoFabricatedArn asserts real
// GetAccessPointForObjectLambdaOutput has no ObjectLambdaAccessPointArn
// field — its only members are Alias/CreationDate/Name/PublicAccessBlockConfiguration.
func TestGetAccessPointForObjectLambda_NoFabricatedArn(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateAccessPointForObjectLambda("000000000000", "my-olap")
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodGet, "/v20180820/accesspointforobjectlambda/my-olap", "000000000000", "",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	assert.NotContains(t, rec.Body.String(), "ObjectLambdaAccessPointArn")

	var out struct {
		XMLName xml.Name `xml:"GetAccessPointForObjectLambdaResult"`
		Name    string   `xml:"Name"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "my-olap", out.Name)
}

// TestAccessPointConfigurationForObjectLambda_WireShape covers:
// GetAccessPointConfigurationForObjectLambdaOutput wraps its payload under
// "<Configuration>", not "<ObjectLambdaConfiguration>"
// (awsRestxml_deserializeOpDocumentGetAccessPointConfigurationForObjectLambdaOutput
// only recognizes "Configuration" at the top level), and a nested
// TransformationConfigurations/SupportingAccessPoint structure round-trips
// through Put then Get intact.
func TestAccessPointConfigurationForObjectLambda_WireShape(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateAccessPointForObjectLambda("acct1", "my-olap")
	h := s3control.NewHandler(b)
	path := "/v20180820/accesspointforobjectlambda/my-olap/configuration"

	putBody := `<PutAccessPointConfigurationForObjectLambdaRequest>` +
		`<Configuration>` +
		`<SupportingAccessPoint>arn:aws:s3:us-east-1:000000000000:accesspoint/my-ap</SupportingAccessPoint>` +
		`<TransformationConfigurations>` +
		`<member><Actions><member>GetObject</member></Actions></member>` +
		`</TransformationConfigurations>` +
		`</Configuration>` +
		`</PutAccessPointConfigurationForObjectLambdaRequest>`

	putRec := doS3Request(t, h, http.MethodPut, path, putBody)
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doS3Request(t, h, http.MethodGet, path, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	body := getRec.Body.String()
	assert.Contains(t, body, "<Configuration>")
	assert.NotContains(t, body, "<ObjectLambdaConfiguration>")
	assert.Contains(
		t, body,
		"<SupportingAccessPoint>arn:aws:s3:us-east-1:000000000000:accesspoint/my-ap</SupportingAccessPoint>",
	)
	assert.Contains(t, body, "<TransformationConfigurations>")
	assert.Contains(t, body, "GetObject")
}

func TestObjectLambdaAccessPoint_MutationIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(b *s3control.InMemoryBackend)
		name   string
	}{
		{
			name: "create_return_mutation_does_not_affect_get",
			mutate: func(b *s3control.InMemoryBackend) {
				ap := b.CreateAccessPointForObjectLambda("123456789012", "mut-ap")
				ap.Name = "mutated"
				if ap.Alias != nil {
					ap.Alias.Value = "mutated-alias"
					ap.Alias.Status = "MUTATED"
				}
			},
		},
		{
			name: "get_return_mutation_does_not_affect_subsequent_get",
			mutate: func(b *s3control.InMemoryBackend) {
				b.CreateAccessPointForObjectLambda("123456789012", "mut-ap")
				ap, err := b.GetAccessPointForObjectLambda("123456789012", "mut-ap")
				if err == nil && ap != nil {
					ap.Name = "mutated"
					if ap.Alias != nil {
						ap.Alias.Value = "mutated-alias"
						ap.Alias.Status = "MUTATED"
					}
				}
			},
		},
		{
			name: "list_return_mutation_does_not_affect_get",
			mutate: func(b *s3control.InMemoryBackend) {
				b.CreateAccessPointForObjectLambda("123456789012", "mut-ap")
				aps := b.ListAccessPointsForObjectLambda("123456789012")
				for _, ap := range aps {
					ap.Name = "mutated"
					if ap.Alias != nil {
						ap.Alias.Value = "mutated-alias"
						ap.Alias.Status = "MUTATED"
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			tt.mutate(b)

			got, err := b.GetAccessPointForObjectLambda("123456789012", "mut-ap")
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, "mut-ap", got.Name)
			require.NotNil(t, got.Alias)
			assert.Equal(t, "READY", got.Alias.Status)
			assert.True(t, strings.HasSuffix(got.Alias.Value, "--ol-s3"))
			assert.NotEqual(t, "mutated-alias", got.Alias.Value)
		})
	}
}
