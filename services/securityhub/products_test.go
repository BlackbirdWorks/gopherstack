package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Batch-1 accuracy gap: DescribeProducts is GET /products.
func TestDescribeProductsIsGETProducts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/products", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	products, _ := resp["Products"].([]any)
	assert.NotEmpty(t, products)

	p0 := products[0].(map[string]any)
	assert.NotEmpty(t, p0["ProductArn"])
	assert.NotEmpty(t, p0["ProductName"])
	assert.NotEmpty(t, p0["CompanyName"])
}

// Batch-1 accuracy gap: EnableImportFindingsForProduct is POST /productSubscriptions.
func TestEnableImportFindingsForProductPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	rec := doRequest(t, h, http.MethodPost, "/productSubscriptions", map[string]any{
		"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	subArn, _ := resp["ProductSubscriptionArn"].(string)
	assert.NotEmpty(t, subArn)
}

// Batch-1 accuracy gap: ListEnabledProductsForImport is GET /productSubscriptions.
func TestListEnabledProductsForImportIsGETProductSubscriptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)
	doRequest(t, h, http.MethodPost, "/productSubscriptions", map[string]any{
		"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/inspector",
	})

	rec := doRequest(t, h, http.MethodGet, "/productSubscriptions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	subs, _ := resp["ProductSubscriptions"].([]any)
	assert.Len(t, subs, 1)
}

// TestParity_EnableImportFindingsForProduct_DuplicateReturns409 verifies that
// enabling the same product integration twice returns a conflict error.
func TestEnableImportFindingsForProduct_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	body := map[string]any{
		"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
	}

	rec1 := doRequest(t, h, http.MethodPost, "/productSubscriptions", body)
	assert.Equal(t, http.StatusOK, rec1.Code, "first enable must succeed")

	rec2 := doRequest(t, h, http.MethodPost, "/productSubscriptions", body)
	assert.Equal(t, http.StatusConflict, rec2.Code, "second enable must return 409 Conflict")
}

func TestBackend_DisableImportFindingsForProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		preEnable  bool
	}{
		{
			name:      "disable after enable succeeds",
			preEnable: true,
		},
		{
			name:       "disable non-existent returns error",
			preEnable:  false,
			wantErrMsg: "not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.EnableHub(false, nil))

			var subArn string
			if tc.preEnable {
				var err error
				subArn, err = b.EnableImportFindingsForProduct(
					"arn:aws:securityhub:us-east-1::product/aws/guardduty",
				)
				require.NoError(t, err)
			} else {
				subArn = "arn:aws:securityhub:us-east-1:000000000000:product-subscription/nonexistent"
			}

			err := b.DisableImportFindingsForProduct(subArn)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHandler_DisableImportFindingsForProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "disable product import", wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			doRequest(
				t,
				h,
				http.MethodPost,
				"/accounts",
				map[string]any{"EnableDefaultStandards": false},
			)

			// Enable first
			enableRec := doRequest(t, h, http.MethodPost, "/productSubscriptions", map[string]any{
				"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
			})
			require.Equal(t, http.StatusOK, enableRec.Code)

			var enableResp map[string]any
			require.NoError(t, json.Unmarshal(enableRec.Body.Bytes(), &enableResp))
			subArn, _ := enableResp["ProductSubscriptionArn"].(string)
			require.NotEmpty(t, subArn)

			// Now disable
			rec := doRequest(t, h, http.MethodDelete, "/productSubscriptions/"+subArn, nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeProductsV2_WithFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    string
		wantCode int
	}{
		{name: "list all products V2", query: "", wantCode: http.StatusOK},
		{
			name:     "filter by product ARN",
			query:    "?ProductArn=arn:aws:securityhub:us-east-1::product/aws/guardduty",
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/productsV2"+tc.query, nil)
			assert.Equal(t, tc.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			products, _ := resp["Products"].([]any)
			assert.NotNil(t, products)
		})
	}
}

func TestDescribeProductsV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, code int, resp map[string]any)
		name  string
	}{
		{
			name: "DescribeProductsV2 returns products list",
			check: func(t *testing.T, code int, resp map[string]any) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				products, _ := resp["Products"].([]any)
				assert.NotNil(t, products)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/productsV2", nil)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			tc.check(t, rec.Code, resp)
		})
	}
}

func TestRecommendedPolicyV2(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "GenerateAndGetRecommendedPolicyV2",
			steps: []step{
				{
					name:   "generate",
					method: http.MethodPost,
					path:   "/recommendedPolicyV2/metadata-uid-001",
					body:   map[string]any{},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "metadata-uid-001", resp["MetadataUid"])
						assert.NotEmpty(t, resp["Policy"])
						assert.NotEmpty(t, resp["GenerationTime"])
					},
				},
				{
					name:   "get",
					method: http.MethodGet,
					path:   "/recommendedPolicyV2/metadata-uid-001",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "metadata-uid-001", resp["MetadataUid"])
					},
				},
				{
					name:   "get non-existent returns 404",
					method: http.MethodGet,
					path:   "/recommendedPolicyV2/does-not-exist",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}
