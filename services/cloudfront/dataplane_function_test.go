package cloudfront_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func publishFn(t *testing.T, f *dpFixture, name, code string) string {
	t.Helper()

	etag := createFn(t, f.cf, name, "cloudfront-js-2.0", code)

	pub, err := f.cf.PublishFunction(t.Context(), &cfsdk.PublishFunctionInput{
		Name: aws.String(name), IfMatch: aws.String(etag),
	})
	require.NoError(t, err)

	return aws.ToString(pub.FunctionSummary.FunctionMetadata.FunctionARN)
}

func TestDataPlane_ViewerFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantHeader [2]string
		name       string
		reqCode    string
		respCode   string
		path       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "request_rewrites_uri",
			reqCode:    `function handler(e) { e.request.uri = '/rewritten'; return e.request; }`,
			path:       "/orig",
			wantStatus: http.StatusOK, wantBody: "/rewritten",
		},
		{
			name:       "request_short_circuits",
			reqCode:    `function handler(e) { return {statusCode: 302, headers: {location: {value: '/x'}}}; }`,
			path:       "/orig",
			wantStatus: http.StatusFound,
			wantHeader: [2]string{"Location", "/x"},
		},
		{
			name:       "response_sets_header",
			respCode:   `function handler(e) { e.response.headers['x-fn'] = {value: 'resp'}; return e.response; }`,
			path:       "/orig",
			wantStatus: http.StatusOK, wantBody: "/orig", wantHeader: [2]string{"X-Fn", "resp"},
		},
		{
			name:       "request_error_is_502",
			reqCode:    `function handler(e) { throw new Error('x'); }`,
			path:       "/orig",
			wantStatus: http.StatusBadGateway,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			origin := newOriginServer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(r.URL.Path))
			})
			cachePol := f.cachePolicy(t, 0)

			def := behavior("api", cachePol)
			assoc := &types.FunctionAssociations{}

			if tt.reqCode != "" {
				assoc.Items = append(assoc.Items, types.FunctionAssociation{
					EventType:   types.EventTypeViewerRequest,
					FunctionARN: aws.String(publishFn(t, f, "req", tt.reqCode)),
				})
			}

			if tt.respCode != "" {
				assoc.Items = append(assoc.Items, types.FunctionAssociation{
					EventType:   types.EventTypeViewerResponse,
					FunctionARN: aws.String(publishFn(t, f, "resp", tt.respCode)),
				})
			}

			assoc.Quantity = aws.Int32(int32(len(assoc.Items)))
			def.FunctionAssociations = assoc

			d := f.dist(t, distSpec{origins: []types.Origin{customOrigin("api", origin.hostPort())}, def: def})

			got := f.viewer(t, d, http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.wantStatus, got.status, got.body)

			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, got.body)
			}

			if tt.wantHeader[0] != "" {
				assert.Equal(t, tt.wantHeader[1], got.header.Get(tt.wantHeader[0]))
			}
		})
	}
}
