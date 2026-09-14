package apigateway

import (
	"encoding/json"
	"net/http"
)

const (
	opGetSdk      = "GetSdk"
	opGetSdkType  = "GetSdkType"
	opGetSdkTypes = "GetSdkTypes"
)

// sdkTypeView is the response for GetSdkType, and one entry of GetSdkTypes.
type sdkTypeView struct {
	ID           string `json:"id"`
	FriendlyName string `json:"friendlyName,omitempty"`
}

// sdkTypesView is the response for GetSdkTypes.
type sdkTypesView struct {
	Items []sdkTypeView `json:"item"`
}

// parseAPIGWSdkTypesPath handles /sdktypes/... paths.
func parseAPIGWSdkTypesPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch n {
	case pathDepth1:
		if method == http.MethodGet {
			return opGetSdkTypes, nil, true
		}
	case pathDepth2:
		if method == http.MethodGet {
			return opGetSdkType, map[string]string{keySdkTypeID: segs[1]}, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// sdkActions returns real handlers for the SDK generation operations, backed
// by the fixed SDK type catalog and a real per-API/stage generated package.
func (h *Handler) sdkActions() map[string]actionFn {
	return map[string]actionFn{
		opGetSdk: func(b []byte) (int, any, error) {
			var input struct {
				RestAPIID string `json:"restApiId"`
				StageName string `json:"stageName"`
				SdkType   string `json:"sdkType"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			sdk, err := h.Backend.GetSdk(input.RestAPIID, input.StageName, input.SdkType)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, &rawBinaryResponse{
				contentType:        sdk.ContentType,
				contentDisposition: sdk.ContentDisposition,
				body:               sdk.Body,
			}, nil
		},
		opGetSdkType: func(b []byte) (int, any, error) {
			var input struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			t, err := h.Backend.GetSdkType(input.ID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, &sdkTypeView{ID: t.ID, FriendlyName: t.FriendlyName}, nil
		},
		opGetSdkTypes: func(b []byte) (int, any, error) {
			var input struct {
				Limit int `json:"limit,omitempty"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			catalog := h.Backend.GetSdkTypes()
			// GetSdkTypesOutput has no Position/continuation-token field
			// (apigateway@v1.42.4 api_op_GetSdkTypes.go:41-49, Items only), so
			// Limit can only truncate -- there is no wire-shape way to hand
			// back a cursor for a further page.
			if input.Limit > 0 && input.Limit < len(catalog) {
				catalog = catalog[:input.Limit]
			}

			items := make([]sdkTypeView, 0, len(catalog))
			for _, t := range catalog {
				items = append(items, sdkTypeView(t))
			}

			return http.StatusOK, &sdkTypesView{Items: items}, nil
		},
	}
}
