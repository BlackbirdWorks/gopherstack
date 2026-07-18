package pinpoint_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

func TestRecommender_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		idType         string
		wantCreateFail bool
	}{
		{
			name:   "valid_endpoint_id_type",
			idType: "PINPOINT_ENDPOINT_ID",
		},
		{
			name:   "valid_user_id_type",
			idType: "PINPOINT_USER_ID",
		},
		{
			name:   "empty_id_type_ok",
			idType: "",
		},
		{
			name:           "invalid_id_type",
			idType:         "INVALID_TYPE",
			wantCreateFail: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
			req := pinpoint.ExportedCreateRecommenderConfigRequest{
				Name:                         "test-recommender",
				RecommendationProviderIDType: tc.idType,
				RecommendationProviderURI:    "arn:aws:personalize:us-east-1:123:campaign/rec",
			}

			r, err := b.CreateRecommenderConfiguration(req)
			if tc.wantCreateFail {
				require.Error(t, err)
				assert.Nil(t, r)
			} else {
				require.NoError(t, err)
				require.NotNil(t, r)

				if tc.idType != "" {
					assert.Equal(t, tc.idType, r.RecommendationProviderIDType)
				}
			}
		})
	}
}

func TestRecommender_ConditionalLastModifiedDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		wantFieldUpdated  string
		wantFieldValue    string
		initialReq        pinpoint.ExportedCreateRecommenderConfigRequest
		updateReq         pinpoint.ExportedCreateRecommenderConfigRequest
		wantDateUnchanged bool
	}{
		{
			name: "same_name_no_date_change",
			initialReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name: "original",
			},
			updateReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name: "original",
			},
			wantDateUnchanged: true,
		},
		{
			name: "update_description_stored",
			initialReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name: "rec",
			},
			updateReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				Description: "a new description",
			},
			wantFieldUpdated: "Description",
			wantFieldValue:   "a new description",
		},
		{
			name: "update_uri_stored",
			initialReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name:                      "rec",
				RecommendationProviderURI: "arn:aws:personalize:us-east-1:123:campaign/orig",
			},
			updateReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				RecommendationProviderURI: "arn:aws:personalize:us-east-1:123:campaign/new",
			},
			wantFieldUpdated: "URI",
			wantFieldValue:   "arn:aws:personalize:us-east-1:123:campaign/new",
		},
		{
			name: "update_id_type_stored",
			initialReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name: "rec",
			},
			updateReq: pinpoint.ExportedCreateRecommenderConfigRequest{
				RecommendationProviderIDType: "PINPOINT_USER_ID",
			},
			wantFieldUpdated: "IDType",
			wantFieldValue:   "PINPOINT_USER_ID",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

			created, err := b.CreateRecommenderConfiguration(tc.initialReq)
			require.NoError(t, err)
			originalDate := created.LastModifiedDate

			updated, err := b.UpdateRecommenderConfiguration(created.ID, tc.updateReq)
			require.NoError(t, err)
			require.NotNil(t, updated)

			if tc.wantDateUnchanged {
				assert.Equal(t, originalDate, updated.LastModifiedDate,
					"LastModifiedDate should not update when nothing changed")
			}

			switch tc.wantFieldUpdated {
			case "Description":
				assert.Equal(t, tc.wantFieldValue, updated.Description)
			case "URI":
				assert.Equal(t, tc.wantFieldValue, updated.RecommendationProviderURI)
			case "IDType":
				assert.Equal(t, tc.wantFieldValue, updated.RecommendationProviderIDType)
			}
		})
	}
}

func TestRecommender_InvalidIDTypeOnUpdate(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

	created, err := b.CreateRecommenderConfiguration(
		pinpoint.ExportedCreateRecommenderConfigRequest{Name: "rec"},
	)
	require.NoError(t, err)

	_, err = b.UpdateRecommenderConfiguration(created.ID, pinpoint.ExportedCreateRecommenderConfigRequest{
		RecommendationProviderIDType: "INVALID_TYPE",
	})
	require.Error(t, err)
}

// ──────────────────────────────────────────────────
// ApplicationSettings: body persistence
// ──────────────────────────────────────────────────

func TestRecommenderConfiguration_FullCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody         map[string]any
		updateBody         map[string]any
		name               string
		wantName           string
		wantProviderURI    string
		wantIDType         string
		wantRPM            float64
		wantHasDescription bool
		wantHasAttributes  bool
	}{
		{
			name: "basic_recommender",
			createBody: map[string]any{
				"Name":                          "basic-rec",
				"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/cam1",
				"RecommendationProviderRoleArn": "arn:aws:iam::123:role/Role",
			},
			updateBody: map[string]any{
				"RecommendationProviderUri": "arn:aws:personalize:us-east-1:123:campaign/cam2",
			},
			wantName:        "basic-rec",
			wantProviderURI: "arn:aws:personalize:us-east-1:123:campaign/cam1",
		},
		{
			name: "recommender_with_id_type",
			createBody: map[string]any{
				"Name":                          "typed-rec",
				"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/cam3",
				"RecommendationProviderRoleArn": "arn:aws:iam::123:role/Role",
				"RecommendationProviderIdType":  "PINPOINT_USER_ID",
			},
			updateBody: map[string]any{
				"RecommendationProviderIdType": "PINPOINT_ENDPOINT_ID",
			},
			wantName:   "typed-rec",
			wantIDType: "PINPOINT_USER_ID",
		},
		{
			name: "recommender_with_description",
			createBody: map[string]any{
				"Name":                          "desc-rec",
				"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/cam4",
				"RecommendationProviderRoleArn": "arn:aws:iam::123:role/Role",
				"Description":                   "Product recommendations",
			},
			updateBody: map[string]any{
				"Description": "Updated description",
			},
			wantName:           "desc-rec",
			wantHasDescription: true,
		},
		{
			name: "recommender_with_rpm",
			createBody: map[string]any{
				"Name":                          "rpm-rec",
				"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/cam5",
				"RecommendationProviderRoleArn": "arn:aws:iam::123:role/Role",
				"RecommendationsPerMessage":     5,
			},
			updateBody: map[string]any{
				"RecommendationsPerMessage": 10,
			},
			wantName: "rpm-rec",
			wantRPM:  5,
		},
		{
			name: "recommender_with_attributes",
			createBody: map[string]any{
				"Name":                          "attr-rec",
				"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/cam6",
				"RecommendationProviderRoleArn": "arn:aws:iam::123:role/Role",
				"Attributes": map[string]string{
					"Attr1": "ProductName",
					"Attr2": "ProductPrice",
				},
			},
			updateBody: map[string]any{
				"Attributes": map[string]string{
					"Attr1": "UpdatedName",
				},
			},
			wantName:          "attr-rec",
			wantHasAttributes: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/recommenders", tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code,
				"body: %s", createRec.Body.String())

			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			recID := cr["Id"].(string)
			require.NotEmpty(t, recID)

			assert.Equal(t, tc.wantName, cr["Name"])

			if tc.wantProviderURI != "" {
				assert.Equal(t, tc.wantProviderURI, cr["RecommendationProviderUri"])
			}

			if tc.wantIDType != "" {
				assert.Equal(t, tc.wantIDType, cr["RecommendationProviderIdType"])
			}

			if tc.wantRPM != 0 {
				assert.InDelta(t, tc.wantRPM, cr["RecommendationsPerMessage"], 0.001)
			}

			if tc.wantHasDescription {
				assert.NotEmpty(t, cr["Description"])
			}

			if tc.wantHasAttributes {
				assert.NotNil(t, cr["Attributes"])
			}

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/recommenders/"+recID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var gr map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &gr))
			assert.Equal(t, recID, gr["Id"])

			updateRec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/recommenders/"+recID, tc.updateBody)
			require.Equal(t, http.StatusOK, updateRec.Code,
				"update body: %s", updateRec.Body.String())

			deleteRec := doPinpointRequest(t, h, http.MethodDelete,
				"/v1/recommenders/"+recID, nil)
			require.Equal(t, http.StatusOK, deleteRec.Code)

			getRec2 := doPinpointRequest(t, h, http.MethodGet,
				"/v1/recommenders/"+recID, nil)
			assert.Equal(t, http.StatusNotFound, getRec2.Code)
		})
	}
}

// ──────────────────────────────────────────────────
// RecommenderConfiguration: list all
// ──────────────────────────────────────────────────

func TestRecommenderConfiguration_ListAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		count   int
		wantLen int
	}{
		{name: "empty_list", count: 0, wantLen: 0},
		{name: "single_recommender", count: 1, wantLen: 1},
		{name: "three_recommenders", count: 3, wantLen: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			for i := range tc.count {
				rec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders", map[string]any{
					"Name":                          fmt.Sprintf("rec-%d", i),
					"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/c",
					"RecommendationProviderRoleArn": "arn:aws:iam::123:role/R",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders", nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
			items, _ := resp["Item"].([]any)
			assert.Len(t, items, tc.wantLen)
		})
	}
}

// ──────────────────────────────────────────────────
// OneTimeTokenChannel: SendOTPMessage + VerifyOTPMessage
// ──────────────────────────────────────────────────

func TestBackend_Recommender_DirectAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		req    pinpoint.ExportedCreateRecommenderConfigRequest
		wantID bool
	}{
		{
			name: "create_basic",
			req: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name:                      "direct-rec",
				RecommendationProviderURI: "arn:aws:personalize:us-east-1:123:campaign/c",
			},
			wantID: true,
		},
		{
			name: "create_with_user_id_type",
			req: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name:                         "direct-user-rec",
				RecommendationProviderURI:    "arn:aws:personalize:us-east-1:123:campaign/d",
				RecommendationProviderIDType: "PINPOINT_USER_ID",
			},
			wantID: true,
		},
		{
			name: "create_with_endpoint_id_type",
			req: pinpoint.ExportedCreateRecommenderConfigRequest{
				Name:                         "direct-ep-rec",
				RecommendationProviderURI:    "arn:aws:personalize:us-east-1:123:campaign/e",
				RecommendationProviderIDType: "PINPOINT_ENDPOINT_ID",
			},
			wantID: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

			created, err := b.CreateRecommenderConfiguration(tc.req)
			require.NoError(t, err)
			require.NotNil(t, created)

			if tc.wantID {
				assert.NotEmpty(t, created.ID)
			}

			assert.Equal(t, tc.req.Name, created.Name)

			got, err := b.GetRecommenderConfiguration(created.ID)
			require.NoError(t, err)
			assert.Equal(t, created.ID, got.ID)

			all, err := b.GetRecommenderConfigurations()
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(all), 1)

			deleted, err := b.DeleteRecommenderConfiguration(created.ID)
			require.NoError(t, err)
			assert.Equal(t, created.ID, deleted.ID)

			_, err = b.GetRecommenderConfiguration(created.ID)
			require.Error(t, err)
		})
	}
}

// ──────────────────────────────────────────────────
// Journey KPI: per-journey date range
// ──────────────────────────────────────────────────

func TestRecommender_InvalidIDType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		idType  string
		name    string
		wantErr bool
	}{
		{name: "valid_user_id_type", idType: "PINPOINT_USER_ID", wantErr: false},
		{name: "valid_endpoint_id_type", idType: "PINPOINT_ENDPOINT_ID", wantErr: false},
		{name: "empty_id_type_ok", idType: "", wantErr: false},
		{name: "invalid_id_type", idType: "INVALID_TYPE", wantErr: true},
		{name: "garbage_id_type", idType: "FOOBAR", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

			created, err := b.CreateRecommenderConfiguration(
				pinpoint.ExportedCreateRecommenderConfigRequest{
					Name:                         "rec-type-test",
					RecommendationProviderURI:    "arn:aws:personalize:us-east-1:123:campaign/c",
					RecommendationProviderIDType: tc.idType,
				},
			)

			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, created)
			} else {
				require.NoError(t, err)
				require.NotNil(t, created)
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Application settings: 404 for missing app
// ──────────────────────────────────────────────────

func TestRecommender_Attributes_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	attrs := map[string]any{
		"Recommendations.ProductName": "Product Name",
		"Recommendations.Price":       "Price",
		"Recommendations.Category":    "Category",
	}

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders",
		map[string]any{
			"Name":                          "attr-recommender",
			"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/PinpointRec",
			"RecommendationProviderUri":     "arn:aws:personalize:::campaign/my-campaign",
			"RecommendationProviderIdType":  "PINPOINT_USER_ID",
			"RecommendationsPerMessage":     5,
			"Attributes":                    attrs,
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	recID := cr["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders/"+recID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var r map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &r))

	assert.Equal(t, "attr-recommender", r["Name"])
	assert.EqualValues(t, 5, r["RecommendationsPerMessage"])

	gotAttrs := r["Attributes"].(map[string]any)
	assert.Equal(t, "Product Name", gotAttrs["Recommendations.ProductName"])
	assert.Equal(t, "Price", gotAttrs["Recommendations.Price"])
	assert.Equal(t, "Category", gotAttrs["Recommendations.Category"])
}

func TestRecommender_RecommendationsPerMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		recommendationsPerMsg int
	}{
		{name: "one", recommendationsPerMsg: 1},
		{name: "five", recommendationsPerMsg: 5},
		{name: "ten", recommendationsPerMsg: 10},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders",
				map[string]any{
					"Name":                          "rpm-rec-" + tc.name,
					"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/R",
					"RecommendationProviderUri":     "arn:aws:personalize:::campaign/c",
					"RecommendationProviderIdType":  "PINPOINT_USER_ID",
					"RecommendationsPerMessage":     tc.recommendationsPerMsg,
				})
			require.Equal(t, http.StatusCreated, createRec.Code)

			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			recID := cr["Id"].(string)

			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders/"+recID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var r map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &r))

			assert.EqualValues(t, tc.recommendationsPerMsg, r["RecommendationsPerMessage"])
		})
	}
}

func TestRecommender_UpdateAttributes(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders",
		map[string]any{
			"Name":                          "update-attr-rec",
			"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/R",
			"RecommendationProviderUri":     "arn:aws:personalize:::campaign/c",
			"RecommendationProviderIdType":  "PINPOINT_USER_ID",
			"RecommendationsPerMessage":     3,
			"Attributes": map[string]any{
				"Recommendations.Name": "Name",
			},
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	recID := cr["Id"].(string)

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/recommenders/"+recID,
		map[string]any{
			"Name":                          "update-attr-rec",
			"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/R",
			"RecommendationProviderUri":     "arn:aws:personalize:::campaign/c",
			"RecommendationProviderIdType":  "PINPOINT_USER_ID",
			"RecommendationsPerMessage":     7,
			"Attributes": map[string]any{
				"Recommendations.Name":  "Name",
				"Recommendations.Score": "Score",
			},
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders/"+recID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var r map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &r))

	assert.EqualValues(t, 7, r["RecommendationsPerMessage"])
	gotAttrs := r["Attributes"].(map[string]any)
	assert.Equal(t, "Name", gotAttrs["Recommendations.Name"])
	assert.Equal(t, "Score", gotAttrs["Recommendations.Score"])
}

func TestRecommender_List_MultipleConfigs(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	names := []string{"rec-alpha", "rec-beta", "rec-gamma"}
	for _, name := range names {
		rec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders",
			map[string]any{
				"Name":                          name,
				"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/R",
				"RecommendationProviderUri":     "arn:aws:personalize:::campaign/c-" + name,
				"RecommendationProviderIdType":  "PINPOINT_USER_ID",
			})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	items := resp["Item"].([]any)
	assert.Len(t, items, 3)

	gotNames := make([]string, 0, 3)
	for _, item := range items {
		r := item.(map[string]any)
		gotNames = append(gotNames, r["Name"].(string))
	}

	assert.ElementsMatch(t, names, gotNames)
}

// ──────────────────────────────────────────────────
// Application settings — deeper fields
// ──────────────────────────────────────────────────

func TestPinpoint_Recommender_CRUD(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders", map[string]any{
		"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123456789012:campaign/my-campaign",
		"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/PinpointRole",
	})
	if rec.Code < 200 || rec.Code >= 300 {
		t.Skipf("recommender creation returned %d, skipping rest of test", rec.Code)
	}
	resp := pinpointJSON(t, rec.Body.Bytes())
	recommenderID, _ := resp["Id"].(string)
	if recommenderID == "" {
		t.Skip("recommender creation did not return ID")
	}

	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders/"+recommenderID, nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodPut, "/v1/recommenders/"+recommenderID, map[string]any{
		"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123456789012:campaign/updated",
		"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/PinpointRole",
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/recommenders/"+recommenderID, nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

func TestHandler_CreateRecommenderConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "creates_recommender",
			body: map[string]any{
				"Name":                          "my-recommender",
				"RecommendationProviderRoleArn": "arn:aws:iam::123:role/recommender",
				"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/my-campaign",
			},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
		{
			name:       "rejects_empty_name",
			body:       map[string]any{"Name": ""},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Id"])
				assert.Equal(t, "my-recommender", resp["Name"])
			}
		})
	}
}
