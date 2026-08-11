package route53resolver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestResolverQueryLogConfig_AssociationCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
		"Name":           "qlc-assoc-count",
		"DestinationArn": "arn:aws:s3:::my-query-logs",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	cfg := createResp["ResolverQueryLogConfig"].(map[string]any)
	cfgID := cfg["Id"].(string)
	assert.EqualValues(t, 0, cfg["AssociationCount"])
	assert.Equal(t, "NOT_SHARED", cfg["ShareStatus"])
	assert.NotEmpty(t, cfg["CreationTime"])

	// Associate once.
	doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": cfgID,
		"ResourceId":               "vpc-aaa",
	})

	// Associate again.
	assocRec := doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": cfgID,
		"ResourceId":               "vpc-bbb",
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	// Get config and verify count.
	getRec := doRequest(
		t,
		h,
		"GetResolverQueryLogConfig",
		map[string]any{"ResolverQueryLogConfigId": cfgID},
	)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	cfg = getResp["ResolverQueryLogConfig"].(map[string]any)
	assert.EqualValues(t, 2, cfg["AssociationCount"])
}

// --- Issue 15: DestinationArn validation ---

func TestResolverQueryLogConfig_DestinationArnValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		destinationArn string
		wantCode       int
	}{
		{
			name:           "s3_arn_valid",
			destinationArn: "arn:aws:s3:::my-bucket",
			wantCode:       http.StatusOK,
		},
		{
			name:           "cloudwatch_logs_arn_valid",
			destinationArn: "arn:aws:logs:us-east-1:000000000000:log-group:/aws/route53/query",
			wantCode:       http.StatusOK,
		},
		{
			name:           "firehose_arn_valid",
			destinationArn: "arn:aws:firehose:us-east-1:000000000000:deliverystream/my-stream",
			wantCode:       http.StatusOK,
		},
		{
			name:           "invalid_arn_rejected",
			destinationArn: "arn:aws:ec2:us-east-1:000000000000:instance/i-12345",
			wantCode:       http.StatusBadRequest,
		},
		{
			name:           "empty_arn_rejected",
			destinationArn: "",
			wantCode:       http.StatusBadRequest,
		},
		{
			name:           "plain_string_rejected",
			destinationArn: "not-an-arn",
			wantCode:       http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"Name": "qlc-dest-test",
			}
			if tt.destinationArn != "" {
				body["DestinationArn"] = tt.destinationArn
			}
			rec := doRequest(t, h, "CreateResolverQueryLogConfig", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- Issue 17: ResolverDnssecConfig ValidationStatus ---

func TestResolverQueryLogConfig_AssociationCountDecrement(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
		"Name":           "qlc-decrement",
		"DestinationArn": "arn:aws:s3:::my-logs",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	cfgID := createResp["ResolverQueryLogConfig"].(map[string]any)["Id"].(string)

	// Associate.
	assocRec := doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": cfgID,
		"ResourceId":               "vpc-dec-test",
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	// Disassociate: the real API keys this by (ResolverQueryLogConfigId,
	// ResourceId), not the opaque association ID returned by Associate/Get.
	doRequest(t, h, "DisassociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": cfgID,
		"ResourceId":               "vpc-dec-test",
	})

	// Count should be 0.
	getRec := doRequest(
		t,
		h,
		"GetResolverQueryLogConfig",
		map[string]any{"ResolverQueryLogConfigId": cfgID},
	)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	cfg := getResp["ResolverQueryLogConfig"].(map[string]any)
	assert.EqualValues(t, 0, cfg["AssociationCount"])
}

// --- QueryLogConfigAssociation Error/ErrorMessage fields ---

func TestBackend_QueryLogDestinationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		destinationArn string
		wantErr        bool
	}{
		{name: "s3_valid", destinationArn: "arn:aws:s3:::my-bucket", wantErr: false},
		{
			name:           "cloudwatch_valid",
			destinationArn: "arn:aws:logs:us-east-1:000000000000:log-group:/test",
			wantErr:        false,
		},
		{
			name:           "firehose_valid",
			destinationArn: "arn:aws:firehose:us-east-1:000000000000:deliverystream/s",
			wantErr:        false,
		},
		{
			name:           "ec2_invalid",
			destinationArn: "arn:aws:ec2:us-east-1:000000000000:instance/i-abc",
			wantErr:        true,
		},
		{name: "empty_invalid", destinationArn: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateResolverQueryLogConfig(context.Background(), "qlc", "req-1", tt.destinationArn)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// --- Backend direct: DNSSEC default DISABLED ---

func TestQueryLogConfigDeleteAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "delete_and_list", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a config.
			rec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
				"Name":           "cfg-del",
				"DestinationArn": "arn:aws:s3:::my-bucket",
			})
			require.Equal(t, tt.wantCode, rec.Code)
			resp := decodeJSON(t, rec)
			cfg, _ := resp["ResolverQueryLogConfig"].(map[string]any)
			cfgID := cfg["Id"].(string)

			// ListResolverQueryLogConfigs.
			rec = doRequest(t, h, "ListResolverQueryLogConfigs", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			listResp := decodeJSON(t, rec)
			items, _ := listResp["ResolverQueryLogConfigs"].([]any)
			assert.Len(t, items, 1)

			// DeleteResolverQueryLogConfig.
			rec = doRequest(t, h, "DeleteResolverQueryLogConfig", map[string]any{
				"ResolverQueryLogConfigId": cfgID,
			})
			assert.Equal(t, http.StatusOK, rec.Code)
			delResp := decodeJSON(t, rec)
			deleted, _ := delResp["ResolverQueryLogConfig"].(map[string]any)
			assert.Equal(t, cfgID, deleted["Id"])

			// Verify gone.
			rec = doRequest(t, h, "ListResolverQueryLogConfigs", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			afterResp := decodeJSON(t, rec)
			afterItems, _ := afterResp["ResolverQueryLogConfigs"].([]any)
			assert.Empty(t, afterItems)
		})
	}
}

func TestQueryLogConfigErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "delete_missing_id",
			action:   "DeleteResolverQueryLogConfig",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete_not_found",
			action:   "DeleteResolverQueryLogConfig",
			body:     map[string]any{"ResolverQueryLogConfigId": "rqlc-notexist"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- GetResolverQueryLogConfigAssociation / ListResolverQueryLogConfigAssociations ---

func TestQueryLogConfigPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		arn        string
		policy     string
		wantPolicy string
		wantCode   int
	}{
		{
			name:       "put_and_get",
			arn:        "arn:aws:route53resolver:us-east-1:000000000000:resolver-query-log-config/rqlc-abc",
			policy:     `{"Version":"2012-10-17"}`,
			wantPolicy: `{"Version":"2012-10-17"}`,
			wantCode:   http.StatusOK,
		},
		{
			name:     "put_missing_arn",
			arn:      "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_missing_arn",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "get_missing_arn" {
				rec := doRequest(t, h, "GetResolverQueryLogConfigPolicy", map[string]any{})
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			if tt.arn == "" {
				rec := doRequest(t, h, "PutResolverQueryLogConfigPolicy", map[string]any{
					"ResolverQueryLogConfigPolicy": tt.policy,
				})
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			// Put.
			rec := doRequest(t, h, "PutResolverQueryLogConfigPolicy", map[string]any{
				"Arn":                          tt.arn,
				"ResolverQueryLogConfigPolicy": tt.policy,
			})
			require.Equal(t, tt.wantCode, rec.Code)
			putResp := decodeJSON(t, rec)
			assert.Equal(t, true, putResp["ReturnValue"])

			// Get.
			rec = doRequest(t, h, "GetResolverQueryLogConfigPolicy", map[string]any{"Arn": tt.arn})
			assert.Equal(t, http.StatusOK, rec.Code)
			getResp := decodeJSON(t, rec)
			assert.Equal(t, tt.wantPolicy, getResp["ResolverQueryLogConfigPolicy"])
		})
	}
}

// --- GetResolverRuleAssociation / DisassociateResolverRule / ListResolverRuleAssociations ---

func TestCreateResolverQueryLogConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantName string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"Name":             "my-log-config",
				"DestinationArn":   "arn:aws:s3:::my-bucket",
				"CreatorRequestId": "req-log-1",
			},
			wantCode: http.StatusOK,
			wantName: "my-log-config",
		},
		{
			name:     "missing_name",
			body:     map[string]any{"DestinationArn": "arn:aws:s3:::bucket"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_destination_arn",
			body:     map[string]any{"Name": "cfg"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResolverQueryLogConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				cfg, ok := resp["ResolverQueryLogConfig"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, cfg["Name"])
				assert.Equal(t, "CREATED", cfg["Status"])
				assert.NotEmpty(t, cfg["Id"])
				assert.Contains(t, cfg["Arn"].(string), "arn:aws:route53resolver:")
			}
		})
	}
}

// --- AssociateResolverQueryLogConfig ---

// TestParity_ListResolverQueryLogConfigs_Pagination verifies NextToken/MaxResults on
// ListResolverQueryLogConfigs.
func TestListResolverQueryLogConfigs_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
			"Name":           fmt.Sprintf("cfg-%d", i),
			"DestinationArn": fmt.Sprintf("arn:aws:s3:::bucket-%d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListResolverQueryLogConfigs", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			cfgs, _ := out["ResolverQueryLogConfigs"].([]any)
			assert.Len(t, cfgs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

func TestListResolverQueryLogConfigs_Filters(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *route53resolver.Handler {
		t.Helper()
		h := newTestHandler(t)
		doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
			"Name": "s3-cfg", "DestinationArn": "arn:aws:s3:::my-bucket",
			"CreatorRequestId": "req-s3",
		})
		doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
			"Name":           "cwl-cfg",
			"DestinationArn": "arn:aws:logs:us-east-1:000000000000:log-group:/my/group",
		})

		return h
	}

	tests := []struct {
		filter    map[string]any
		name      string
		wantNames []string
	}{
		{
			name:      "name canonical",
			filter:    map[string]any{"Name": "Name", "Values": []string{"s3-cfg"}},
			wantNames: []string{"s3-cfg"},
		},
		{
			name:      "name legacy uppercase",
			filter:    map[string]any{"Name": "NAME", "Values": []string{"cwl-cfg"}},
			wantNames: []string{"cwl-cfg"},
		},
		{
			name:      "creator request id",
			filter:    map[string]any{"Name": "CreatorRequestId", "Values": []string{"req-s3"}},
			wantNames: []string{"s3-cfg"},
		},
		{
			name:      "destination derived from arn kind",
			filter:    map[string]any{"Name": "Destination", "Values": []string{"CloudWatchLogs"}},
			wantNames: []string{"cwl-cfg"},
		},
		{
			name:      "values are OR-combined",
			filter:    map[string]any{"Name": "Destination", "Values": []string{"S3", "CloudWatchLogs"}},
			wantNames: []string{"s3-cfg", "cwl-cfg"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := setup(t)

			rec := doRequest(t, h, "ListResolverQueryLogConfigs", map[string]any{
				"Filters": []map[string]any{tt.filter},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			cfgs, _ := resp["ResolverQueryLogConfigs"].([]any)
			gotNames := make([]string, len(cfgs))
			for i, c := range cfgs {
				gotNames[i] = c.(map[string]any)["Name"].(string)
			}
			assert.ElementsMatch(t, tt.wantNames, gotNames)
		})
	}
}

func TestListResolverQueryLogConfigs_Sort(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *route53resolver.Handler {
		t.Helper()
		h := newTestHandler(t)
		doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
			"Name": "charlie", "DestinationArn": "arn:aws:s3:::bucket",
		})
		doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
			"Name": "alpha", "DestinationArn": "arn:aws:s3:::bucket",
		})
		doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
			"Name": "bravo", "DestinationArn": "arn:aws:s3:::bucket",
		})

		return h
	}

	tests := []struct {
		body      map[string]any
		name      string
		wantNames []string
	}{
		{
			name:      "sort_by_name_ascending_explicit",
			body:      map[string]any{"SortBy": "Name", "SortOrder": "ASCENDING"},
			wantNames: []string{"alpha", "bravo", "charlie"},
		},
		{
			name:      "sort_by_name_default_order",
			body:      map[string]any{"SortBy": "Name"},
			wantNames: []string{"alpha", "bravo", "charlie"},
		},
		{
			name:      "sort_by_name_descending",
			body:      map[string]any{"SortBy": "Name", "SortOrder": "DESCENDING"},
			wantNames: []string{"charlie", "bravo", "alpha"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := setup(t)

			rec := doRequest(t, h, "ListResolverQueryLogConfigs", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := decodeJSON(t, rec)
			cfgs, _ := resp["ResolverQueryLogConfigs"].([]any)
			gotNames := make([]string, len(cfgs))
			for i, c := range cfgs {
				gotNames[i] = c.(map[string]any)["Name"].(string)
			}
			assert.Equal(t, tt.wantNames, gotNames)
		})
	}
}

func TestListResolverQueryLogConfigs_SortAppliesBeforePagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	names := []string{"delta", "alpha", "charlie", "bravo"}
	for _, n := range names {
		doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
			"Name": n, "DestinationArn": "arn:aws:s3:::bucket",
		})
	}

	// DESCENDING: the backend's own incidental ordering (see
	// ListResolverQueryLogConfigs's sort.Slice by Name ascending) is the
	// opposite direction, so this only passes once SortBy/SortOrder are
	// actually honored -- an ASCENDING-only assertion here would pass by
	// coincidence even with SortBy fully dropped.
	rec := doRequest(t, h, "ListResolverQueryLogConfigs", map[string]any{
		"SortBy": "Name", "SortOrder": "DESCENDING", "MaxResults": float64(2),
	})
	require.Equal(t, http.StatusOK, rec.Code)
	page1 := decodeJSON(t, rec)
	cfgs, _ := page1["ResolverQueryLogConfigs"].([]any)
	require.Len(t, cfgs, 2)
	assert.Equal(t, "delta", cfgs[0].(map[string]any)["Name"])
	assert.Equal(t, "charlie", cfgs[1].(map[string]any)["Name"])
	nextToken, _ := page1["NextToken"].(string)
	require.NotEmpty(t, nextToken)

	rec = doRequest(t, h, "ListResolverQueryLogConfigs", map[string]any{
		"SortBy": "Name", "SortOrder": "DESCENDING", "NextToken": nextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	page2 := decodeJSON(t, rec)
	cfgs, _ = page2["ResolverQueryLogConfigs"].([]any)
	require.Len(t, cfgs, 2)
	assert.Equal(t, "bravo", cfgs[0].(map[string]any)["Name"])
	assert.Equal(t, "alpha", cfgs[1].(map[string]any)["Name"])
}

func TestListResolverQueryLogConfigs_UnknownSortByRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
		"Name": "cfg1", "DestinationArn": "arn:aws:s3:::bucket",
	})

	rec := doRequest(t, h, "ListResolverQueryLogConfigs", map[string]any{
		"SortBy": "NotARealSortKey",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	resp := decodeJSON(t, rec)
	assert.Equal(t, "InvalidParameterException", resp["__type"])
}

func TestListResolverQueryLogConfigs_UnknownSortOrderRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
		"Name": "cfg1", "DestinationArn": "arn:aws:s3:::bucket",
	})

	rec := doRequest(t, h, "ListResolverQueryLogConfigs", map[string]any{
		"SortBy": "Name", "SortOrder": "SIDEWAYS",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	resp := decodeJSON(t, rec)
	assert.Equal(t, "InvalidParameterException", resp["__type"])
}

func TestListResolverQueryLogConfigs_UnknownFilterNameRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
		"Name": "cfg1", "DestinationArn": "arn:aws:s3:::bucket",
	})

	rec := doRequest(t, h, "ListResolverQueryLogConfigs", map[string]any{
		"Filters": []map[string]any{
			{"Name": "NotARealFilter", "Values": []string{"x"}},
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidParameterException", resp["__type"])
}
