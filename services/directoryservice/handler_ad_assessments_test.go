package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestADAssessments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "start list describe delete cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Start
			rec1 := doRequest(t, h, "StartADAssessment", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			assessID, _ := r1["AssessmentId"].(string)
			assert.NotEmpty(t, assessID)

			// List
			rec2 := doRequest(t, h, "ListADAssessments", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			assessments, _ := r2["ADAssessments"].([]any)
			assert.Len(t, assessments, 1)

			// Describe
			rec3 := doRequest(t, h, "DescribeADAssessment", map[string]any{
				"DirectoryId":  dirID,
				"AssessmentId": assessID,
			})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			assessment, _ := r3["ADAssessment"].(map[string]any)
			assert.Equal(t, assessID, assessment["AssessmentId"])

			// ReportType is a real Assessment field ("CUSTOMER" or "SYSTEM");
			// "AssessmentType" and "Region" are not real Assessment/
			// AssessmentSummary members and must not appear on the wire.
			assert.Equal(t, "CUSTOMER", assessment["ReportType"])
			assert.NotContains(t, assessment, "AssessmentType")
			assert.NotContains(t, assessment, "Region")

			// Delete
			rec4 := doRequest(t, h, "DeleteADAssessment", map[string]any{
				"DirectoryId":  dirID,
				"AssessmentId": assessID,
			})
			assert.Equal(t, http.StatusOK, rec4.Code)

			// List after delete
			rec5 := doRequest(t, h, "ListADAssessments", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec5.Code)
			var r5 map[string]any
			require.NoError(t, json.Unmarshal(rec5.Body.Bytes(), &r5))
			assessments2, _ := r5["ADAssessments"].([]any)
			assert.Empty(t, assessments2)

			_ = tc
		})
	}
}

// TestStartADAssessment_ConfigurationValidation covers StartADAssessmentInput's
// real, optional AssessmentConfiguration member (types.AssessmentConfiguration):
// when supplied at all, CustomerDnsIps/DnsName/InstanceIds/VpcSettings are
// required (confirmed against the SDK's validateAssessmentConfiguration).
func TestStartADAssessment_ConfigurationValidation(t *testing.T) {
	t.Parallel()

	validVPC := map[string]any{"VpcId": "vpc-0123456789abcdef0", "SubnetIds": []string{"subnet-1", "subnet-2"}}

	tests := []struct {
		cfg  map[string]any
		name string
	}{
		{
			name: "missing CustomerDnsIps",
			cfg: map[string]any{
				"DnsName":     "corp.example.com",
				"InstanceIds": []string{"i-0123456789abcdef0"},
				"VpcSettings": validVPC,
			},
		},
		{
			name: "missing DnsName",
			cfg: map[string]any{
				"CustomerDnsIps": []string{"10.0.0.1"},
				"InstanceIds":    []string{"i-0123456789abcdef0"},
				"VpcSettings":    validVPC,
			},
		},
		{
			name: "missing InstanceIds",
			cfg: map[string]any{
				"CustomerDnsIps": []string{"10.0.0.1"},
				"DnsName":        "corp.example.com",
				"VpcSettings":    validVPC,
			},
		},
		{
			name: "missing VpcSettings",
			cfg: map[string]any{
				"CustomerDnsIps": []string{"10.0.0.1"},
				"DnsName":        "corp.example.com",
				"InstanceIds":    []string{"i-0123456789abcdef0"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			rec := doRequest(t, h, "StartADAssessment", map[string]any{
				"DirectoryId":             dirID,
				"AssessmentConfiguration": tc.cfg,
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errBody map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errBody))
			assert.Equal(t, "InvalidParameterException", errBody["__type"])
		})
	}
}

// TestStartADAssessment_ConfigurationRoundTrip proves the real
// AssessmentConfiguration request member (gopherstack-10hx follow-up: the
// AD-assessment configuration gap) is genuinely captured and surfaced --
// real, non-fabricated data on DescribeADAssessment's Assessment shape, and
// the correct field-set SUBSET (no SecurityGroupIds/SelfManagedInstanceIds/
// SubnetIds/VpcId) on ListADAssessments' AssessmentSummary shape, matching
// the real types.Assessment vs types.AssessmentSummary member sets.
func TestStartADAssessment_ConfigurationRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "full AssessmentConfiguration round trip"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			rec := doRequest(t, h, "StartADAssessment", map[string]any{
				"DirectoryId": dirID,
				"AssessmentConfiguration": map[string]any{
					"CustomerDnsIps":   []string{"10.0.0.1", "10.0.0.2"},
					"DnsName":          "corp.example.com",
					"InstanceIds":      []string{"i-0123456789abcdef0"},
					"SecurityGroupIds": []string{"sg-0123456789abcdef0"},
					"VpcSettings": map[string]any{
						"VpcId":     "vpc-0123456789abcdef0",
						"SubnetIds": []string{"subnet-1", "subnet-2"},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			assessID, _ := startResp["AssessmentId"].(string)
			require.NotEmpty(t, assessID)

			// Describe (real Assessment shape): every AssessmentConfiguration
			// field must round-trip, plus LastUpdateDateTime.
			descRec := doRequest(t, h, "DescribeADAssessment", map[string]any{
				"DirectoryId":  dirID,
				"AssessmentId": assessID,
			})
			require.Equal(t, http.StatusOK, descRec.Code)
			var descResp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
			assessment, _ := descResp["ADAssessment"].(map[string]any)

			assert.ElementsMatch(t, []any{"10.0.0.1", "10.0.0.2"}, assessment["CustomerDnsIps"])
			assert.Equal(t, "corp.example.com", assessment["DnsName"])
			assert.ElementsMatch(t, []any{"i-0123456789abcdef0"}, assessment["SelfManagedInstanceIds"])
			assert.ElementsMatch(t, []any{"sg-0123456789abcdef0"}, assessment["SecurityGroupIds"])
			assert.ElementsMatch(t, []any{"subnet-1", "subnet-2"}, assessment["SubnetIds"])
			assert.Equal(t, "vpc-0123456789abcdef0", assessment["VpcId"])
			assert.NotZero(t, assessment["LastUpdateDateTime"])

			// StatusCode/StatusReason/Version are real Assessment members this
			// backend cannot honestly derive (see ADAssessmentInfo's doc
			// comment) -- must never appear fabricated on the wire.
			assert.NotContains(t, assessment, "StatusCode")
			assert.NotContains(t, assessment, "StatusReason")
			assert.NotContains(t, assessment, "Version")

			// List (AssessmentSummary shape): CustomerDnsIps/DnsName/
			// LastUpdateDateTime are real AssessmentSummary members and must
			// be present; SecurityGroupIds/SelfManagedInstanceIds/SubnetIds/
			// VpcId are Assessment-only and must NOT leak onto the summary.
			listRec := doRequest(t, h, "ListADAssessments", map[string]any{"DirectoryId": dirID})
			require.Equal(t, http.StatusOK, listRec.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			summaries, _ := listResp["ADAssessments"].([]any)
			require.Len(t, summaries, 1)
			summary, _ := summaries[0].(map[string]any)

			assert.ElementsMatch(t, []any{"10.0.0.1", "10.0.0.2"}, summary["CustomerDnsIps"])
			assert.Equal(t, "corp.example.com", summary["DnsName"])
			assert.NotZero(t, summary["LastUpdateDateTime"])
			assert.NotContains(t, summary, "SecurityGroupIds")
			assert.NotContains(t, summary, "SelfManagedInstanceIds")
			assert.NotContains(t, summary, "SubnetIds")
			assert.NotContains(t, summary, "VpcId")
		})
	}
}
