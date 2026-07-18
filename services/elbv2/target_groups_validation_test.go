package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteTargetGroupMissingARN tests missing ARN for delete.
func TestDeleteTargetGroupMissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DeleteTargetGroup"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDeleteTargetGroupNotFound tests not found for delete.
func TestDeleteTargetGroupNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DeleteTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/no-such/0"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestPortValidationCreateTargetGroup tests port validation for CreateTargetGroup.
func TestPortValidationCreateTargetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		port       string
		wantStatus int
	}{
		{"port_zero", "0", http.StatusBadRequest},
		{"port_65536", "65536", http.StatusBadRequest},
		{"port_valid", "8080", http.StatusOK},
		{"port_max", "65535", http.StatusOK},
		{"port_min", "1", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELBv2(t, h, url.Values{
				"Action":   {"CreateTargetGroup"},
				"Version":  {"2015-12-01"},
				"Name":     {"tg-port-" + tt.name},
				"Protocol": {"HTTP"},
				"Port":     {tt.port},
				"VpcId":    {"vpc-00000000"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "port=%s", tt.port)
		})
	}
}

// TestDeleteTargetGroupInUse tests that deleting a TG referenced by a listener fails.
func TestDeleteTargetGroupInUse(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "tg-inuse-lb")
	tgArn := mustCreateTG(t, h, "tg-inuse-tg")
	_ = mustCreateListener(t, h, lbArn, tgArn)

	// Attempting to delete the TG should fail because the listener references it.
	rec := doELBv2(t, h, url.Values{
		"Action":         {"DeleteTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Error struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceInUse", errResp.Error.Code)
}

// TestDeleteTargetGroupNotInUse tests that deleting an unreferenced TG succeeds.
func TestDeleteTargetGroupNotInUse(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "tg-notinuse")

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DeleteTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCreateTargetGroupInvalidTargetType verifies that invalid TargetType is rejected.
func TestCreateTargetGroupInvalidTargetType(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELBv2(t, h, url.Values{
		"Action":     {"CreateTargetGroup"},
		"Version":    {"2015-12-01"},
		"Name":       {"bad-type-tg"},
		"Protocol":   {"HTTP"},
		"Port":       {"80"},
		"VpcId":      {"vpc-00000000"},
		"TargetType": {"bogus"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCreateTargetGroupLambdaNoPort verifies that lambda target groups do not require Port.
func TestCreateTargetGroupLambdaNoPort(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELBv2(t, h, url.Values{
		"Action":     {"CreateTargetGroup"},
		"Version":    {"2015-12-01"},
		"Name":       {"lambda-no-port-tg"},
		"TargetType": {"lambda"},
		"VpcId":      {"vpc-00000000"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDescribeTargetGroupsByNameNotFound verifies that querying non-existent TG names
// returns 400 (TargetGroupNotFound), matching real AWS which raises
// TargetGroupNotFoundException for any unknown name.
func TestDescribeTargetGroupsByNameNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals   url.Values
		name   string
		expect int
	}{
		{
			name: "single_missing_name",
			vals: url.Values{
				"Action":         {"DescribeTargetGroups"},
				"Version":        {"2015-12-01"},
				"Names.member.1": {"does-not-exist"},
			},
			expect: http.StatusBadRequest,
		},
		{
			name: "one_valid_one_missing_name",
			vals: url.Values{
				"Action":         {"DescribeTargetGroups"},
				"Version":        {"2015-12-01"},
				"Names.member.1": {"desc-tg-name-exists"},
				"Names.member.2": {"does-not-exist"},
			},
			expect: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tc.name == "one_valid_one_missing_name" {
				mustCreateTG(t, h, "desc-tg-name-exists")
			}

			rec := doELBv2(t, h, tc.vals)
			assert.Equal(t, tc.expect, rec.Code)
		})
	}
}

func TestCreateTG_NameTooLong(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":   {"CreateTargetGroup"},
		"Version":  {"2015-12-01"},
		"Name":     {"this-name-is-definitely-too-long-yes"},
		"Protocol": {"HTTP"},
		"Port":     {"80"},
		"VpcId":    {"vpc-00000000"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateTG_InvalidTargetType(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":     {"CreateTargetGroup"},
		"Version":    {"2015-12-01"},
		"Name":       {"bad-type-tg"},
		"Protocol":   {"HTTP"},
		"Port":       {"80"},
		"VpcId":      {"vpc-00000000"},
		"TargetType": {"invalid"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDeleteTG_InUseRejected(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "tg-inuse-lb")
	tgArn := b1CreateTG(t, h, "tg-inuse")
	b1CreateListener(t, h, lbArn, tgArn) // forward to tgArn

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DeleteTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeTargetGroups_UnknownArnReturnsNotFound verifies that querying
// a non-existent target group ARN returns TargetGroupNotFound (HTTP 400, AWS query-protocol status), matching AWS.
func TestDescribeTargetGroups_UnknownArnReturnsNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arns []string
	}{
		{
			name: "single_unknown_arn",
			arns: []string{
				"arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/no-such-tg/0000000000000000",
			},
		},
		{
			name: "mix_of_known_and_unknown",
			arns: nil, // set in test body after creating a real TG
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newBatch2Handler()

			arns := tc.arns
			if tc.name == "mix_of_known_and_unknown" {
				realArn := mustCreateTG(t, h, "real-tg-"+tc.name)
				arns = []string{
					realArn,
					"arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/ghost/0000000000000000",
				}
			}

			vals := url.Values{
				"Action":  {"DescribeTargetGroups"},
				"Version": {"2015-12-01"},
			}
			for i, a := range arns {
				vals.Set("TargetGroupArns.member."+itoa(i+1), a)
			}

			rec := doELBv2(t, h, vals)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

// TestDescribeTargetGroups_AllKnownArnsSucceeds verifies that querying
// target groups by ARN continues to work when all ARNs exist.
func TestDescribeTargetGroups_AllKnownArnsSucceeds(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	arn1 := mustCreateTG(t, h, "known-tg-1")
	arn2 := mustCreateTG(t, h, "known-tg-2")

	rec := doELBv2(t, h, url.Values{
		"Action":                   {"DescribeTargetGroups"},
		"Version":                  {"2015-12-01"},
		"TargetGroupArns.member.1": {arn1},
		"TargetGroupArns.member.2": {arn2},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.TargetGroups.Members, 2)
}

// TestDeleteTargetGroup_ErrorCode verifies that deleting a TG still
// attached to a listener returns the AWS-accurate error code "ResourceInUse".
func TestDeleteTargetGroup_ErrorCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantErrCode  string
		wantCode     int
		detachBefore bool
	}{
		{
			name:         "in_use_returns_ResourceInUse",
			detachBefore: false,
			wantCode:     http.StatusBadRequest,
			wantErrCode:  "ResourceInUse",
		},
		{
			name:         "after_detach_returns_ok",
			detachBefore: true,
			wantCode:     http.StatusOK,
			wantErrCode:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityBHandler()
			lbArn := pbCreateLB(t, h, "dtg-lb")
			tgArn := pbCreateTG(t, h, "dtg-tg")
			lArn := pbCreateListener(t, h, lbArn, tgArn)

			if tc.detachBefore {
				tgArn2 := pbCreateTG(t, h, "dtg-tg2")
				rec := doELBv2(t, h, url.Values{
					"Action":                                 {"ModifyListener"},
					"Version":                                {"2015-12-01"},
					"ListenerArn":                            {lArn},
					"DefaultActions.member.1.Type":           {"forward"},
					"DefaultActions.member.1.TargetGroupArn": {tgArn2},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			}

			rec := doELBv2(t, h, url.Values{
				"Action":         {"DeleteTargetGroup"},
				"Version":        {"2015-12-01"},
				"TargetGroupArn": {tgArn},
			})
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantErrCode != "" {
				var errResp struct {
					Error struct {
						Code string `xml:"Code"`
					} `xml:"Error"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tc.wantErrCode, errResp.Error.Code)
			}
		})
	}
}
