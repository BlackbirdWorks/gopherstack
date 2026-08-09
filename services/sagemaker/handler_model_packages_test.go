package sagemaker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestHandler_CreateModelPackage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{
		"ModelPackageName": "my-pkg",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ModelPackageArn"], "my-pkg")
}

func TestHandler_DescribeModelPackage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{"ModelPackageName": "pkg-1"})

	rec := doSageMakerRequest(t, h, "DescribeModelPackage", map[string]any{"ModelPackageName": "pkg-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "pkg-1", resp["ModelPackageName"])
}

func TestHandler_DeleteModelPackage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{"ModelPackageName": "pkg-del"})
	rec := doSageMakerRequest(t, h, "DeleteModelPackage", map[string]any{"ModelPackageName": "pkg-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeModelPackage", map[string]any{"ModelPackageName": "pkg-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListModelPackages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{"ModelPackageName": "pkg-a"})
	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{"ModelPackageName": "pkg-b"})

	rec := doSageMakerRequest(t, h, "ListModelPackages", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["ModelPackageSummaryList"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// ModelPackageGroup
// ---------------------------------------------------------------------------

func TestHandler_CreateModelPackageGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "my-group",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ModelPackageGroupArn"], "my-group")
}

func TestHandler_DescribeModelPackageGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-1"})

	rec := doSageMakerRequest(t, h, "DescribeModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "grp-1", resp["ModelPackageGroupName"])
	assert.Equal(t, "Completed", resp["ModelPackageGroupStatus"])
}

func TestHandler_DeleteModelPackageGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-del"})
	rec := doSageMakerRequest(t, h, "DeleteModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListModelPackageGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-a"})
	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-b"})

	rec := doSageMakerRequest(t, h, "ListModelPackageGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["ModelPackageGroupSummaryList"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// AutoMLJob
// ---------------------------------------------------------------------------

func TestDeleteModelPackageGroup_WithPackages_Conflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "grp-with-pkgs",
	})
	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{
		"ModelPackageName":      "pkg-in-group",
		"ModelPackageGroupName": "grp-with-pkgs",
	})

	rec := doSageMakerRequest(t, h, "DeleteModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "grp-with-pkgs",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDeleteModelPackageGroup_WithPackages_RealClient asserts the wire error
// type is ConflictException -- DeleteModelPackageGroup's only documented
// error (botocore sagemaker/2017-07-24@1.43.56 service-2.json) -- not the
// generic ResourceInUse gopherstack-kbxx found this mapped to.
func TestDeleteModelPackageGroup_WithPackages_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateModelPackageGroup(t.Context(), &sagemakersdk.CreateModelPackageGroupInput{
		ModelPackageGroupName: aws.String("grp-with-pkgs-real"),
	})
	require.NoError(t, err)

	_, err = client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
		ModelPackageName:      aws.String("pkg-in-group-real"),
		ModelPackageGroupName: aws.String("grp-with-pkgs-real"),
	})
	require.NoError(t, err)

	_, err = client.DeleteModelPackageGroup(t.Context(), &sagemakersdk.DeleteModelPackageGroupInput{
		ModelPackageGroupName: aws.String("grp-with-pkgs-real"),
	})
	require.Error(t, err)

	var conflict *smtypes.ConflictException
	require.ErrorAs(t, err, &conflict)
}

func TestDeleteModelPackageGroup_AfterPackagesRemoved_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "grp-empty",
	})
	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{
		"ModelPackageName":      "pkg-to-remove",
		"ModelPackageGroupName": "grp-empty",
	})
	doSageMakerRequest(t, h, "DeleteModelPackage", map[string]any{
		"ModelPackageName": "pkg-to-remove",
	})

	rec := doSageMakerRequest(t, h, "DeleteModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "grp-empty",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// AutoMLJob: initial status and terminal-state stop guard
// ---------------------------------------------------------------------------

func TestAddModelPackageInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pkgName   string
		wantCount int
	}{
		{
			name:      "creates model package",
			pkgName:   "my-pkg",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			arnStr := "arn:aws:sagemaker:us-east-1:000000000000:model-package/" + tt.pkgName
			b.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
				ModelPackageName:   tt.pkgName,
				ModelPackageArn:    arnStr,
				ModelPackageStatus: "Approved",
			})

			assert.Equal(t, tt.wantCount, sagemaker.ModelPackageCount(b))
		})
	}
}

func TestAddTags_ModelPackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "tags on model package via ARN",
			wantCode: http.StatusOK,
			wantTags: map[string]string{"env": "prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arnStr := "arn:aws:sagemaker:us-east-1:000000000000:model-package/my-pkg"
			h.Backend.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
				ModelPackageName:   "my-pkg",
				ModelPackageArn:    arnStr,
				ModelPackageStatus: "Approved",
				Tags:               make(map[string]string),
			})

			rec := doSageMakerRequest(t, h, "AddTags", map[string]any{
				"ResourceArn": arnStr,
				"Tags":        []map[string]string{{"Key": "env", "Value": "prod"}},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			rec2 := doSageMakerRequest(t, h, "ListTags", map[string]any{
				"ResourceArn": arnStr,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

			tags := resp["Tags"].([]any)
			require.Len(t, tags, 1)

			tag := tags[0].(map[string]any)
			assert.Equal(t, "env", tag["Key"])
			assert.Equal(t, "prod", tag["Value"])
		})
	}
}

func TestBatchDescribeModelPackage_MixedResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedArns  []string
		queryArns []string
		wantFound int
		wantErr   int
	}{
		{
			name:     "some found some not",
			seedArns: []string{"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-1"},
			queryArns: []string{
				"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-1",
				"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-missing",
			},
			wantFound: 1,
			wantErr:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for _, arnStr := range tt.seedArns {
				b.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
					ModelPackageName:   "pkg-1",
					ModelPackageArn:    arnStr,
					ModelPackageStatus: "Approved",
				})
			}

			results := b.BatchDescribeModelPackage(context.Background(), tt.queryArns)
			found, errs := 0, 0

			for _, r := range results {
				if r.ModelPackage != nil {
					found++
				} else {
					errs++
				}
			}

			assert.Equal(t, tt.wantFound, found)
			assert.Equal(t, tt.wantErr, errs)
		})
	}
}

func TestHandler_ModelPackageGroupPolicy_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})

	// No policy attached yet.
	recGet := doSageMakerRequest(t, h, "GetModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})
	assert.Equal(t, http.StatusBadRequest, recGet.Code)

	policy := `{"Version":"2012-10-17","Statement":[]}`
	recPut := doSageMakerRequest(t, h, "PutModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
		"ResourcePolicy":        policy,
	})
	require.Equal(t, http.StatusOK, recPut.Code)

	var putOut map[string]any
	require.NoError(t, json.Unmarshal(recPut.Body.Bytes(), &putOut))
	assert.NotEmpty(t, putOut["ModelPackageGroupArn"])

	recGet2 := doSageMakerRequest(t, h, "GetModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})
	require.Equal(t, http.StatusOK, recGet2.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(recGet2.Body.Bytes(), &getOut))
	assert.Equal(t, policy, getOut["ResourcePolicy"])

	recDelete := doSageMakerRequest(t, h, "DeleteModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})
	require.Equal(t, http.StatusOK, recDelete.Code)

	recGet3 := doSageMakerRequest(t, h, "GetModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})
	assert.Equal(t, http.StatusBadRequest, recGet3.Code)
}

func TestHandler_ModelPackageGroupPolicy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		body map[string]any
		op   string
	}{
		{op: "GetModelPackageGroupPolicy", body: map[string]any{"ModelPackageGroupName": "no-such-group"}},
		{
			op: "PutModelPackageGroupPolicy",
			body: map[string]any{
				"ModelPackageGroupName": "no-such-group", "ResourcePolicy": "{}",
			},
		},
		{op: "DeleteModelPackageGroupPolicy", body: map[string]any{"ModelPackageGroupName": "no-such-group"}},
	}

	for _, tc := range tests {
		t.Run(tc.op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, tc.op, tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Service Catalog portfolio toggle
// ---------------------------------------------------------------------------

func TestHandler_UpdateModelPackage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{
		"ModelPackageName": "my-pkg",
	})

	rec := doSageMakerRequest(t, h, "UpdateModelPackage", map[string]any{
		"ModelPackageName":    "my-pkg",
		"ModelApprovalStatus": "Approved",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ModelPackageArn"], "my-pkg")

	// Describe and verify approval status
	rec = doSageMakerRequest(t, h, "DescribeModelPackage", map[string]any{
		"ModelPackageName": "my-pkg",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "Approved", descResp["ModelApprovalStatus"])
}

func TestHandler_UpdateModelPackage_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateModelPackage", map[string]any{
		"ModelPackageName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// UpdateSpace tests
// ---------------------------------------------------------------------------

func TestHandler_BatchDescribeModelPackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *sagemaker.Handler)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success with existing packages",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()
				h.Backend.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
					ModelPackageName:   "my-pkg",
					ModelPackageArn:    "arn:aws:sagemaker:us-east-1:000000000000:model-package/my-pkg",
					ModelPackageStatus: "Completed",
					CreationTime:       time.Now(),
				})
			},
			body: map[string]any{
				"ModelPackageArnList": []string{
					"arn:aws:sagemaker:us-east-1:000000000000:model-package/my-pkg",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "package not found goes to errors map",
			body: map[string]any{
				"ModelPackageArnList": []string{
					"arn:aws:sagemaker:us-east-1:000000000000:model-package/nonexistent",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "empty list",
			body: map[string]any{
				"ModelPackageArnList": []string{},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "BatchDescribeModelPackage", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "ModelPackageSummaries")
				assert.Contains(t, resp, "BatchDescribeModelPackageErrorMap")
			}
		})
	}
}

func TestHandler_BatchDescribeModelPackage_ExistingAndMissing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	h.Backend.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
		ModelPackageName:   "pkg-a",
		ModelPackageArn:    "arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-a",
		ModelPackageStatus: "Completed",
		CreationTime:       time.Now(),
	})

	body := map[string]any{
		"ModelPackageArnList": []string{
			"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-a",
			"arn:aws:sagemaker:us-east-1:000000000000:model-package/missing",
		},
	}

	rec := doSageMakerRequest(t, h, "BatchDescribeModelPackage", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["ModelPackageSummaries"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, summaries, "arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-a")

	errors, ok := resp["BatchDescribeModelPackageErrorMap"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, errors, "arn:aws:sagemaker:us-east-1:000000000000:model-package/missing")
}
