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

// TestHandler_UpdateModelPackage_RealClient asserts UpdateModelPackage works
// through the real aws-sdk-go-v2 client. The previous handler decoded
// "ModelPackageName" from the request body, but UpdateModelPackageInput's
// sole identifier on the real wire is ModelPackageArn -- no genuine SDK
// client serializes a ModelPackageName field for this op at all, so every
// real UpdateModelPackage call against this backend failed outright before
// this fix.
func TestHandler_UpdateModelPackage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	created, err := client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
		ModelPackageName: aws.String("my-pkg"),
	})
	require.NoError(t, err)

	_, err = client.UpdateModelPackage(t.Context(), &sagemakersdk.UpdateModelPackageInput{
		ModelPackageArn:     created.ModelPackageArn,
		ModelApprovalStatus: smtypes.ModelApprovalStatusApproved,
	})
	require.NoError(t, err)

	desc, err := client.DescribeModelPackage(t.Context(), &sagemakersdk.DescribeModelPackageInput{
		ModelPackageName: created.ModelPackageArn,
	})
	require.NoError(t, err)
	assert.Equal(t, smtypes.ModelApprovalStatusApproved, desc.ModelApprovalStatus)
}

func TestHandler_UpdateModelPackage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{
		"ModelPackageName": "my-pkg",
	})

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	pkgArn := createResp["ModelPackageArn"]
	require.NotEmpty(t, pkgArn)

	rec := doSageMakerRequest(t, h, "UpdateModelPackage", map[string]any{
		"ModelPackageArn":     pkgArn,
		"ModelApprovalStatus": "Approved",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, pkgArn, resp["ModelPackageArn"])

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
		"ModelPackageArn": "arn:aws:sagemaker:us-east-1:000000000000:model-package/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateModelPackage_MissingArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateModelPackage", map[string]any{
		"ModelApprovalStatus": "Approved",
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

// ---------------------------------------------------------------------------
// gopherstack-oc9v: model_packages inline-struct conversion round-trip tests
// ---------------------------------------------------------------------------

// TestHandler_CreateModelPackage_FullFields_RealClient asserts that
// CreateModelPackageInput fields absent before this pass (ModelApprovalStatus,
// CustomerMetadataProperties, InferenceSpecification, and friends) are
// accepted and echoed back by DescribeModelPackage, not silently dropped.
func TestHandler_CreateModelPackage_FullFields_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
		ModelPackageName:      aws.String("pkg-full"),
		ModelApprovalStatus:   smtypes.ModelApprovalStatusPendingManualApproval,
		Domain:                aws.String("COMPUTER_VISION"),
		Task:                  aws.String("IMAGE_CLASSIFICATION"),
		SamplePayloadUrl:      aws.String("s3://bucket/sample.tar.gz"),
		SourceUri:             aws.String("s3://bucket/model"),
		SkipModelValidation:   smtypes.SkipModelValidationNone,
		CertifyForMarketplace: aws.Bool(true),
		CustomerMetadataProperties: map[string]string{
			"team": "ml-platform",
		},
		InferenceSpecification: &smtypes.InferenceSpecification{
			Containers: []smtypes.ModelPackageContainerDefinition{
				{Image: aws.String("123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest")},
			},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeModelPackage(t.Context(), &sagemakersdk.DescribeModelPackageInput{
		ModelPackageName: aws.String("pkg-full"),
	})
	require.NoError(t, err)

	assert.Equal(t, smtypes.ModelApprovalStatusPendingManualApproval, desc.ModelApprovalStatus)
	assert.Equal(t, "COMPUTER_VISION", aws.ToString(desc.Domain))
	assert.Equal(t, "IMAGE_CLASSIFICATION", aws.ToString(desc.Task))
	assert.Equal(t, "s3://bucket/sample.tar.gz", aws.ToString(desc.SamplePayloadUrl))
	assert.Equal(t, "s3://bucket/model", aws.ToString(desc.SourceUri))
	assert.Equal(t, smtypes.SkipModelValidationNone, desc.SkipModelValidation)
	assert.True(t, aws.ToBool(desc.CertifyForMarketplace))
	assert.Equal(t, "ml-platform", desc.CustomerMetadataProperties["team"])
	require.NotNil(t, desc.InferenceSpecification)
	require.Len(t, desc.InferenceSpecification.Containers, 1)
	assert.Equal(t,
		"123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
		aws.ToString(desc.InferenceSpecification.Containers[0].Image),
	)
}

// TestHandler_ListModelPackages_FilterSortPage_RealClient asserts
// ListModelPackagesInput's filter/sort/pagination fields -- absent before
// this pass except ModelPackageGroupName/NextToken -- actually narrow,
// reorder, and paginate the result set rather than being silently ignored.
func TestHandler_ListModelPackages_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	names := []string{"alpha-model", "beta-model", "gamma-widget"}
	for _, n := range names {
		_, err := client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
			ModelPackageName: aws.String(n),
		})
		require.NoError(t, err)
	}

	t.Run("name contains filters", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListModelPackages(t.Context(), &sagemakersdk.ListModelPackagesInput{
			NameContains: aws.String("model"),
		})
		require.NoError(t, err)
		assert.Len(t, out.ModelPackageSummaryList, 2)
	})

	t.Run("sort by name ascending", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListModelPackages(t.Context(), &sagemakersdk.ListModelPackagesInput{
			SortBy:    smtypes.ModelPackageSortByName,
			SortOrder: smtypes.SortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.ModelPackageSummaryList, 3)
		assert.Equal(t, "alpha-model", aws.ToString(out.ModelPackageSummaryList[0].ModelPackageName))
		assert.Equal(t, "beta-model", aws.ToString(out.ModelPackageSummaryList[1].ModelPackageName))
		assert.Equal(t, "gamma-widget", aws.ToString(out.ModelPackageSummaryList[2].ModelPackageName))
	})

	t.Run("max results caps the page and returns a token", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListModelPackages(t.Context(), &sagemakersdk.ListModelPackagesInput{
			MaxResults: aws.Int32(1),
			SortBy:     smtypes.ModelPackageSortByName,
			SortOrder:  smtypes.SortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.ModelPackageSummaryList, 1)
		assert.Equal(t, "alpha-model", aws.ToString(out.ModelPackageSummaryList[0].ModelPackageName))
		assert.NotEmpty(t, aws.ToString(out.NextToken))
	})
}

// TestHandler_ListModelPackages_ApprovalStatusFilter_RealClient asserts the
// ModelApprovalStatus list filter -- absent before this pass -- actually
// narrows the result set.
func TestHandler_ListModelPackages_ApprovalStatusFilter_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
		ModelPackageName:    aws.String("pkg-approved"),
		ModelApprovalStatus: smtypes.ModelApprovalStatusApproved,
	})
	require.NoError(t, err)

	_, err = client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
		ModelPackageName:    aws.String("pkg-pending"),
		ModelApprovalStatus: smtypes.ModelApprovalStatusPendingManualApproval,
	})
	require.NoError(t, err)

	out, err := client.ListModelPackages(t.Context(), &sagemakersdk.ListModelPackagesInput{
		ModelApprovalStatus: smtypes.ModelApprovalStatusApproved,
	})
	require.NoError(t, err)
	require.Len(t, out.ModelPackageSummaryList, 1)
	assert.Equal(t, "pkg-approved", aws.ToString(out.ModelPackageSummaryList[0].ModelPackageName))
}

// TestHandler_ListModelPackages_ModelPackageType_RealClient asserts the
// ModelPackageType filter (Versioned/Unversioned/Both) -- absent before this
// pass -- distinguishes model packages created with a ModelPackageGroupName
// from those without, defaulting to Unversioned per the op's documented
// default when omitted. This backend interprets "versioned" as "has a group
// name", the only sense in which it distinguishes the two, since it does not
// implement AWS's group+version ARN addressing scheme (disclosed on
// ModelPackage).
func TestHandler_ListModelPackages_ModelPackageType_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateModelPackageGroup(t.Context(), &sagemakersdk.CreateModelPackageGroupInput{
		ModelPackageGroupName: aws.String("grp-type-test"),
	})
	require.NoError(t, err)

	_, err = client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
		ModelPackageName:      aws.String("pkg-versioned"),
		ModelPackageGroupName: aws.String("grp-type-test"),
	})
	require.NoError(t, err)

	_, err = client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
		ModelPackageName: aws.String("pkg-unversioned"),
	})
	require.NoError(t, err)

	t.Run("default is unversioned", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListModelPackages(t.Context(), &sagemakersdk.ListModelPackagesInput{})
		require.NoError(t, listErr)
		require.Len(t, out.ModelPackageSummaryList, 1)
		assert.Equal(t, "pkg-unversioned", aws.ToString(out.ModelPackageSummaryList[0].ModelPackageName))
	})

	t.Run("versioned returns only grouped packages", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListModelPackages(t.Context(), &sagemakersdk.ListModelPackagesInput{
			ModelPackageType: smtypes.ModelPackageTypeVersioned,
		})
		require.NoError(t, listErr)
		require.Len(t, out.ModelPackageSummaryList, 1)
		assert.Equal(t, "pkg-versioned", aws.ToString(out.ModelPackageSummaryList[0].ModelPackageName))
	})

	t.Run("both returns everything", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListModelPackages(t.Context(), &sagemakersdk.ListModelPackagesInput{
			ModelPackageType: smtypes.ModelPackageTypeBoth,
		})
		require.NoError(t, listErr)
		assert.Len(t, out.ModelPackageSummaryList, 2)
	})
}

// TestHandler_ListModelPackageGroups_FilterSortPage_RealClient asserts
// ListModelPackageGroupsInput's filter/sort/pagination fields -- absent
// before this pass except NextToken -- actually narrow, reorder, and
// paginate the result set.
func TestHandler_ListModelPackageGroups_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	names := []string{"alpha-group", "beta-group", "gamma-team"}
	for _, n := range names {
		_, err := client.CreateModelPackageGroup(t.Context(), &sagemakersdk.CreateModelPackageGroupInput{
			ModelPackageGroupName: aws.String(n),
		})
		require.NoError(t, err)
	}

	t.Run("name contains filters", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListModelPackageGroups(t.Context(), &sagemakersdk.ListModelPackageGroupsInput{
			NameContains: aws.String("group"),
		})
		require.NoError(t, err)
		assert.Len(t, out.ModelPackageGroupSummaryList, 2)
	})

	t.Run("sort by name descending", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListModelPackageGroups(t.Context(), &sagemakersdk.ListModelPackageGroupsInput{
			SortBy:    smtypes.ModelPackageGroupSortByName,
			SortOrder: smtypes.SortOrderDescending,
		})
		require.NoError(t, err)
		require.Len(t, out.ModelPackageGroupSummaryList, 3)
		assert.Equal(t, "gamma-team", aws.ToString(out.ModelPackageGroupSummaryList[0].ModelPackageGroupName))
		assert.Equal(t, "beta-group", aws.ToString(out.ModelPackageGroupSummaryList[1].ModelPackageGroupName))
		assert.Equal(t, "alpha-group", aws.ToString(out.ModelPackageGroupSummaryList[2].ModelPackageGroupName))
	})

	t.Run("cross account filter yields no local groups", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListModelPackageGroups(t.Context(), &sagemakersdk.ListModelPackageGroupsInput{
			CrossAccountFilterOption: smtypes.CrossAccountFilterOptionCrossAccount,
		})
		require.NoError(t, err)
		assert.Empty(t, out.ModelPackageGroupSummaryList)
	})
}

// TestHandler_CreateModelPackageGroup_ManagedConfiguration_RealClient asserts
// CreateModelPackageGroupInput.ManagedConfiguration -- absent before this
// pass -- round-trips through DescribeModelPackageGroup.
func TestHandler_CreateModelPackageGroup_ManagedConfiguration_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateModelPackageGroup(t.Context(), &sagemakersdk.CreateModelPackageGroupInput{
		ModelPackageGroupName: aws.String("grp-managed"),
		ManagedConfiguration: &smtypes.ManagedConfiguration{
			ManagedStorageType: smtypes.ManagedStorageTypeRestricted,
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeModelPackageGroup(t.Context(), &sagemakersdk.DescribeModelPackageGroupInput{
		ModelPackageGroupName: aws.String("grp-managed"),
	})
	require.NoError(t, err)
	require.NotNil(t, desc.ManagedConfiguration)
	assert.Equal(t, smtypes.ManagedStorageTypeRestricted, desc.ManagedConfiguration.ManagedStorageType)
}

// TestHandler_UpdateModelPackage_FullFields_RealClient asserts
// UpdateModelPackageInput's fields beyond ModelApprovalStatus -- all absent
// before this pass -- are accepted and round-trip through
// DescribeModelPackage: ApprovalDescription, CustomerMetadataProperties
// (merged, not replaced), ModelCard, and ModelLifeCycle.
func TestHandler_UpdateModelPackage_FullFields_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	created, err := client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
		ModelPackageName: aws.String("pkg-update-full"),
		CustomerMetadataProperties: map[string]string{
			"owner": "team-a",
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateModelPackage(t.Context(), &sagemakersdk.UpdateModelPackageInput{
		ModelPackageArn:     created.ModelPackageArn,
		ApprovalDescription: aws.String("looks good"),
		CustomerMetadataProperties: map[string]string{
			"reviewer": "team-b",
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeModelPackage(t.Context(), &sagemakersdk.DescribeModelPackageInput{
		ModelPackageName: created.ModelPackageArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "looks good", aws.ToString(desc.ApprovalDescription))
	assert.Equal(t, "team-a", desc.CustomerMetadataProperties["owner"])
	assert.Equal(t, "team-b", desc.CustomerMetadataProperties["reviewer"])
}

// TestHandler_UpdateModelPackage_CustomerMetadataPropertiesToRemove_RealClient
// asserts UpdateModelPackageInput.CustomerMetadataPropertiesToRemove -- a
// real field with no counterpart before this pass -- actually deletes the
// named keys rather than being silently ignored.
func TestHandler_UpdateModelPackage_CustomerMetadataPropertiesToRemove_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	created, err := client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
		ModelPackageName: aws.String("pkg-remove-metadata"),
		CustomerMetadataProperties: map[string]string{
			"keep":   "yes",
			"remove": "no",
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateModelPackage(t.Context(), &sagemakersdk.UpdateModelPackageInput{
		ModelPackageArn:                    created.ModelPackageArn,
		CustomerMetadataPropertiesToRemove: []string{"remove"},
	})
	require.NoError(t, err)

	desc, err := client.DescribeModelPackage(t.Context(), &sagemakersdk.DescribeModelPackageInput{
		ModelPackageName: created.ModelPackageArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "yes", desc.CustomerMetadataProperties["keep"])
	assert.NotContains(t, desc.CustomerMetadataProperties, "remove")
}

// TestHandler_UpdateModelPackage_AdditionalInferenceSpecificationsToAdd_RealClient
// asserts UpdateModelPackageInput.AdditionalInferenceSpecificationsToAdd --
// absent before this pass -- appends onto the existing array rather than
// replacing it or being dropped, per the real op's doc: "to be added to the
// existing array".
func TestHandler_UpdateModelPackage_AdditionalInferenceSpecificationsToAdd_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	firstSpec := smtypes.AdditionalInferenceSpecificationDefinition{
		Name: aws.String("first"),
		Containers: []smtypes.ModelPackageContainerDefinition{
			{Image: aws.String("123456789012.dkr.ecr.us-east-1.amazonaws.com/first:latest")},
		},
	}

	created, err := client.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
		ModelPackageName: aws.String("pkg-additional-spec"),
		AdditionalInferenceSpecifications: []smtypes.AdditionalInferenceSpecificationDefinition{
			firstSpec,
		},
	})
	require.NoError(t, err)

	secondSpec := smtypes.AdditionalInferenceSpecificationDefinition{
		Name: aws.String("second"),
		Containers: []smtypes.ModelPackageContainerDefinition{
			{Image: aws.String("123456789012.dkr.ecr.us-east-1.amazonaws.com/second:latest")},
		},
	}

	_, err = client.UpdateModelPackage(t.Context(), &sagemakersdk.UpdateModelPackageInput{
		ModelPackageArn: created.ModelPackageArn,
		AdditionalInferenceSpecificationsToAdd: []smtypes.AdditionalInferenceSpecificationDefinition{
			secondSpec,
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeModelPackage(t.Context(), &sagemakersdk.DescribeModelPackageInput{
		ModelPackageName: created.ModelPackageArn,
	})
	require.NoError(t, err)
	require.Len(t, desc.AdditionalInferenceSpecifications, 2)
	assert.Equal(t, "first", aws.ToString(desc.AdditionalInferenceSpecifications[0].Name))
	assert.Equal(t, "second", aws.ToString(desc.AdditionalInferenceSpecifications[1].Name))
}
