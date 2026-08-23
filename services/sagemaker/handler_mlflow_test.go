package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "my-server",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["TrackingServerArn"], "my-server")
}

func TestHandler_DescribeMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-1"})
	rec := doSageMakerRequest(t, h, "DescribeMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ts-1", resp["TrackingServerName"])
}

func TestHandler_StartStopMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-ss"})

	rec := doSageMakerRequest(t, h, "StartMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-ss"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	assert.Contains(t, startResp["TrackingServerArn"], "ts-ss")

	rec = doSageMakerRequest(t, h, "StopMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-ss"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var stopResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stopResp))
	assert.Contains(t, stopResp["TrackingServerArn"], "ts-ss")
}

// TestHandler_StartStopMlflowTrackingServer_IsActive_RealClient proves
// DescribeMlflowTrackingServer's IsActive — absent before this pass — tracks
// the real Start/Stop-driven TrackingServerStatus transitions.
func TestHandler_StartStopMlflowTrackingServer_IsActive_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateMlflowTrackingServer(t.Context(), &sagemakersdk.CreateMlflowTrackingServerInput{
		TrackingServerName: aws.String("ts-active"),
		ArtifactStoreUri:   aws.String("s3://bucket/mlflow"),
		RoleArn:            aws.String("arn:aws:iam::000000000000:role/access"),
	})
	require.NoError(t, err)

	created, err := client.DescribeMlflowTrackingServer(t.Context(), &sagemakersdk.DescribeMlflowTrackingServerInput{
		TrackingServerName: aws.String("ts-active"),
	})
	require.NoError(t, err)
	assert.Equal(t, smtypes.IsTrackingServerActiveInactive, created.IsActive)
	assert.Contains(t, aws.ToString(created.TrackingServerUrl), "ts-active")

	_, err = client.StartMlflowTrackingServer(t.Context(), &sagemakersdk.StartMlflowTrackingServerInput{
		TrackingServerName: aws.String("ts-active"),
	})
	require.NoError(t, err)

	started, err := client.DescribeMlflowTrackingServer(t.Context(), &sagemakersdk.DescribeMlflowTrackingServerInput{
		TrackingServerName: aws.String("ts-active"),
	})
	require.NoError(t, err)
	assert.Equal(t, smtypes.IsTrackingServerActiveActive, started.IsActive)

	_, err = client.StopMlflowTrackingServer(t.Context(), &sagemakersdk.StopMlflowTrackingServerInput{
		TrackingServerName: aws.String("ts-active"),
	})
	require.NoError(t, err)

	stopped, err := client.DescribeMlflowTrackingServer(t.Context(), &sagemakersdk.DescribeMlflowTrackingServerInput{
		TrackingServerName: aws.String("ts-active"),
	})
	require.NoError(t, err)
	assert.Equal(t, smtypes.IsTrackingServerActiveInactive, stopped.IsActive)
}

func TestHandler_DeleteMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-del"})
	rec := doSageMakerRequest(t, h, "DeleteMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var deleteResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &deleteResp))
	assert.Contains(t, deleteResp["TrackingServerArn"], "ts-del")

	rec = doSageMakerRequest(t, h, "DescribeMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_CreateMlflowTrackingServer_FullFields_RealClient proves the six
// previously-absent CreateMlflowTrackingServerInput fields
// (ArtifactStoreUri, AutomaticModelRegistration, S3BucketOwnerAccountId,
// S3BucketOwnerVerification, TrackingServerSize, WeeklyMaintenanceWindowStart)
// all round-trip through DescribeMlflowTrackingServer.
func TestHandler_CreateMlflowTrackingServer_FullFields_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateMlflowTrackingServer(t.Context(), &sagemakersdk.CreateMlflowTrackingServerInput{
		TrackingServerName:           aws.String("ts-full"),
		ArtifactStoreUri:             aws.String("s3://bucket/full"),
		RoleArn:                      aws.String("arn:aws:iam::000000000000:role/access"),
		AutomaticModelRegistration:   aws.Bool(true),
		S3BucketOwnerAccountId:       aws.String("111122223333"),
		S3BucketOwnerVerification:    aws.Bool(false),
		TrackingServerSize:           smtypes.TrackingServerSizeL,
		WeeklyMaintenanceWindowStart: aws.String("TUE:03:30"),
	})
	require.NoError(t, err)

	out, err := client.DescribeMlflowTrackingServer(t.Context(), &sagemakersdk.DescribeMlflowTrackingServerInput{
		TrackingServerName: aws.String("ts-full"),
	})
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/full", aws.ToString(out.ArtifactStoreUri))
	assert.True(t, aws.ToBool(out.AutomaticModelRegistration))
	assert.Equal(t, "111122223333", aws.ToString(out.S3BucketOwnerAccountId))
	assert.False(t, aws.ToBool(out.S3BucketOwnerVerification))
	assert.Equal(t, smtypes.TrackingServerSizeL, out.TrackingServerSize)
	assert.Equal(t, "TUE:03:30", aws.ToString(out.WeeklyMaintenanceWindowStart))
}

// TestHandler_CreateMlflowTrackingServer_Defaults_RealClient proves the
// documented defaults (api_op_CreateMlflowTrackingServer.go:55-58,71-73,79-87)
// apply when the three optional fields they cover are omitted.
func TestHandler_CreateMlflowTrackingServer_Defaults_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateMlflowTrackingServer(t.Context(), &sagemakersdk.CreateMlflowTrackingServerInput{
		TrackingServerName: aws.String("ts-defaults"),
		ArtifactStoreUri:   aws.String("s3://bucket/defaults"),
		RoleArn:            aws.String("arn:aws:iam::000000000000:role/access"),
	})
	require.NoError(t, err)

	out, err := client.DescribeMlflowTrackingServer(t.Context(), &sagemakersdk.DescribeMlflowTrackingServerInput{
		TrackingServerName: aws.String("ts-defaults"),
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(out.AutomaticModelRegistration))
	assert.True(t, aws.ToBool(out.S3BucketOwnerVerification))
	assert.Equal(t, smtypes.TrackingServerSizeS, out.TrackingServerSize)
}

// TestHandler_UpdateMlflowTrackingServer_FullFields_RealClient proves
// UpdateMlflowTrackingServer's ArtifactStoreUri/S3BucketOwnerAccountId/
// TrackingServerSize/WeeklyMaintenanceWindowStart — all previously absent —
// apply, and that omitting AutomaticModelRegistration/S3BucketOwnerVerification
// on a later update leaves their prior explicit values unchanged (disclosed
// leave-unchanged-if-omitted semantics; see UpdateMlflowTrackingServerOptions).
func TestHandler_UpdateMlflowTrackingServer_FullFields_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateMlflowTrackingServer(t.Context(), &sagemakersdk.CreateMlflowTrackingServerInput{
		TrackingServerName:         aws.String("ts-update"),
		ArtifactStoreUri:           aws.String("s3://bucket/original"),
		RoleArn:                    aws.String("arn:aws:iam::000000000000:role/access"),
		AutomaticModelRegistration: aws.Bool(true),
		S3BucketOwnerVerification:  aws.Bool(false),
	})
	require.NoError(t, err)

	_, err = client.UpdateMlflowTrackingServer(t.Context(), &sagemakersdk.UpdateMlflowTrackingServerInput{
		TrackingServerName:           aws.String("ts-update"),
		ArtifactStoreUri:             aws.String("s3://bucket/updated"),
		S3BucketOwnerAccountId:       aws.String("444455556666"),
		TrackingServerSize:           smtypes.TrackingServerSizeM,
		WeeklyMaintenanceWindowStart: aws.String("WED:04:00"),
	})
	require.NoError(t, err)

	out, err := client.DescribeMlflowTrackingServer(t.Context(), &sagemakersdk.DescribeMlflowTrackingServerInput{
		TrackingServerName: aws.String("ts-update"),
	})
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/updated", aws.ToString(out.ArtifactStoreUri))
	assert.Equal(t, "444455556666", aws.ToString(out.S3BucketOwnerAccountId))
	assert.Equal(t, smtypes.TrackingServerSizeM, out.TrackingServerSize)
	assert.Equal(t, "WED:04:00", aws.ToString(out.WeeklyMaintenanceWindowStart))
	assert.True(t, aws.ToBool(out.AutomaticModelRegistration), "omitted bool must leave prior explicit value unchanged")
	assert.False(t, aws.ToBool(out.S3BucketOwnerVerification), "omitted bool must leave prior explicit value unchanged")
}

// TestHandler_ListMlflowTrackingServers_FilterSortPage_RealClient proves the
// seven previously-absent ListMlflowTrackingServersInput fields (CreatedAfter,
// CreatedBefore, MaxResults, MlflowVersion, SortBy, SortOrder,
// TrackingServerStatus) filter, sort and page the real result set.
func TestHandler_ListMlflowTrackingServers_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, tc := range []struct{ name, version string }{
		{"ts-old", "2.0.0"},
		{"ts-new", "2.1.0"},
	} {
		_, err := client.CreateMlflowTrackingServer(t.Context(), &sagemakersdk.CreateMlflowTrackingServerInput{
			TrackingServerName: aws.String(tc.name),
			ArtifactStoreUri:   aws.String("s3://bucket/" + tc.name),
			RoleArn:            aws.String("arn:aws:iam::000000000000:role/access"),
			MlflowVersion:      aws.String(tc.version),
		})
		require.NoError(t, err)
	}

	future := time.Now().Add(365 * 24 * time.Hour)
	past := time.Now().Add(-365 * 24 * time.Hour)

	windowed, err := client.ListMlflowTrackingServers(t.Context(), &sagemakersdk.ListMlflowTrackingServersInput{
		CreatedAfter:  &past,
		CreatedBefore: &future,
		SortBy:        smtypes.SortTrackingServerByCreationTime,
		SortOrder:     smtypes.SortOrderAscending,
	})
	require.NoError(t, err)
	require.Len(t, windowed.TrackingServerSummaries, 2)
	assert.Equal(t, "ts-old", aws.ToString(windowed.TrackingServerSummaries[0].TrackingServerName))
	assert.Equal(t, "ts-new", aws.ToString(windowed.TrackingServerSummaries[1].TrackingServerName))
	assert.Equal(t, smtypes.IsTrackingServerActiveInactive, windowed.TrackingServerSummaries[0].IsActive)

	excluded, err := client.ListMlflowTrackingServers(t.Context(), &sagemakersdk.ListMlflowTrackingServersInput{
		CreatedAfter: &future,
	})
	require.NoError(t, err)
	assert.Empty(t, excluded.TrackingServerSummaries)

	byVersion, err := client.ListMlflowTrackingServers(t.Context(), &sagemakersdk.ListMlflowTrackingServersInput{
		MlflowVersion: aws.String("2.1.0"),
	})
	require.NoError(t, err)
	require.Len(t, byVersion.TrackingServerSummaries, 1)
	assert.Equal(t, "ts-new", aws.ToString(byVersion.TrackingServerSummaries[0].TrackingServerName))

	defaultOrder, err := client.ListMlflowTrackingServers(t.Context(), &sagemakersdk.ListMlflowTrackingServersInput{})
	require.NoError(t, err)
	require.Len(t, defaultOrder.TrackingServerSummaries, 2)
	assert.Equal(t, "ts-new", aws.ToString(defaultOrder.TrackingServerSummaries[0].TrackingServerName),
		"default sort is Descending by CreationTime per api_op_ListMlflowTrackingServers.go:20-22")

	page1, err := client.ListMlflowTrackingServers(t.Context(), &sagemakersdk.ListMlflowTrackingServersInput{
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.TrackingServerSummaries, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))
}

// ---------------------------------------------------------------------------
// ModelCard
// ---------------------------------------------------------------------------

func TestHandler_CreatePresignedMlflowTrackingServerUrl(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "ts-1",
	})

	rec := doSageMakerRequest(t, h, "CreatePresignedMlflowTrackingServerUrl", map[string]any{
		"TrackingServerName": "ts-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["AuthorizedUrl"], "ts-1")
}

func TestHandler_CreatePresignedMlflowTrackingServerUrl_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePresignedMlflowTrackingServerUrl", map[string]any{
		"TrackingServerName": "missing",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// MlflowApp
// ---------------------------------------------------------------------------

func TestHandler_MlflowApp_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreateMlflowApp", map[string]any{
		"Name":             "app-1",
		"ArtifactStoreUri": "s3://bucket/path",
		"RoleArn":          "arn:aws:iam::000000000000:role/mlflow",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	appARN := createResp["Arn"]
	require.Contains(t, appARN, "mlflow-app/app-1")

	describeRec := doSageMakerRequest(t, h, "DescribeMlflowApp", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, describeRec.Code)

	var describeResp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "app-1", describeResp["Name"])
	assert.Equal(t, "Created", describeResp["Status"])
	assert.Equal(t, "s3://bucket/path", describeResp["ArtifactStoreUri"])

	updateRec := doSageMakerRequest(t, h, "UpdateMlflowApp", map[string]any{
		"Arn":              appARN,
		"ArtifactStoreUri": "s3://bucket/new-path",
	})
	assert.Equal(t, http.StatusOK, updateRec.Code)

	describeRec = doSageMakerRequest(t, h, "DescribeMlflowApp", map[string]any{"Arn": appARN})
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "s3://bucket/new-path", describeResp["ArtifactStoreUri"])

	presignRec := doSageMakerRequest(t, h, "CreatePresignedMlflowAppUrl", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, presignRec.Code)

	var presignResp map[string]string
	require.NoError(t, json.Unmarshal(presignRec.Body.Bytes(), &presignResp))
	assert.Contains(t, presignResp["AuthorizedUrl"], "app-1")

	listRec := doSageMakerRequest(t, h, "ListMlflowApps", map[string]any{})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries, ok := listResp["Summaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)

	deleteRec := doSageMakerRequest(t, h, "DeleteMlflowApp", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, deleteRec.Code)

	var deleteResp map[string]string
	require.NoError(t, json.Unmarshal(deleteRec.Body.Bytes(), &deleteResp))
	assert.Equal(t, appARN, deleteResp["Arn"])

	describeRec = doSageMakerRequest(t, h, "DescribeMlflowApp", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusBadRequest, describeRec.Code)
}

// TestHandler_MlflowApp_WeeklyMaintenanceWindowStart_RealClient proves
// WeeklyMaintenanceWindowStart — absent from both CreateMlflowAppInput and
// UpdateMlflowAppInput before this pass — round-trips through
// DescribeMlflowApp.
func TestHandler_MlflowApp_WeeklyMaintenanceWindowStart_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	created, err := client.CreateMlflowApp(t.Context(), &sagemakersdk.CreateMlflowAppInput{
		Name:                         aws.String("app-window"),
		ArtifactStoreUri:             aws.String("s3://bucket/window"),
		RoleArn:                      aws.String("arn:aws:iam::000000000000:role/access"),
		WeeklyMaintenanceWindowStart: aws.String("MON:01:00"),
	})
	require.NoError(t, err)

	out, err := client.DescribeMlflowApp(t.Context(), &sagemakersdk.DescribeMlflowAppInput{Arn: created.Arn})
	require.NoError(t, err)
	assert.Equal(t, "MON:01:00", aws.ToString(out.WeeklyMaintenanceWindowStart))

	_, err = client.UpdateMlflowApp(t.Context(), &sagemakersdk.UpdateMlflowAppInput{
		Arn:                          created.Arn,
		WeeklyMaintenanceWindowStart: aws.String("FRI:05:15"),
	})
	require.NoError(t, err)

	out, err = client.DescribeMlflowApp(t.Context(), &sagemakersdk.DescribeMlflowAppInput{Arn: created.Arn})
	require.NoError(t, err)
	assert.Equal(t, "FRI:05:15", aws.ToString(out.WeeklyMaintenanceWindowStart))
}

// TestHandler_UpdateMlflowApp_NameIsNoOp_RealClient proves UpdateMlflowApp's
// Name field is decoded but does not rename the App — see
// UpdateMlflowAppOptions' doc comment (mlflow.go) for why: this backend's
// Arn is built from Name at creation and rekeying on an update is not
// something either AWS's or this repo's docs establish as real behavior.
func TestHandler_UpdateMlflowApp_NameIsNoOp_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	created, err := client.CreateMlflowApp(t.Context(), &sagemakersdk.CreateMlflowAppInput{
		Name:             aws.String("app-original-name"),
		ArtifactStoreUri: aws.String("s3://bucket/name"),
		RoleArn:          aws.String("arn:aws:iam::000000000000:role/access"),
	})
	require.NoError(t, err)

	_, err = client.UpdateMlflowApp(t.Context(), &sagemakersdk.UpdateMlflowAppInput{
		Arn:  created.Arn,
		Name: aws.String("attempted-rename"),
	})
	require.NoError(t, err)

	out, err := client.DescribeMlflowApp(t.Context(), &sagemakersdk.DescribeMlflowAppInput{Arn: created.Arn})
	require.NoError(t, err)
	assert.Equal(t, "app-original-name", aws.ToString(out.Name))
}

// TestHandler_ListMlflowApps_FilterSortPage_RealClient proves eight of the
// nine previously-absent ListMlflowAppsInput fields (AccountDefaultStatus,
// CreatedAfter, CreatedBefore, DefaultForDomainId, MaxResults, SortBy,
// SortOrder, Status) filter, sort and page the real result set.
// MlflowVersion (both the input filter and Summaries.MlflowVersion, also
// newly modeled) is deliberately not exercised here: neither
// CreateMlflowAppInput nor UpdateMlflowAppInput carries an MlflowVersion
// field at all (confirmed against both api_op_ files), so no real client can
// ever populate a non-empty value for this backend to filter or sort on —
// disclosed as unreachable rather than fabricated, the same shape as
// parity-10's ListPipelineExecutionSteps pagination finding.
func TestHandler_ListMlflowApps_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, tc := range []struct{ name, accountDefaultStatus, domain string }{
		{"app-old", "ENABLED", "d-aaaaaaaaaa"},
		{"app-new", "DISABLED", "d-bbbbbbbbbb"},
	} {
		_, err := client.CreateMlflowApp(t.Context(), &sagemakersdk.CreateMlflowAppInput{
			Name:                 aws.String(tc.name),
			ArtifactStoreUri:     aws.String("s3://bucket/" + tc.name),
			RoleArn:              aws.String("arn:aws:iam::000000000000:role/access"),
			AccountDefaultStatus: smtypes.AccountDefaultStatus(tc.accountDefaultStatus),
			DefaultDomainIdList:  []string{tc.domain},
		})
		require.NoError(t, err)
	}

	future := time.Now().Add(365 * 24 * time.Hour)
	past := time.Now().Add(-365 * 24 * time.Hour)

	windowed, err := client.ListMlflowApps(t.Context(), &sagemakersdk.ListMlflowAppsInput{
		CreatedAfter:  &past,
		CreatedBefore: &future,
		SortBy:        smtypes.SortMlflowAppByCreationTime,
		SortOrder:     smtypes.SortOrderAscending,
	})
	require.NoError(t, err)
	require.Len(t, windowed.Summaries, 2)
	assert.Equal(t, "app-old", aws.ToString(windowed.Summaries[0].Name))
	assert.Equal(t, "app-new", aws.ToString(windowed.Summaries[1].Name))

	excluded, err := client.ListMlflowApps(t.Context(), &sagemakersdk.ListMlflowAppsInput{CreatedAfter: &future})
	require.NoError(t, err)
	assert.Empty(t, excluded.Summaries)

	byStatus, err := client.ListMlflowApps(t.Context(), &sagemakersdk.ListMlflowAppsInput{
		Status: smtypes.MlflowAppStatusCreated,
	})
	require.NoError(t, err)
	assert.Len(t, byStatus.Summaries, 2, "every app created by this backend is Created; never Deleting/Deleted")

	byAccountDefault, err := client.ListMlflowApps(t.Context(), &sagemakersdk.ListMlflowAppsInput{
		AccountDefaultStatus: smtypes.AccountDefaultStatusDisabled,
	})
	require.NoError(t, err)
	require.Len(t, byAccountDefault.Summaries, 1)
	assert.Equal(t, "app-new", aws.ToString(byAccountDefault.Summaries[0].Name))

	byDomain, err := client.ListMlflowApps(t.Context(), &sagemakersdk.ListMlflowAppsInput{
		DefaultForDomainId: aws.String("d-bbbbbbbbbb"),
	})
	require.NoError(t, err)
	require.Len(t, byDomain.Summaries, 1)
	assert.Equal(t, "app-new", aws.ToString(byDomain.Summaries[0].Name))

	defaultOrder, err := client.ListMlflowApps(t.Context(), &sagemakersdk.ListMlflowAppsInput{})
	require.NoError(t, err)
	require.Len(t, defaultOrder.Summaries, 2)
	assert.Equal(t, "app-new", aws.ToString(defaultOrder.Summaries[0].Name),
		"default sort is Descending by CreationTime per api_op_ListMlflowApps.go:22-24")

	page1, err := client.ListMlflowApps(t.Context(), &sagemakersdk.ListMlflowAppsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.Summaries, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))
}

func TestHandler_CreateMlflowApp_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateMlflowApp", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeMlflowApp_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeMlflowApp", map[string]any{
		"Arn": "arn:aws:sagemaker:us-east-1:0:mlflow-app/missing",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateMlflowApp_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"Name": "dup-app"}

	rec := doSageMakerRequest(t, h, "CreateMlflowApp", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateMlflowApp", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ---------------------------------------------------------------------------
// PartnerApp — UpdatePartnerApp / ListPartnerApps / CreatePartnerAppPresignedUrl
// ---------------------------------------------------------------------------

func TestHandler_ListMlflowTrackingServers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Initially empty
	rec := doSageMakerRequest(t, h, "ListMlflowTrackingServers", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["TrackingServerSummaries"])

	// Create one
	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "my-server",
		"RoleArn":            "arn:aws:iam::000000000000:role/TestRole",
		"MlflowVersion":      "2.0.0",
	})

	// List shows it
	rec = doSageMakerRequest(t, h, "ListMlflowTrackingServers", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["TrackingServerSummaries"].([]any)
	assert.Len(t, summaries, 1)

	summary := summaries[0].(map[string]any)
	assert.Equal(t, "my-server", summary["TrackingServerName"])
	assert.Equal(t, "2.0.0", summary["MlflowVersion"])
}

// ---------------------------------------------------------------------------
// UpdateMlflowTrackingServer tests
// ---------------------------------------------------------------------------

func TestHandler_UpdateMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "my-server",
		"RoleArn":            "arn:aws:iam::000000000000:role/TestRole",
		"MlflowVersion":      "2.0.0",
	})

	rec := doSageMakerRequest(t, h, "UpdateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "my-server",
		"MlflowVersion":      "2.1.0",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["TrackingServerArn"])
}

func TestHandler_UpdateMlflowTrackingServer_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ModelCard list tests
// ---------------------------------------------------------------------------
