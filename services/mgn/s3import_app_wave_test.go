package mgn_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/aws/aws-sdk-go-v2/service/mgn/types"
	"github.com/stretchr/testify/require"
)

// runImportAndWait drives one StartImport call to a terminal status and
// returns the finished ImportTask, mirroring sdk_roundtrip_test.go's own
// inline helper (kept local since this file's cases also want the errors
// list, not just Summary).
func runImportAndWait(t *testing.T, client *mgnsdk.Client, bucket, csvBody string, s3 *mockS3) types.ImportTask {
	t.Helper()

	const key = "servers.csv"

	ctx := t.Context()
	s3.put(bucket, key, csvBody)

	started, err := client.StartImport(ctx, &mgnsdk.StartImportInput{
		S3BucketSource: &types.S3BucketSource{S3Bucket: aws.String(bucket), S3Key: aws.String(key)},
	})
	require.NoError(t, err)

	importID := aws.ToString(started.ImportTask.ImportID)

	var final types.ImportTask

	require.Eventually(t, func() bool {
		out, listErr := client.ListImports(ctx, &mgnsdk.ListImportsInput{
			Filters: &types.ListImportsRequestFilters{ImportIDs: []string{importID}},
		})
		if listErr != nil || len(out.Items) != 1 {
			return false
		}

		status := out.Items[0].Status
		if status != types.ImportStatusSucceeded && status != types.ImportStatusFailed {
			return false
		}

		final = out.Items[0]

		return true
	}, defaultAsyncWait, defaultAsyncPoll, "import task never reached a terminal status")

	return final
}

// TestStartImport_ApplicationsAndWaves drives mgn:app:*/mgn:wave:* rows
// through the real StartImport wire path, confirming Applications/Waves are
// really created (or updated, by name), attached in the documented Wave ->
// Application -> SourceServer hierarchy, and that ImportTaskSummary's
// Applications/Waves counts -- always zero before this pass -- now reflect
// real activity (AWS MGN User Guide, "Inventory Import parameters").
func TestStartImport_ApplicationsAndWaves(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	s3 := newMockS3()
	h.Backend.SetS3Backend(s3)

	header := "mgn:server:hostname,mgn:server:user-provided-id,mgn:app:name,mgn:app:description," +
		"mgn:wave:name,mgn:wave:description\n"
	csvBody := header +
		"web-1.example.com,web-1-id,payments-app,Payments service,wave-1,First migration wave\n" +
		"web-2.example.com,web-2-id,payments-app,Payments service,wave-1,First migration wave\n"

	final := runImportAndWait(t, client, "app-wave-bucket", csvBody, s3)

	require.Equal(t, types.ImportStatusSucceeded, final.Status)
	require.EqualValues(t, 2, final.Summary.Servers.CreatedCount, "two rows created a server")
	require.EqualValues(t, 1, final.Summary.Applications.CreatedCount, "one app across two rows: created once")
	require.EqualValues(t, 1, final.Summary.Applications.ModifiedCount, "second row updates the same app")
	require.EqualValues(t, 1, final.Summary.Waves.CreatedCount, "one wave across two rows: created once")
	require.EqualValues(t, 1, final.Summary.Waves.ModifiedCount, "second row updates the same wave")

	listedApps, err := client.ListApplications(ctx, &mgnsdk.ListApplicationsInput{})
	require.NoError(t, err)
	require.Len(t, listedApps.Items, 1)
	require.Equal(t, "payments-app", aws.ToString(listedApps.Items[0].Name))
	require.Equal(t, "Payments service", aws.ToString(listedApps.Items[0].Description))

	appID := aws.ToString(listedApps.Items[0].ApplicationID)

	listedWaves, err := client.ListWaves(ctx, &mgnsdk.ListWavesInput{})
	require.NoError(t, err)
	require.Len(t, listedWaves.Items, 1)
	require.Equal(t, "wave-1", aws.ToString(listedWaves.Items[0].Name))

	waveID := aws.ToString(listedWaves.Items[0].WaveID)
	require.Equal(t, waveID, aws.ToString(listedApps.Items[0].WaveID), "app attached to the wave both rows named")

	describedServers, err := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{})
	require.NoError(t, err)
	require.Len(t, describedServers.Items, 2)

	for _, s := range describedServers.Items {
		require.Equal(t, appID, aws.ToString(s.ApplicationID), "server attached to the app its row named")
	}
}

// TestStartImport_ResourcePropertyWithoutIdentity confirms a row that gives
// a resource's property (e.g. mgn:wave:description) without also naming
// that resource (mgn:wave:name/mgn:wave:id) still imports its identified
// server cleanly -- the orphaned property is dropped (see s3import.go's
// parseImportRow doc comment), not treated as a row failure and not
// applied to any Wave.
func TestStartImport_ResourcePropertyWithoutIdentity(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	s3 := newMockS3()
	h.Backend.SetS3Backend(s3)

	csvBody := "mgn:server:hostname,mgn:wave:description\nweb-1.example.com,orphaned wave property\n"

	final := runImportAndWait(t, client, "orphan-bucket", csvBody, s3)

	require.Equal(t, types.ImportStatusSucceeded, final.Status)
	require.EqualValues(t, 1, final.Summary.Servers.CreatedCount)
	require.EqualValues(t, 0, final.Summary.Waves.CreatedCount)

	errs, err := client.ListImportErrors(t.Context(), &mgnsdk.ListImportErrorsInput{ImportID: final.ImportID})
	require.NoError(t, err)
	require.Empty(t, errs.Items)

	waves, err := client.ListWaves(ctx, &mgnsdk.ListWavesInput{})
	require.NoError(t, err)
	require.Empty(t, waves.Items)
}

// TestStartImport_ExplicitIDNotFound drives mgn:app:id/mgn:wave:id/
// mgn:server:id values that name no existing resource -- AWS's own
// documented rule ("If this resource is not found, the import will fail" --
// import-parameters.html, Additional considerations #3), interpreted here
// (per this file's own doc comment) as a row-level ImportTaskError carrying
// the referenced ID back, not a whole-task FAILED, matching this backend's
// established partial-success convention.
func TestStartImport_ExplicitIDNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkID func(*types.ImportErrorData) string
		name    string
		csv     string
		wantID  string
	}{
		{
			name:    "unknown mgn:server:id",
			csv:     "mgn:server:id\nsrv-does-not-exist\n",
			wantID:  "srv-does-not-exist",
			checkID: func(d *types.ImportErrorData) string { return aws.ToString(d.SourceServerID) },
		},
		{
			name:    "unknown mgn:app:id",
			csv:     "mgn:app:id\napp-does-not-exist\n",
			wantID:  "app-does-not-exist",
			checkID: func(d *types.ImportErrorData) string { return aws.ToString(d.ApplicationID) },
		},
		{
			name:    "unknown mgn:wave:id",
			csv:     "mgn:wave:id\nwave-does-not-exist\n",
			wantID:  "wave-does-not-exist",
			checkID: func(d *types.ImportErrorData) string { return aws.ToString(d.WaveID) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, client := newTestHandlerAndClient(t)

			s3 := newMockS3()
			h.Backend.SetS3Backend(s3)

			final := runImportAndWait(t, client, "id-not-found-bucket", tt.csv, s3)

			require.Equal(t, types.ImportStatusSucceeded, final.Status, "an id-not-found row fails only itself")

			errs, err := client.ListImportErrors(t.Context(), &mgnsdk.ListImportErrorsInput{ImportID: final.ImportID})
			require.NoError(t, err)
			require.Len(t, errs.Items, 1)
			require.Equal(t, tt.wantID, tt.checkID(errs.Items[0].ErrorData))
		})
	}
}

// TestStartImport_UpdateByExplicitID drives mgn:app:id/mgn:wave:id against a
// previously-created Application/Wave, confirming the explicit-ID path
// updates the same resource (ModifiedCount) rather than creating a second
// one -- the "update" half of Additional considerations #3.
func TestStartImport_UpdateByExplicitID(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	app, err := client.CreateApplication(ctx, &mgnsdk.CreateApplicationInput{Name: aws.String("pre-existing-app")})
	require.NoError(t, err)

	s3 := newMockS3()
	h.Backend.SetS3Backend(s3)

	csvBody := "mgn:app:id,mgn:app:description\n" + aws.ToString(app.ApplicationID) + ",updated via import\n"

	final := runImportAndWait(t, client, "update-by-id-bucket", csvBody, s3)

	require.Equal(t, types.ImportStatusSucceeded, final.Status)
	require.EqualValues(t, 0, final.Summary.Applications.CreatedCount)
	require.EqualValues(t, 1, final.Summary.Applications.ModifiedCount)

	listed, err := client.ListApplications(ctx, &mgnsdk.ListApplicationsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)
	require.Equal(t, "updated via import", aws.ToString(listed.Items[0].Description))
}
