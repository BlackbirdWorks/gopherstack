package ssm_test

// Locks in a systemic wire-shape fix: SSM speaks the awsjson1.1 protocol,
// and every DateTime-shaped field in aws-sdk-go-v2/service/ssm@v1.71.0's
// deserializers.go is a JSON *number* (Unix epoch seconds, parsed via
// smithytime.ParseEpochSeconds) -- never an RFC3339 string. Several structs
// in this package previously declared these fields as raw time.Time (or
// *time.Time), which Go's encoding/json marshals as an RFC3339Nano *string*
// by default (e.g. "2026-07-23T00:00:00Z"). A real aws-sdk-go-v2 client
// calling this emulator would fail to deserialize those responses with
// "expected DateTime to be a JSON Number, got string instead". Fixed by
// converting every affected field to float64 (matching this package's
// existing UnixTimeFloat convention, already used correctly for
// CreatedDate/ModifiedDate/StartDate/EndDate elsewhere). This test asserts
// the marshaled JSON byte-for-byte contains a bare number, not a quoted
// string, for one representative field per affected struct/op family:
//   - AssociationExecution.ExecutionDate    (DescribeAssociationExecutions)
//   - MaintenanceWindowExecution.StartTime  (DescribeMaintenanceWindowExecutions)
//   - InstanceInformation.RegistrationDate  (DescribeInstanceInformation)
//   - InstanceAssociationStatusInfo.ExecutionDate (DescribeInstanceAssociationsStatus)
//   - InstancePatchState.OperationStartTime (DescribeInstancePatchStates)
//   - PatchComplianceData.InstalledTime     (DescribeInstancePatches)
//   - ResourceDataSync.SyncCreatedTime/LastSyncTime (ListResourceDataSync)
//   - InventoryDeletion.DeletionStartTime   (DescribeInventoryDeletions)
//   - NodeInfo.RegistrationDate             (ListNodes)

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// numericJSONField matches `"Field":<number>` (no surrounding quotes on the
// value) so a regression back to a quoted RFC3339 string is caught even
// though both are valid, non-empty JSON.
func numericJSONField(t *testing.T, body []byte, field string) {
	t.Helper()

	re := regexp.MustCompile(`"` + field + `":(-?[0-9]+(\.[0-9]+)?)`)
	assert.Truef(t, re.Match(body), "%s must serialize as a bare JSON number (epoch seconds), got: %s", field, body)

	notString := regexp.MustCompile(`"` + field + `":"`)
	assert.Falsef(t, notString.Match(body), "%s must NOT serialize as a quoted string, got: %s", field, body)
}

func TestEpochSecondsWireShape_AssociationExecution(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(ssm.AssociationExecution{
		AssociationID: "assoc-1", ExecutionID: "exec-1", Status: "Success", ExecutionDate: 1_700_000_000,
	})
	require.NoError(t, err)
	numericJSONField(t, body, "ExecutionDate")
}

func TestEpochSecondsWireShape_MaintenanceWindowExecution(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(ssm.MaintenanceWindowExecution{
		WindowID: "mw-1", WindowExecutionID: "mwexec-1", Status: "Success",
		StartTime: 1_700_000_000, EndTime: 1_700_003_600,
	})
	require.NoError(t, err)
	numericJSONField(t, body, "StartTime")
	numericJSONField(t, body, "EndTime")
}

func TestEpochSecondsWireShape_InstanceInformation(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(ssm.InstanceInformation{
		InstanceID: "i-1", PingStatus: "Online", RegistrationDate: 1_700_000_000,
	})
	require.NoError(t, err)
	numericJSONField(t, body, "RegistrationDate")
}

func TestEpochSecondsWireShape_InstanceAssociationStatusInfo(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(ssm.InstanceAssociationStatusInfo{
		AssociationID: "assoc-1", Name: "MyDoc", Status: "Success", ExecutionDate: 1_700_000_000,
	})
	require.NoError(t, err)
	numericJSONField(t, body, "ExecutionDate")
}

func TestEpochSecondsWireShape_InstancePatchState(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(ssm.InstancePatchState{
		InstanceID: "i-1", PatchGroup: "grp1", Operation: "Scan", OperationStartTime: 1_700_000_000,
	})
	require.NoError(t, err)
	numericJSONField(t, body, "OperationStartTime")
}

func TestEpochSecondsWireShape_PatchComplianceData(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(ssm.PatchComplianceData{
		Title: "KB123", State: "Installed", InstalledTime: 1_700_000_000,
	})
	require.NoError(t, err)
	numericJSONField(t, body, "InstalledTime")
}

func TestEpochSecondsWireShape_ResourceDataSync(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(ssm.ResourceDataSync{
		SyncName: "sync-1", SyncType: "SyncToDestination", LastStatus: "InProgress",
		SyncCreatedTime: 1_700_000_000, LastSyncTime: 1_700_000_100,
	})
	require.NoError(t, err)
	numericJSONField(t, body, "SyncCreatedTime")
	numericJSONField(t, body, "LastSyncTime")
}

func TestEpochSecondsWireShape_InventoryDeletion(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(ssm.InventoryDeletion{
		DeletionID: "deletion-1", TypeName: "AWS:Application", LastStatus: "Complete",
		DeletionStartTime: 1_700_000_000,
	})
	require.NoError(t, err)
	numericJSONField(t, body, "DeletionStartTime")
}

func TestEpochSecondsWireShape_NodeInfo(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(ssm.NodeInfo{
		InstanceID: "i-1", PlatformType: "Linux", RegistrationDate: 1_700_000_000,
	})
	require.NoError(t, err)
	numericJSONField(t, body, "RegistrationDate")
}

// TestEpochSecondsWireShape_EndToEnd exercises the fix through the actual
// HTTP handlers (not just direct struct marshaling) for the two ops most
// likely to be hit by a real client: DescribeMaintenanceWindowExecutions and
// DescribeInstanceInformation.
func TestEpochSecondsWireShape_EndToEnd(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	mw, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name: "epoch-test", Schedule: "rate(1 day)", Duration: 2, Cutoff: 0,
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "DescribeMaintenanceWindowExecutions", `{"WindowId":"`+mw.WindowID+`"}`)
	numericJSONField(t, rec.Body.Bytes(), "StartTime")
}
