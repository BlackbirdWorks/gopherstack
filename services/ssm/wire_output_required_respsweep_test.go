package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/stretchr/testify/require"
)

// Test_SDKRoundTrip_DescribeInstancePatches_InstalledTime proves
// PatchComplianceData.InstalledTime -- "This member is required."
// (types.PatchComplianceData, deserializers.go:41268-41276, ssm@v1.77.0) --
// stays present on the wire for a Missing-state patch (never installed,
// zero installedTime). Before this fix the field was tagged omitempty, so
// a real client's Scan-only instance (the default Operation, no approved
// patches installed) decoded a nil InstalledTime for every patch, silently
// dropping a required member instead of emitting it present-and-empty.
func Test_SDKRoundTrip_DescribeInstancePatches_InstalledTime(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	client := newTestSSMClient(t, h)

	createOut, err := client.CreatePatchBaseline(t.Context(), &ssmsdk.CreatePatchBaselineInput{
		Name:            aws.String("respsweep-baseline"),
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)

	_, err = client.RegisterPatchBaselineForPatchGroup(t.Context(), &ssmsdk.RegisterPatchBaselineForPatchGroupInput{
		BaselineId: createOut.BaselineId,
		PatchGroup: aws.String("respsweep-group"),
	})
	require.NoError(t, err)

	_, err = client.SendCommand(t.Context(), &ssmsdk.SendCommandInput{
		DocumentName: aws.String("AWS-RunPatchBaseline"),
		InstanceIds:  []string{"i-respsweep"},
		Parameters: map[string][]string{
			"PatchGroup": {"respsweep-group"},
			// Operation deliberately omitted -- defaults to Scan, so every
			// patch is Missing (never installed) and InstalledTime is the
			// Go zero value, the exact case omitempty used to drop.
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeInstancePatches(t.Context(), &ssmsdk.DescribeInstancePatchesInput{
		InstanceId: aws.String("i-respsweep"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Patches)

	for _, p := range out.Patches {
		require.NotNil(t, p.InstalledTime, "required member InstalledTime must be present even when empty")
	}
}
