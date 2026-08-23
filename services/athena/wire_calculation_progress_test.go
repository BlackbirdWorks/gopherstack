package athena_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

// TestGetCalculationExecutionStatus_ProgressDecodesAsString drives
// GetCalculationExecutionStatus through the real aws-sdk-go-v2 athena
// client. CalculationStatistics.Progress is a bare string on the real shape
// (types.go), not a number -- the deserializer's case "Progress" type-
// switches on value.(string), so gopherstack's previous int64 mock value
// (100) failed every real client's decode as soon as a calculation existed.
func TestGetCalculationExecutionStatus_ProgressDecodesAsString(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	h := athena.NewHandler(b)
	client := newTestAthenaClient(t, h)

	require.NoError(t, b.CreateWorkGroup("wg", "", "ENABLED", athena.WorkGroupConfiguration{}, nil))
	sessionID, _, err := b.StartSession(
		"wg",
		"",
		"",
		athena.EngineConfiguration{},
		athena.SessionConfiguration{},
		athena.MonitoringConfiguration{},
		"",
	)
	require.NoError(t, err)

	calcID, _, err := b.StartCalculationExecution(sessionID, "", "print(1)")
	require.NoError(t, err)

	out, err := client.GetCalculationExecutionStatus(
		t.Context(),
		&athenasdk.GetCalculationExecutionStatusInput{
			CalculationExecutionId: aws.String(calcID),
		},
	)
	require.NoError(
		t,
		err,
		"real SDK client must decode GetCalculationExecutionStatus without error",
	)
	require.NotNil(t, out.Statistics)
	assert.Equal(t, "COMPLETED", aws.ToString(out.Statistics.Progress))
}
