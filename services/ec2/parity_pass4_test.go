package ec2_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDescribeInstanceStatus_IncludesHealthObjects verifies that
// DescribeInstanceStatus emits the systemStatus and instanceStatus health
// objects (status "initializing" while pending, "ok" once running) that the SDK
// InstanceStatusOk waiter polls. Previously these objects were omitted entirely.
func TestDescribeInstanceStatus_IncludesHealthObjects(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	runResp, err := dispatchHandler(h, url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-12345678"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
		"MaxCount":     {"1"},
	})
	require.NoError(t, err)

	id := accuracyExtractXMLValue(runResp, "instanceId")
	require.NotEmpty(t, id)

	statusReq := url.Values{
		"Action":     {"DescribeInstanceStatus"},
		"Version":    {"2016-11-15"},
		"InstanceId": {id},
	}

	// While pending: health objects present, reporting "initializing".
	pendingResp, err := dispatchHandler(h, statusReq)
	require.NoError(t, err)
	assert.Contains(t, pendingResp, "<systemStatus>", "systemStatus health object must be present")
	assert.Contains(t, pendingResp, "<instanceStatus>", "instanceStatus health object must be present")
	assert.Contains(t, pendingResp, "reachability")
	assert.Contains(t, pendingResp, "<status>initializing</status>")

	// Advance pending → running deterministically.
	b.TickLifecycleForTest()

	runningResp, err := dispatchHandler(h, statusReq)
	require.NoError(t, err)
	assert.Contains(t, runningResp, "<name>running</name>")
	assert.GreaterOrEqual(t, strings.Count(runningResp, "<status>ok</status>"), 2,
		"both system and instance status should report ok once running")
	assert.Contains(t, runningResp, "<status>passed</status>")
}
