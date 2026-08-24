package ec2_test

import (
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestEnableSerialConsoleAccess_WireShape_RealClient covers
// handleEnableSerialConsoleAccess, which pre-fix rendered a bare
// <return>true</return> via stubResponse. The real
// EnableSerialConsoleAccessOutput has no Return member -- only
// SerialConsoleAccessEnabled (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentEnableSerialConsoleAccessOutput has no
// case for "return") -- so a client checking whether access was enabled saw
// false pre-fix regardless of the real outcome.
func TestEnableSerialConsoleAccess_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	out, err := client.EnableSerialConsoleAccess(t.Context(), &ec2sdk.EnableSerialConsoleAccessInput{})
	require.NoError(t, err)
	assert.True(
		t, aws.ToBool(out.SerialConsoleAccessEnabled),
		"SerialConsoleAccessEnabled false - real deserializer has no case for <return>",
	)
}

// TestDisableSerialConsoleAccess_WireShape_RealClient covers
// handleDisableSerialConsoleAccess, same shape gap as Enable (ec2@v1.319.1
// deserializers.go,
// awsEc2query_deserializeOpDocumentDisableSerialConsoleAccessOutput). Correct
// SerialConsoleAccessEnabled here is false, the same as a real client's
// zero-value decode of an absent field, so a real-SDK round trip alone can't
// distinguish pre-fix from post-fix. Asserting on the raw response body
// instead: pre-fix this rendered a bare <return>true</return> with no
// <serialConsoleAccessEnabled> element at all.
func TestDisableSerialConsoleAccess_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))

	_, err := ec2.ExportDispatch(h, url.Values{"Action": {"EnableSerialConsoleAccess"}})
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{"Action": {"DisableSerialConsoleAccess"}})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DisableSerialConsoleAccessResponse>")
	assert.Contains(
		t, resp, "<serialConsoleAccessEnabled>false</serialConsoleAccessEnabled>",
		"pre-fix this element never appeared, only a bare <return>true</return>",
	)
	assert.NotContains(t, resp, "<return>", "pre-fix shape had a spurious <return> the real deserializer never matches")
}
