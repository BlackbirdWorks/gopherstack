package ses_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// inboundHarness starts a listener whose single catch-all rule publishes to topic (and runs extra actions first).
func inboundHarness(
	t *testing.T, topic string, extra ...ses.ReceiptAction,
) (*ses.Handler, *fakeInboundSNS, *ses.InMemoryBackend) {
	t.Helper()

	b := ses.NewInMemoryBackend()
	snsF := &fakeInboundSNS{msgs: map[string][]string{}}
	b.SetSNSPublisher(snsF)

	_, err := b.VerifyDomainIdentity("example.com")
	require.NoError(t, err)
	require.NoError(t, b.CreateReceiptRuleSet("rs"))

	actions := extra
	if topic != "" {
		actions = append(actions, ses.ReceiptAction{Type: ses.ReceiptActionTypeSNS, SNSTopicARN: topic})
	}

	require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "r", Enabled: true, Actions: actions}, ""))
	require.NoError(t, b.SetActiveReceiptRuleSet("rs"))

	h := ses.NewHandler(b).WithInbound(ses.InboundConfig{Addr: "127.0.0.1:0"})
	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	return h, snsF, b
}
