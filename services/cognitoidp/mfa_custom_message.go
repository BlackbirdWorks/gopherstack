package cognitoidp

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// customMessage is the message override returned by a CustomMessage trigger.
type customMessage struct {
	sms, email, subject string
}

// mfaCustomMessageLocked fires CustomMessage_Authentication with b.mu released; nil means no override.
// Caller holds the write lock.
func (b *InMemoryBackend) mfaCustomMessageLocked(pool *UserPool, clientID, username string) *customMessage {
	call := b.prepareCustomMessageInPoolLocked(pool.ID, clientID, username, triggerSourceCustomMessageAuth, nil)
	if call == nil {
		return nil
	}

	var (
		result map[string]any
		err    error
	)

	b.releaseLocked("MFACustomMessage", func() {
		result, err = call.inv.InvokeTrigger(context.Background(), call.functionARN, call.event)
	})

	resp, err := parseTriggerResult(triggerKeyCustomMessage, result, err)
	if err != nil || resp == nil {
		if err != nil {
			logger.Load(context.Background()).Warn("cognitoidp: CustomMessage_Authentication trigger failed",
				"pool", pool.ID, "error", err)
		}

		return nil
	}

	cm := &customMessage{}
	cm.sms, _ = resp["smsMessage"].(string)
	cm.email, _ = resp["emailMessage"].(string)
	cm.subject, _ = resp["emailSubject"].(string)

	return cm
}

// apply replaces the rendered bodies (and email subject) with the trigger's, substituting the code.
func (m *customMessage) apply(jobs []sendJob, code string) {
	if m == nil {
		return
	}

	for i := range jobs {
		j := &jobs[i]

		msg := m.email
		if j.medium == mediumSMSName {
			msg = m.sms
		} else if m.subject != "" {
			j.subject = m.subject
		}

		if msg != "" {
			j.body = strings.ReplaceAll(msg, customMessageCodeParameter, code)
		}
	}
}
