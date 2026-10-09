package eks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCertificateAuthorityTimeline_RollbackWindow(t *testing.T) {
	t.Parallel()

	notAfter := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	activated := notAfter.AddDate(-4, 0, 0)

	tests := []struct {
		now  time.Time
		name string
		want bool
	}{
		{notAfter.AddDate(0, 0, -46), "before_final_activation", true},
		{notAfter.AddDate(0, 0, -45), "at_final_activation", false},
		{notAfter.AddDate(0, 0, -10), "after_final_activation", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ca := &CertificateAuthority{
				NotAfter: notAfter, SigningStatus: caSigningStatusNotUsed,
				ActivatedAt: &activated, RollbackAvailable: true,
			}
			(&InMemoryBackend{}).applyCertificateAuthorityTimeline(ca, "c", tt.now)

			assert.Equal(t, tt.want, ca.RollbackAvailable)
		})
	}
}
