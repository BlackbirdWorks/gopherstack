package iotwireless_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func TestFuotaTask_LifecycleProgression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		want   string
		before string
		delay  time.Duration
		wait   time.Duration
	}{
		{name: "no_delay", delay: 0, wait: time.Hour, before: "FuotaSession_Waiting", want: "FuotaSession_Waiting"},
		{
			name:   "in_session",
			delay:  time.Minute,
			wait:   time.Minute,
			before: "FuotaSession_Waiting",
			want:   "In_FuotaSession",
		},
		{name: "done", delay: time.Minute, wait: 2 * time.Minute, before: "FuotaSession_Waiting", want: "FuotaDone"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := iotwireless.NewInMemoryBackend()
				b.SetLifecycleDelay(tt.delay)

				ft, err := b.CreateFuotaTask("111122223333", "us-east-1", "f", "", "", "", "", 0, 0, 0, nil, nil)
				require.NoError(t, err)
				assert.Equal(t, "Pending", ft.Status)

				require.NoError(t, b.StartFuotaTask("111122223333", "us-east-1", ft.ID, nil))

				got, err := b.GetFuotaTask("111122223333", "us-east-1", ft.ID)
				require.NoError(t, err)
				assert.Equal(t, tt.before, got.Status)

				time.Sleep(tt.wait)

				got, err = b.GetFuotaTask("111122223333", "us-east-1", ft.ID)
				require.NoError(t, err)
				assert.Equal(t, tt.want, got.Status)
			})
		})
	}
}
