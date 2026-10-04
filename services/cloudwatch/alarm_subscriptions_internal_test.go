package cloudwatch

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnsubscribeAlarmStateChange_ReleasesArnEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arns int
	}{
		{name: "one arn", arns: 1},
		{name: "many arns", arns: 50},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()

			for i := range tc.arns {
				unsub := b.SubscribeAlarmStateChange("arn:"+strconv.Itoa(i), func(string) {})
				unsub()
			}

			b.mu.RLock("test")
			defer b.mu.RUnlock()

			assert.Empty(t, b.alarmStateSubscribers)
		})
	}
}
