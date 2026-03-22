package batch_test

import (
"log/slog"
"testing"
"time"

"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"

"github.com/blackbirdworks/gopherstack/pkgs/service"
"github.com/blackbirdworks/gopherstack/services/batch"
)

type batchConfigProvider struct{ settings batch.Settings }

func (c batchConfigProvider) GetBatchSettings() batch.Settings { return c.settings }

// TestBatchProvider_Init_WithConfigProvider verifies that Provider.Init picks up
// JanitorInterval, InactiveJobDefTTL and CompletedJobTTL from the ConfigProvider.
func TestBatchProvider_Init_WithConfigProvider(t *testing.T) {
t.Parallel()

tests := []struct {
name                   string
config                 any
wantInterval           time.Duration
wantInactiveJobDefTTL  time.Duration
wantCompletedJobTTL    time.Duration
}{
{
name:                  "custom_settings_propagated",
config:                batchConfigProvider{batch.Settings{JanitorInterval: 5 * time.Minute, InactiveJobDefTTL: 48 * time.Hour, CompletedJobTTL: 12 * time.Hour}},
wantInterval:          5 * time.Minute,
wantInactiveJobDefTTL: 48 * time.Hour,
wantCompletedJobTTL:   12 * time.Hour,
},
{
name:                  "no_config_provider",
config:                nil,
wantInterval:          batch.DefaultJanitorInterval,
wantInactiveJobDefTTL: batch.DefaultInactiveJobDefTTL,
wantCompletedJobTTL:   batch.DefaultCompletedJobTTL,
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
t.Parallel()

p := &batch.Provider{}
svc, err := p.Init(&service.AppContext{Config: tt.config, Logger: slog.Default()})
require.NoError(t, err)

h, ok := svc.(*batch.Handler)
require.True(t, ok)
assert.Equal(t, tt.wantInterval, h.GetJanitorInterval())
assert.Equal(t, tt.wantInactiveJobDefTTL, h.GetJanitorInactiveJobDefTTL())
assert.Equal(t, tt.wantCompletedJobTTL, h.GetJanitorCompletedJobTTL())
})
}
}
