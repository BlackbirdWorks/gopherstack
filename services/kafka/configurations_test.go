package kafka_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestCreateConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*kafka.InMemoryBackend)
		name     string
		confName string
		wantErr  bool
	}{
		{
			name:     "success",
			confName: "my-config",
			setup:    func(_ *kafka.InMemoryBackend) {},
		},
		{
			name:     "duplicate_name",
			confName: "my-config",
			setup: func(b *kafka.InMemoryBackend) {
				_, _ = b.CreateConfiguration(
					context.Background(),
					"my-config",
					"",
					[]string{"2.8.0"},
					"auto.create.topics.enable=false",
				)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			tt.setup(b)

			config, err := b.CreateConfiguration(context.Background(),
				tt.confName,
				"test config",
				[]string{"2.8.0"},
				"auto.create.topics.enable=false",
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.confName, config.Name)
			assert.NotEmpty(t, config.Arn)
			assert.Contains(t, config.Arn, "configuration/"+tt.confName+"/")
		})
	}
}

func TestDescribeConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kafka.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "existing_config",
			setup: func(b *kafka.InMemoryBackend) string {
				c, _ := b.CreateConfiguration(context.Background(), "my-config", "", []string{"2.8.0"}, "")

				return c.Arn
			},
		},
		{
			name: "not_found",
			setup: func(_ *kafka.InMemoryBackend) string {
				return "arn:aws:kafka:us-east-1:000000000000:configuration/nonexistent/uuid"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			arn := tt.setup(b)

			config, err := b.DescribeConfiguration(context.Background(), arn)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, arn, config.Arn)
		})
	}
}

func TestCreateConfiguration_RequiresName(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateConfiguration(context.Background(), "", "", nil, "")

	require.Error(t, err)
	require.ErrorIs(t, err, kafka.ErrValidation)
}

func TestSortedListConfigurations(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	b.AddConfigurationInternal("zzz-cfg")
	b.AddConfigurationInternal("aaa-cfg")
	b.AddConfigurationInternal("mmm-cfg")

	cfgs := b.ListConfigurations(context.Background())
	require.Len(t, cfgs, 3)
	assert.Equal(t, "aaa-cfg", cfgs[0].Name)
	assert.Equal(t, "mmm-cfg", cfgs[1].Name)
	assert.Equal(t, "zzz-cfg", cfgs[2].Name)
}

func TestDeepCopy_ConfigurationDoesNotAlias(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cfg := b.AddConfigurationInternal("cfg1")

	cfg.Name = "mutated"
	described, err := b.DescribeConfiguration(context.Background(), cfg.Arn)
	require.NoError(t, err)
	assert.Equal(t, "cfg1", described.Name)
}
