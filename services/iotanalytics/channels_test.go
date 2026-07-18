package iotanalytics_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

func TestInMemoryBackend_Channel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		channelName string
		errType     string
		wantErr     bool
	}{
		{
			name:        "create_and_describe",
			channelName: "my_channel",
		},
		{
			name:        "describe_not_found",
			channelName: "nonexistent",
			wantErr:     true,
			errType:     "describe",
		},
		{
			name:        "delete_not_found",
			channelName: "nonexistent",
			wantErr:     true,
			errType:     "delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()

			switch tt.errType {
			case "describe":
				_, err := b.DescribeChannel(tt.channelName)
				require.Error(t, err)
				assert.Equal(t, iotanalytics.ErrChannelNotFound, err)
			case "delete":
				err := b.DeleteChannel(tt.channelName)
				require.Error(t, err)
				assert.Equal(t, iotanalytics.ErrChannelNotFound, err)
			default:
				ch, err := b.CreateChannel(
					context.Background(), tt.channelName, map[string]string{"env": "test"}, nil, nil)
				require.NoError(t, err)
				assert.Equal(t, tt.channelName, ch.Name)
				assert.Equal(t, "ACTIVE", ch.Status)
				assert.NotEmpty(t, ch.ARN)

				got, err := b.DescribeChannel(tt.channelName)
				require.NoError(t, err)
				assert.Equal(t, tt.channelName, got.Name)

				err = b.UpdateChannel(tt.channelName, nil, nil)
				require.NoError(t, err)

				list := b.ListChannels()
				assert.Len(t, list, 1)

				err = b.DeleteChannel(tt.channelName)
				require.NoError(t, err)

				list = b.ListChannels()
				assert.Empty(t, list)
			}
		})
	}
}

// TestInMemoryBackend_SortedListChannels verifies ListChannels returns channels sorted by name.
func TestInMemoryBackend_SortedListChannels(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	b.AddChannelInternal("zz_channel")
	b.AddChannelInternal("aa_channel")
	b.AddChannelInternal("mm_channel")

	channels := b.ListChannels()
	require.Len(t, channels, 3)
	assert.Equal(t, "aa_channel", channels[0].Name)
	assert.Equal(t, "mm_channel", channels[1].Name)
	assert.Equal(t, "zz_channel", channels[2].Name)
}

// TestInMemoryBackend_DeepCopy_Channel verifies DescribeChannel returns an independent copy.
func TestInMemoryBackend_DeepCopy_Channel(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	_, err := b.CreateChannel(context.Background(), "immutable_ch", map[string]string{"key": "original"}, nil, nil)
	require.NoError(t, err)

	ch, err := b.DescribeChannel("immutable_ch")
	require.NoError(t, err)

	// Mutate the returned copy; stored state should be unchanged.
	ch.Tags["key"] = "mutated"
	ch.Name = "mutated_name"

	ch2, err := b.DescribeChannel("immutable_ch")
	require.NoError(t, err)
	assert.Equal(t, "immutable_ch", ch2.Name)
	assert.Equal(t, "original", ch2.Tags["key"])
}
