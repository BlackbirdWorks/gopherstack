package firehose_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func TestBackend_StreamCount(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, firehose.StreamCount(b))

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "s1"})
	require.NoError(t, err)
	assert.Equal(t, 1, firehose.StreamCount(b))

	b.Reset()
	assert.Equal(t, 0, firehose.StreamCount(b))
}

func TestBackend_AddStreamInternal(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	s := &firehose.DeliveryStream{
		Name:   "injected",
		ARN:    "arn:aws:firehose:us-east-1:000000000000:deliverystream/injected",
		Status: "ACTIVE",
	}
	b.AddStreamInternal(s)

	got, err := b.DescribeDeliveryStream(context.TODO(), "injected")
	require.NoError(t, err)
	assert.Equal(t, "injected", got.Name)
}
