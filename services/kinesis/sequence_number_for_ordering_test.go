package kinesis_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// PutRecordInput.SequenceNumberForOrdering: chaining the previous sequence number yields increasing numbers.
func TestPutRecord_SequenceNumberForOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ordering string
		chain    bool
		wantErr  bool
	}{
		{name: "chained", chain: true},
		{name: "zero", ordering: "0"},
		{name: "letters", ordering: "abc", wantErr: true},
		{name: "leading-zero", ordering: "0123", wantErr: true},
		{name: "negative", ordering: "-5", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clock := newFakeClock(time.Now())
			client := newTestKinesisClient(t, kinesis.NewHandler(kinesis.NewInMemoryBackend().WithClock(clock.Now)))
			ctx := t.Context()

			_, err := client.CreateStream(ctx, &kinesissdk.CreateStreamInput{
				StreamName: aws.String("s"), ShardCount: aws.Int32(1),
			})
			require.NoError(t, err)
			clock.Advance(streamSettleWait)

			first, err := client.PutRecord(ctx, &kinesissdk.PutRecordInput{
				StreamName: aws.String("s"), PartitionKey: aws.String("pk"), Data: []byte("a"),
			})
			require.NoError(t, err)

			ordering := tt.ordering
			if tt.chain {
				ordering = aws.ToString(first.SequenceNumber)
			}

			second, err := client.PutRecord(ctx, &kinesissdk.PutRecordInput{
				StreamName: aws.String("s"), PartitionKey: aws.String("pk"), Data: []byte("b"),
				SequenceNumberForOrdering: aws.String(ordering),
			})
			if tt.wantErr {
				require.Error(t, err)

				var inv *types.InvalidArgumentException
				require.ErrorAs(t, err, &inv)

				return
			}

			require.NoError(t, err)

			a, _ := new(big.Int).SetString(aws.ToString(first.SequenceNumber), 10)
			b, _ := new(big.Int).SetString(aws.ToString(second.SequenceNumber), 10)
			assert.Equal(t, 1, b.Cmp(a), "second sequence number must exceed the first")
		})
	}
}
