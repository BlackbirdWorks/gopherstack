package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go"

	"github.com/blackbirdworks/gopherstack/internal/awswire/kinesis"
)

const errCodeStreamNotFound = "ResourceNotFoundException"

var errStreamWaitTimeout = errors.New("exceeded max wait time for the stream state")

// awaitStream polls DescribeStream until the stream is ACTIVE (or, with gone, not found) or maxWait passes.
func awaitStream(ctx context.Context, c *kinesis.Client, name string, gone bool, maxWait time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()

	delay := ccStreamPollFloor

	for {
		out, err := c.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(name)})

		switch {
		case gone && err != nil:
			if apiErr, ok := errors.AsType[smithy.APIError](err); ok && apiErr.ErrorCode() == errCodeStreamNotFound {
				return nil
			}

			return err
		case err != nil:
			return err
		case !gone && out.StreamDescription != nil && out.StreamDescription.StreamStatus == "ACTIVE":
			return nil
		}

		t := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			t.Stop()

			return fmt.Errorf("%w: %w", errStreamWaitTimeout, ctx.Err())
		case <-t.C:
		}

		delay = min(delay+delay, ccStreamPollCeil)
	}
}
