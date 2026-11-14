package cloudwatchlogs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubscriptionDeniedError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want string
	}{
		{
			"lambda", "arn:aws:lambda:us-east-1:123456789012:function:f",
			"Could not execute the lambda function. Make sure you have given CloudWatch Logs permission " +
				"to execute your function.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.ErrorContains(t, subscriptionDeniedError(tt.arn), tt.want)
		})
	}
}
