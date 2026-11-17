package cloudformation_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestStackInstancesErrorCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		name string
		want string
	}{
		{name: "set_not_found", err: cloudformation.ErrStackSetNotFound, want: "StackSetNotFoundException"},
		{name: "in_progress", err: cloudformation.ErrOperationInProgress, want: "OperationInProgressException"},
		{
			name: "operation_id_exists", err: cloudformation.ErrOperationIDAlreadyExists,
			want: "OperationIdAlreadyExistsException",
		},
		{
			name: "instance_not_found", err: fmt.Errorf("%w: a/b", cloudformation.ErrStackInstanceNotFound),
			want: "StackInstanceNotFoundException",
		},
		{name: "invalid_operation", err: cloudformation.ErrInvalidOperation, want: "InvalidOperationException"},
		{name: "other", err: cloudformation.ErrInvalidParameterOverrides, want: "ValidationError"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, cloudformation.StackInstancesErrorCode(tt.err))
		})
	}
}
