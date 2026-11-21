package gcpdatastore_test

import (
	"testing"
	"time"

	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexOperationProgress(t *testing.T) {
	t.Parallel()

	progress := func(t *testing.T, op *longrunningpb.Operation) *adminpb.Progress {
		t.Helper()

		meta := &adminpb.IndexOperationMetadata{}
		require.NoError(t, op.GetMetadata().UnmarshalTo(meta))
		require.NotNil(t, meta.GetProgressEntities())

		return meta.GetProgressEntities()
	}

	tests := []struct {
		name      string
		delay     time.Duration
		estimated int64
		completed int64
		fire      bool
	}{
		{name: "pending", delay: time.Minute, estimated: 2},
		{name: "settled", delay: time.Minute, estimated: 2, completed: 2, fire: true},
		{name: "immediate", estimated: 2, completed: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, ft, ops := newAsyncEnv(t)
			e.handler.Backend.SetIndexBuild(tt.delay, ft.after)
			e.seed(t, "Task", map[string]task{"a": {}, "b": {}})
			e.seed(t, "Other", map[string]task{"c": {}})

			op := e.startIndex(t)
			if tt.fire {
				ft.fire()

				var err error

				op, err = ops.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.GetName()})
				require.NoError(t, err)
			}

			p := progress(t, op)
			assert.Equal(t, tt.estimated, p.GetWorkEstimated())
			assert.Equal(t, tt.completed, p.GetWorkCompleted())
		})
	}
}
