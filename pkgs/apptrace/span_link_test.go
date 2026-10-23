package apptrace_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/apptrace"
)

var errDelivery = errors.New("boom")

func TestLinkAndRoot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled bool
		link    bool
		wantLen int
	}{
		{name: "linked child nests under parent", enabled: true, link: true, wantLen: 1},
		{name: "unlinked detached context is untraced", enabled: true, link: false, wantLen: 0},
		{name: "disabled tracer records nothing", enabled: false, link: true, wantLen: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tr := apptrace.New(apptrace.Config{Capacity: 8, Enabled: true})
			rctx, root := apptrace.StartRoot(apptrace.WithTracer(context.Background(), tr), apptrace.ChildInfo{
				Service: "lambda", Operation: "EventSourceMappingInvoke", Path: "arn:src",
			})
			require.NotNil(t, root)
			root.Finish("")

			tr.SetEnabled(tc.enabled)

			base := context.Background()
			if tc.link {
				base = apptrace.Link(rctx, base)
			}

			_, child := apptrace.StartChild(base, apptrace.ChildInfo{Service: "sqs", Operation: "SendMessage"})
			child.FinishErr(errDelivery)

			internal := tr.List(apptrace.Filter{Service: "sqs"}).Items
			require.Len(t, internal, tc.wantLen)

			if tc.wantLen == 1 {
				assert.Equal(t, root.ID, internal[0].ParentID)
				assert.Equal(t, root.TraceID, internal[0].TraceID)
				assert.Equal(t, "DeliveryFailed", internal[0].ErrorCode)
			}
		})
	}
}

func TestStartRootWithoutTracer(t *testing.T) {
	t.Parallel()

	ctx, sp := apptrace.StartRoot(context.Background(), apptrace.ChildInfo{})
	assert.Nil(t, sp)
	assert.Equal(t, context.Background(), ctx)
	sp.FinishErr(errDelivery)
}

func BenchmarkLinkChildUntraced(b *testing.B) {
	base := context.Background()

	b.ReportAllocs()

	for b.Loop() {
		ctx := apptrace.Link(base, base)
		_, sp := apptrace.StartChild(ctx, apptrace.ChildInfo{Service: "sqs"})
		sp.Finish("")
	}
}
