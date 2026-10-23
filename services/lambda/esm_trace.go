package lambda

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/apptrace"
)

// startESMSpan opens a root span for one event-source-mapping delivery; these
// polls have no originating request, so each starts its own trace.
func startESMSpan(ctx context.Context, functionARN, sourceARN string) (context.Context, *apptrace.Span) {
	return apptrace.StartRoot(ctx, apptrace.ChildInfo{
		Service:   "lambda",
		Operation: "EventSourceMappingInvoke",
		Resource:  functionARN,
		Path:      sourceARN,
		Region:    arnRegionOf(functionARN),
	})
}
