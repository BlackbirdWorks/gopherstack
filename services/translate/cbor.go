package translate

import (
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// cborBridge serves the Smithy RPCv2 CBOR protocol the SDK now prefers.
var cborBridge = service.RPCv2CBORBridge{ //nolint:gochecknoglobals // immutable wire-shape table
	Name:       "AWSShineFrontendService_20170701",
	BinaryKeys: nameSet("Content File"),
	TimeKeys: nameSet("CreatedAt EndTime LastUpdatedAt LatestUpdateAttemptAt " +
		"SubmittedAfterTime SubmittedBeforeTime SubmittedTime"),
}

func nameSet(names string) map[string]bool {
	return maps.Collect(func(yield func(string, bool) bool) {
		for _, n := range slices.Collect(strings.FieldsSeq(names)) {
			if !yield(n, true) {
				return
			}
		}
	})
}
