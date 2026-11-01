package gcpserve

import "net/http"

const (
	// InternalNetworkHeader marks a request as coming from inside the emulated network (VPC, Pub/Sub, Eventarc,
	// Cloud Tasks, Cloud Scheduler). Every caller of the emulator is on the loopback interface, so the source
	// network cannot be told apart otherwise.
	InternalNetworkHeader = "X-Gopherstack-Network"
	// InternalNetwork is the header value that marks a request internal.
	InternalNetwork = "internal"
)

// MarkInternal flags req as originating inside the emulated network.
func MarkInternal(req *http.Request) { req.Header.Set(InternalNetworkHeader, InternalNetwork) }

// IsInternal reports whether r carries the internal-network marker.
func IsInternal(r *http.Request) bool { return r.Header.Get(InternalNetworkHeader) == InternalNetwork }
