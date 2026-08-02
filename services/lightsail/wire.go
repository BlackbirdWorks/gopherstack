package lightsail

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// epochPtr converts t to an epoch-seconds *float64 for the wire (nil if t
// is the zero value), or nil. AWS JSON-protocol timestamps are
// epoch-seconds numbers, never RFC3339 strings -- see
// pkgs/awstime.Epoch's doc comment and parity-principles.md's known bug
// class, and services/directconnect's identical epochPtr.
func epochPtr(t time.Time) *float64 {
	if t.IsZero() {
		return nil
	}

	v := awstime.Epoch(t)

	return &v
}

// tagWire mirrors types.Tag.
type tagWire struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// tagsToWire converts a map[string]string to a sorted wire tag list
// (nil-safe -- returns nil for an empty map so omitempty drops it).
func tagsToWire(m map[string]string) []tagWire {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sortStrings(keys)

	out := make([]tagWire, 0, len(m))
	for _, k := range keys {
		out = append(out, tagWire{Key: k, Value: m[k]})
	}

	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// tagsFromWire converts a wire tag list to a map[string]string (nil-safe).
func tagsFromWire(in []tagWire) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for _, t := range in {
		out[t.Key] = t.Value
	}

	return out
}

// mapFromTags converts a *tags.Tags to its wire tag list.
func mapFromTags(t *tags.Tags) []tagWire {
	if t == nil {
		return nil
	}

	return tagsToWire(t.Clone())
}

// resourceLocationWire mirrors types.ResourceLocation.
type resourceLocationWire struct {
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	RegionName       string `json:"regionName,omitempty"`
}

func locationToWire(l ResourceLocation) *resourceLocationWire {
	return &resourceLocationWire{AvailabilityZone: l.AvailabilityZone, RegionName: l.RegionName}
}

// operationWire mirrors types.Operation.
type operationWire struct {
	CreatedAt        *float64              `json:"createdAt,omitempty"`
	Location         *resourceLocationWire `json:"location,omitempty"`
	StatusChangedAt  *float64              `json:"statusChangedAt,omitempty"`
	ErrorCode        string                `json:"errorCode,omitempty"`
	ErrorDetails     string                `json:"errorDetails,omitempty"`
	ID               string                `json:"id,omitempty"`
	OperationDetails string                `json:"operationDetails,omitempty"`
	OperationType    string                `json:"operationType,omitempty"`
	ResourceName     string                `json:"resourceName,omitempty"`
	ResourceType     string                `json:"resourceType,omitempty"`
	Status           string                `json:"status,omitempty"`
	IsTerminal       bool                  `json:"isTerminal,omitempty"`
}

func operationToWire(o *Operation) operationWire {
	return operationWire{
		CreatedAt: epochPtr(o.CreatedAt), ErrorCode: o.ErrorCode, ErrorDetails: o.ErrorDetails,
		ID: o.ID, IsTerminal: o.IsTerminal, Location: locationToWire(o.Location),
		OperationDetails: o.Details, OperationType: o.Type, ResourceName: o.ResourceName,
		ResourceType: o.ResourceType, Status: o.Status, StatusChangedAt: epochPtr(o.StatusChangedAt),
	}
}

func operationsToWire(ops []Operation) []operationWire {
	out := make([]operationWire, len(ops))
	for i := range ops {
		out[i] = operationToWire(&ops[i])
	}

	return out
}

// operationsEnvelope is the {"operations": [...]} shape almost every
// mutating op in this service returns.
type operationsEnvelope struct {
	Operations []operationWire `json:"operations,omitempty"`
}

// operationEnvelope is the {"operation": {...}} shape a handful of ops
// (single-Operation returns) use.
type operationEnvelope struct {
	Operation *operationWire `json:"operation,omitempty"`
}

func opsEnvelope(ops []Operation) operationsEnvelope {
	return operationsEnvelope{Operations: operationsToWire(ops)}
}

func opEnvelope(op *Operation) operationEnvelope {
	w := operationToWire(op)

	return operationEnvelope{Operation: &w}
}

// pageTokenRequest is embedded by every List/Get-plural op's request shape
// that takes only a PageToken.
type pageTokenRequest struct {
	PageToken string `json:"pageToken,omitempty"`
}
