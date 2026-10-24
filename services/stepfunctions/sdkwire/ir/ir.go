// Package ir is the wire model of a service whose SDK client predates
// schema-based serde: shapes, members and operations with the names and
// bindings their protocol puts on the wire.
package ir

// Scalar target names a Member may reference without a Shapes entry.
const (
	String    = "string"
	Boolean   = "boolean"
	Byte      = "byte"
	Short     = "short"
	Integer   = "integer"
	Long      = "long"
	Float     = "float"
	Double    = "double"
	Timestamp = "timestamp"
	Blob      = "blob"
	Document  = "document"
)

// Shape kinds besides the scalars.
const (
	Struct = "struct"
	List   = "list"
	Map    = "map"
	Union  = "union"
)

// Model is one service.
type Model struct {
	Shapes   map[string]*Shape `json:"shapes"`
	Ops      map[string]*Op    `json:"ops"`
	Protocol string            `json:"protocol"`
	Version  string            `json:"version"`
	// Target is the service name the protocol puts on the wire, when it does.
	Target string `json:"target,omitempty"`
}

// Shape is a struct, union, list or map.
type Shape struct {
	Item    *Member  `json:"item,omitempty"`
	Key     *Member  `json:"key,omitempty"`
	Value   *Member  `json:"value,omitempty"`
	Type    string   `json:"type"`
	Members []Member `json:"members,omitempty"`
}

// Member is a struct member, list item, or map key or value.
type Member struct {
	// Name is the Go field name, which Step Functions parameters use.
	Name string `json:"field,omitempty"`
	// Target is a scalar constant or a Shapes key.
	Target string `json:"to"`
	// Ser and De are the wire names when serializing and deserializing, if
	// not Name.
	Ser string `json:"ser,omitempty"`
	De  string `json:"de,omitempty"`
	// Loc is where the member travels when not in the body: "label", "query",
	// "header" or "prefix", named LocName.
	Loc     string `json:"loc,omitempty"`
	LocName string `json:"locName,omitempty"`
	// Flat marks a list or map with no wrapper element.
	Flat bool `json:"flat,omitempty"`
}

// Op is one operation.
type Op struct {
	In     string `json:"in,omitempty"`
	Out    string `json:"out,omitempty"`
	Method string `json:"method,omitempty"`
	URI    string `json:"uri,omitempty"`
	// Token is the idempotency-token member the SDK fills when absent.
	Token string `json:"token,omitempty"`
}
