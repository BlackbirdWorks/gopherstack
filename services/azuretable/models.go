package azuretable

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

// EdmType identifies an entity property's OData EDM (Entity Data Model)
// type. See https://learn.microsoft.com/rest/api/storageservices/payload-format-for-table-service-operations
// for the wire-level annotation scheme this mirrors.
type EdmType string

// Supported EDM property types. EdmString is the default for a bare JSON
// string with no "@odata.type" annotation; EdmInt32 is the default for a
// bare, whole-number JSON number. See entity_ops.go's decodeProperty for the
// exact inference rules (mirroring azure-sdk-for-go/sdk/data/aztables's own
// EDMEntity.UnmarshalJSON, so unmodified SDK round trips match).
const (
	EdmString   EdmType = "Edm.String"
	EdmInt32    EdmType = "Edm.Int32"
	EdmInt64    EdmType = "Edm.Int64"
	EdmDouble   EdmType = "Edm.Double"
	EdmBoolean  EdmType = "Edm.Boolean"
	EdmDateTime EdmType = "Edm.DateTime"
	EdmGUID     EdmType = "Edm.Guid"
	EdmBinary   EdmType = "Edm.Binary"
)

// EntityProperty is a single typed entity property value. Value's concrete
// Go type is determined by Type:
//
//	EdmString    string
//	EdmInt32     int32
//	EdmInt64     int64
//	EdmDouble    float64
//	EdmBoolean   bool
//	EdmDateTime  time.Time
//	EdmGUID      string (canonical UUID string, not validated)
//	EdmBinary    []byte
type EntityProperty struct {
	Value any
	Type  EdmType
}

// entityPropertyWire is EntityProperty's on-disk (persistence snapshot)
// shape. A plain `any` Value field would lose type fidelity on JSON
// round-trip (encoding/json decodes every bare number into float64 and every
// []byte into a base64 string, regardless of the original Go type), so
// MarshalJSON/UnmarshalJSON re-encode Value per Type explicitly instead of
// relying on encoding/json's default interface{} behavior.
type entityPropertyWire struct {
	Value any     `json:"value"`
	Type  EdmType `json:"type"`
}

// MarshalJSON implements json.Marshaler for persistence snapshots (see
// persistence.go). This is independent of the OData wire encoding entity_ops.go
// uses for HTTP responses.
func (p EntityProperty) MarshalJSON() ([]byte, error) {
	v := p.Value

	switch p.Type {
	case EdmBinary:
		if b, ok := p.Value.([]byte); ok {
			v = base64.StdEncoding.EncodeToString(b)
		}
	case EdmDateTime:
		if t, ok := p.Value.(time.Time); ok {
			v = t.UTC().Format(time.RFC3339Nano)
		}
	case EdmInt64:
		if n, ok := p.Value.(int64); ok {
			v = float64(n)
		}
	case EdmString, EdmInt32, EdmDouble, EdmBoolean, EdmGUID:
		// Value is already a JSON-native type (string/float64/bool); no
		// conversion needed.
	}

	return json.Marshal(entityPropertyWire{Type: p.Type, Value: v})
}

// UnmarshalJSON implements json.Unmarshaler for persistence snapshots. See
// MarshalJSON's doc comment for why this can't just rely on encoding/json's
// default `any` decoding.
func (p *EntityProperty) UnmarshalJSON(data []byte) error {
	var wire entityPropertyWire

	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}

	p.Type = wire.Type

	switch wire.Type {
	case EdmInt32:
		f, _ := wire.Value.(float64)
		p.Value = int32(f)
	case EdmInt64:
		f, _ := wire.Value.(float64)
		p.Value = int64(f)
	case EdmDouble:
		f, _ := wire.Value.(float64)
		p.Value = f
	case EdmBoolean:
		b, _ := wire.Value.(bool)
		p.Value = b
	case EdmDateTime:
		s, _ := wire.Value.(string)
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return err
		}
		p.Value = t
	case EdmGUID:
		s, _ := wire.Value.(string)
		p.Value = s
	case EdmBinary:
		s, _ := wire.Value.(string)
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return err
		}
		p.Value = b
	case EdmString:
		fallthrough
	default:
		s, _ := wire.Value.(string)
		p.Value = s
	}

	return nil
}

// TableInfo is a read-only snapshot of a table's metadata, returned by
// StorageBackend.ListTables.
type TableInfo struct {
	Name string
}

// EntityInfo is a read-only snapshot of an entity, returned by the
// StorageBackend entity accessors. Properties excludes the system properties
// (PartitionKey, RowKey, Timestamp), which are surfaced via their own
// fields.
type EntityInfo struct {
	Timestamp    time.Time
	Properties   map[string]EntityProperty
	PartitionKey string
	RowKey       string
	ETag         string
}

// storedEntity is the backend's internal representation of an entity.
type storedEntity struct {
	Timestamp    time.Time
	Properties   map[string]EntityProperty
	PartitionKey string
	RowKey       string
}

// storedTable is the backend's internal representation of a table.
type storedTable struct {
	Entities map[string]*storedEntity
	Name     string
}

// --- OData wire error envelope ---
//
// {"odata.error":{"code":"TableNotFound","message":{"lang":"en-US","value":"..."}}}

type odataErrorMessage struct {
	Lang  string `json:"lang"`
	Value string `json:"value"`
}

type odataErrorDetail struct {
	Message odataErrorMessage `json:"message"`
	Code    string            `json:"code"`
}

type odataErrorEnvelope struct {
	Error odataErrorDetail `json:"odata.error"`
}
