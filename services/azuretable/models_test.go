package azuretable_test

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

// TestEntityProperty_JSONRoundTrip covers every EDM type's persistence
// (encoding/json Marshal/Unmarshal) round trip, including the exact values
// that would silently corrupt through a bare-float64 Int64 encoding:
// 2^53+1 is the smallest positive integer a float64 cannot represent
// exactly, so it is the correct demonstration case (unlike math.MaxInt64,
// which survives a naive float64 round trip by coincidence -- both are
// covered here regardless).
func TestEntityProperty_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2024, 6, 15, 12, 30, 45, 123456700, time.UTC)

	tests := []struct {
		name string
		prop azuretable.EntityProperty
	}{
		{name: "string", prop: azuretable.EntityProperty{Type: azuretable.EdmString, Value: "hello"}},
		{name: "string_empty", prop: azuretable.EntityProperty{Type: azuretable.EdmString, Value: ""}},
		{name: "int32", prop: azuretable.EntityProperty{Type: azuretable.EdmInt32, Value: int32(42)}},
		{name: "int32_negative", prop: azuretable.EntityProperty{Type: azuretable.EdmInt32, Value: int32(-42)}},
		{name: "int32_max", prop: azuretable.EntityProperty{Type: azuretable.EdmInt32, Value: int32(math.MaxInt32)}},
		{name: "int32_min", prop: azuretable.EntityProperty{Type: azuretable.EdmInt32, Value: int32(math.MinInt32)}},
		{
			name: "int64_beyond_float64_mantissa",
			prop: azuretable.EntityProperty{Type: azuretable.EdmInt64, Value: int64(1<<53 + 1)},
		},
		{name: "int64_max", prop: azuretable.EntityProperty{Type: azuretable.EdmInt64, Value: int64(math.MaxInt64)}},
		{name: "int64_min", prop: azuretable.EntityProperty{Type: azuretable.EdmInt64, Value: int64(math.MinInt64)}},
		{name: "int64_negative_one", prop: azuretable.EntityProperty{Type: azuretable.EdmInt64, Value: int64(-1)}},
		{name: "int64_zero", prop: azuretable.EntityProperty{Type: azuretable.EdmInt64, Value: int64(0)}},
		{name: "double", prop: azuretable.EntityProperty{Type: azuretable.EdmDouble, Value: 3.14159265358979}},
		{name: "double_zero", prop: azuretable.EntityProperty{Type: azuretable.EdmDouble, Value: 0.0}},
		{name: "double_negative", prop: azuretable.EntityProperty{Type: azuretable.EdmDouble, Value: -2.5}},
		{name: "boolean_true", prop: azuretable.EntityProperty{Type: azuretable.EdmBoolean, Value: true}},
		{name: "boolean_false", prop: azuretable.EntityProperty{Type: azuretable.EdmBoolean, Value: false}},
		{name: "datetime", prop: azuretable.EntityProperty{Type: azuretable.EdmDateTime, Value: fixedTime}},
		{
			name: "guid",
			prop: azuretable.EntityProperty{Type: azuretable.EdmGUID, Value: "550e8400-e29b-41d4-a716-446655440000"},
		},
		{name: "binary", prop: azuretable.EntityProperty{Type: azuretable.EdmBinary, Value: []byte("gopherstack")}},
		{name: "binary_empty", prop: azuretable.EntityProperty{Type: azuretable.EdmBinary, Value: []byte{}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data, err := json.Marshal(tt.prop)
			require.NoError(t, err, tt.name)

			var got azuretable.EntityProperty
			require.NoError(t, json.Unmarshal(data, &got), tt.name)

			assert.Equal(t, tt.prop.Type, got.Type, tt.name)

			if tt.prop.Type == azuretable.EdmDateTime {
				wantTime, ok := tt.prop.Value.(time.Time)
				require.True(t, ok, tt.name)
				gotTime, ok := got.Value.(time.Time)
				require.True(t, ok, tt.name)
				assert.True(t, wantTime.Equal(gotTime), "%s: want %v got %v", tt.name, wantTime, gotTime)
				assert.Equal(t, wantTime.UnixNano(), gotTime.UnixNano(), tt.name)

				return
			}

			assert.Equal(t, tt.prop.Value, got.Value, tt.name)
		})
	}
}

// TestEntityProperty_MarshalJSON_TypeMismatchErrors covers the case a
// property's declared Type disagrees with its Go-typed Value (a
// construction bug elsewhere in the package): Marshal must return an error,
// never silently drop or mis-encode the value.
func TestEntityProperty_MarshalJSON_TypeMismatchErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		prop azuretable.EntityProperty
	}{
		{name: "binary_wrong_type", prop: azuretable.EntityProperty{Type: azuretable.EdmBinary, Value: "not bytes"}},
		{
			name: "datetime_wrong_type",
			prop: azuretable.EntityProperty{Type: azuretable.EdmDateTime, Value: "not a time"},
		},
		{name: "int64_wrong_type", prop: azuretable.EntityProperty{Type: azuretable.EdmInt64, Value: int32(1)}},
		{name: "int32_wrong_type", prop: azuretable.EntityProperty{Type: azuretable.EdmInt32, Value: int64(1)}},
		{name: "double_wrong_type", prop: azuretable.EntityProperty{Type: azuretable.EdmDouble, Value: "1.5"}},
		{name: "boolean_wrong_type", prop: azuretable.EntityProperty{Type: azuretable.EdmBoolean, Value: "true"}},
		{name: "guid_wrong_type", prop: azuretable.EntityProperty{Type: azuretable.EdmGUID, Value: 123}},
		{name: "string_wrong_type", prop: azuretable.EntityProperty{Type: azuretable.EdmString, Value: 123}},
		{name: "unknown_type", prop: azuretable.EntityProperty{Type: "Edm.Bogus", Value: "x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := json.Marshal(tt.prop)
			require.Error(t, err, tt.name)
		})
	}
}

// TestEntityProperty_UnmarshalJSON_MalformedValueErrors covers a snapshot
// whose per-type wire value doesn't decode cleanly: Unmarshal must return an
// error, never silently zero the value.
func TestEntityProperty_UnmarshalJSON_MalformedValueErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		json string
	}{
		{name: "int64_not_numeric_string", json: `{"type":"Edm.Int64","value":"not-a-number"}`},
		{name: "int64_wire_is_number_not_string", json: `{"type":"Edm.Int64","value":123}`},
		{name: "int32_wire_is_string_not_number", json: `{"type":"Edm.Int32","value":"123"}`},
		{name: "double_wire_is_string", json: `{"type":"Edm.Double","value":"1.5"}`},
		{name: "boolean_wire_is_string", json: `{"type":"Edm.Boolean","value":"true"}`},
		{name: "datetime_malformed", json: `{"type":"Edm.DateTime","value":"not-a-date"}`},
		{name: "datetime_wire_is_number", json: `{"type":"Edm.DateTime","value":123}`},
		{name: "guid_wire_is_number", json: `{"type":"Edm.Guid","value":123}`},
		{name: "binary_invalid_base64", json: `{"type":"Edm.Binary","value":"not base64!!"}`},
		{name: "binary_wire_is_number", json: `{"type":"Edm.Binary","value":123}`},
		{name: "string_wire_is_number", json: `{"type":"Edm.String","value":123}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var p azuretable.EntityProperty
			err := json.Unmarshal([]byte(tt.json), &p)
			require.Error(t, err, tt.name)
		})
	}
}
