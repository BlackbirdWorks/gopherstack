package apigateway

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegexpCache_BoundedEviction(t *testing.T) {
	t.Parallel()

	const maxEntries = 4
	c := newRegexpCache(maxEntries)

	// Insert more distinct patterns than the cap.
	for i := range 20 {
		c.put(string(rune('a'+i)), nil)
	}

	c.mu.Lock()
	size := c.order.Len()
	mapSize := len(c.entries)
	c.mu.Unlock()

	assert.LessOrEqual(t, size, maxEntries, "LRU list must stay within the cap")
	assert.Equal(t, size, mapSize, "list and index must stay in sync")
}

func TestRegexpCache_LRUOrder(t *testing.T) {
	t.Parallel()

	c := newRegexpCache(2)
	c.put("a", nil)
	c.put("b", nil)

	// Touch "a" so "b" becomes the least-recently-used entry.
	_, ok := c.get("a")
	require.True(t, ok)

	c.put("c", nil) // should evict "b"

	_, hasA := c.get("a")
	_, hasB := c.get("b")
	_, hasC := c.get("c")
	assert.True(t, hasA, "recently used entry must survive")
	assert.False(t, hasB, "least-recently-used entry must be evicted")
	assert.True(t, hasC, "newest entry must be present")
}

func TestDispatchTable_BuiltOnce(t *testing.T) {
	t.Parallel()

	h := NewHandler(NewInMemoryBackend())
	first := h.dispatchTable()
	second := h.dispatchTable()

	// The same map instance must be returned (built exactly once).
	assert.Equal(t, reflect.ValueOf(first).Pointer(), reflect.ValueOf(second).Pointer(),
		"dispatch table must be cached, not rebuilt per call")
	assert.NotEmpty(t, first)
}

func TestTokenBucket_RefillAndConsume(t *testing.T) {
	t.Parallel()

	now := time.Unix(0, 0)
	tb := newTokenBucket(1, 2, now) // rate 1/s, burst 2

	assert.True(t, tb.allow(now), "first token available")
	assert.True(t, tb.allow(now), "second token available")
	assert.False(t, tb.allow(now), "burst exhausted")

	// One second later, one token has refilled.
	assert.True(t, tb.allow(now.Add(time.Second)), "token refilled after 1s")
	assert.False(t, tb.allow(now.Add(time.Second)), "only one token refilled")
}

func TestValidateJSONAgainstSchema(t *testing.T) {
	t.Parallel()

	const schema = `{
		"type": "object",
		"required": ["id", "tags"],
		"properties": {
			"id": {"type": "integer", "minimum": 1},
			"name": {"type": "string", "minLength": 2, "maxLength": 5},
			"status": {"enum": ["on", "off"]},
			"tags": {"type": "array", "items": {"type": "string"}, "minItems": 1}
		},
		"additionalProperties": false
	}`

	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "valid", body: `{"id":3,"name":"abc","status":"on","tags":["x"]}`, wantErr: false},
		{name: "missing_required", body: `{"id":3}`, wantErr: true},
		{name: "wrong_type", body: `{"id":"x","tags":["a"]}`, wantErr: true},
		{name: "non_integer", body: `{"id":1.5,"tags":["a"]}`, wantErr: true},
		{name: "below_minimum", body: `{"id":0,"tags":["a"]}`, wantErr: true},
		{name: "string_too_short", body: `{"id":1,"name":"a","tags":["a"]}`, wantErr: true},
		{name: "string_too_long", body: `{"id":1,"name":"abcdef","tags":["a"]}`, wantErr: true},
		{name: "enum_violation", body: `{"id":1,"status":"maybe","tags":["a"]}`, wantErr: true},
		{name: "empty_array", body: `{"id":1,"tags":[]}`, wantErr: true},
		{name: "array_item_wrong_type", body: `{"id":1,"tags":[5]}`, wantErr: true},
		{name: "additional_property", body: `{"id":1,"tags":["a"],"extra":true}`, wantErr: true},
		{name: "malformed_json", body: `{"id":`, wantErr: true},
		{name: "empty_schema_allows_anything", body: `{"anything":true}`, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := schema
			if tt.name == "empty_schema_allows_anything" {
				s = ""
			}
			err := validateJSONAgainstSchema([]byte(tt.body), s)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateJSONAgainstSchema_LocalRef(t *testing.T) {
	t.Parallel()

	const schema = `{
		"type": "object",
		"required": ["item"],
		"properties": {"item": {"$ref": "#/definitions/Item"}},
		"definitions": {"Item": {"type": "object", "required": ["sku"], "properties": {"sku": {"type": "string"}}}}
	}`

	require.NoError(t, validateJSONAgainstSchema([]byte(`{"item":{"sku":"abc"}}`), schema))
	require.Error(t, validateJSONAgainstSchema([]byte(`{"item":{}}`), schema))
}
