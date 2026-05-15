package asl_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// TestMapItemContext verifies that $$.Map.Item.Index and $$.Map.Item.Value
// are accessible inside a Map state iterator via Parameters references.
func TestMapItemContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		def       string
		input     string
		wantItems []any
	}{
		{
			name: "map_item_index_in_parameters",
			def: `{
				"StartAt": "Map",
				"States": {
					"Map": {
						"Type": "Map",
						"ItemsPath": "$.items",
						"Iterator": {
							"StartAt": "ExtractIndex",
							"States": {
								"ExtractIndex": {
									"Type": "Pass",
									"Parameters": {
										"idx.$": "$$.Map.Item.Index",
										"val.$": "$$.Map.Item.Value"
									},
									"End": true
								}
							}
						},
						"End": true
					}
				}
			}`,
			input: `{"items":["a","b","c"]}`,
			wantItems: []any{
				map[string]any{"idx": float64(0), "val": "a"},
				map[string]any{"idx": float64(1), "val": "b"},
				map[string]any{"idx": float64(2), "val": "c"},
			},
		},
		{
			name: "map_item_value_numeric",
			def: `{
				"StartAt": "Map",
				"States": {
					"Map": {
						"Type": "Map",
						"ItemsPath": "$.nums",
						"Iterator": {
							"StartAt": "Double",
							"States": {
								"Double": {
									"Type": "Pass",
									"Parameters": {
										"original.$": "$$.Map.Item.Value",
										"position.$": "$$.Map.Item.Index"
									},
									"End": true
								}
							}
						},
						"End": true
					}
				}
			}`,
			input: `{"nums":[10,20,30]}`,
			wantItems: []any{
				map[string]any{"original": float64(10), "position": float64(0)},
				map[string]any{"original": float64(20), "position": float64(1)},
				map[string]any{"original": float64(30), "position": float64(2)},
			},
		},
		{
			name: "map_item_value_object",
			def: `{
				"StartAt": "Map",
				"States": {
					"Map": {
						"Type": "Map",
						"ItemsPath": "$.records",
						"Iterator": {
							"StartAt": "Wrap",
							"States": {
								"Wrap": {
									"Type": "Pass",
									"Parameters": {
										"seq.$": "$$.Map.Item.Index",
										"item.$": "$$.Map.Item.Value"
									},
									"End": true
								}
							}
						},
						"End": true
					}
				}
			}`,
			input: `{"records":[{"name":"alice"},{"name":"bob"}]}`,
			wantItems: []any{
				map[string]any{
					"seq":  float64(0),
					"item": map[string]any{"name": "alice"},
				},
				map[string]any{
					"seq":  float64(1),
					"item": map[string]any{"name": "bob"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)

			exec := asl.NewExecutor(sm, nil, nil)
			result, execErr := exec.Execute(context.Background(), "test-exec", tt.input)
			require.NoError(t, execErr)
			require.Empty(t, result.Error)

			got, ok := result.Output.([]any)
			require.True(t, ok, "expected array output, got %T", result.Output)
			require.Len(t, got, len(tt.wantItems))

			for i, wantItem := range tt.wantItems {
				assert.Equal(t, wantItem, got[i], "item[%d] mismatch", i)
			}
		})
	}
}

// TestMapItemContext_ItemProcessor verifies that the context works with
// the ItemProcessor field (newer distributed map syntax).
func TestMapItemContext_ItemProcessor(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Map",
		"States": {
			"Map": {
				"Type": "Map",
				"ItemsPath": "$.data",
				"ItemProcessor": {
					"StartAt": "Tag",
					"States": {
						"Tag": {
							"Type": "Pass",
							"Parameters": {
								"i.$": "$$.Map.Item.Index",
								"v.$": "$$.Map.Item.Value"
							},
							"End": true
						}
					}
				},
				"End": true
			}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	result, execErr := exec.Execute(context.Background(), "test-exec", `{"data":["x","y"]}`)
	require.NoError(t, execErr)
	require.Empty(t, result.Error)

	got, ok := result.Output.([]any)
	require.True(t, ok)
	require.Len(t, got, 2)

	idx0, _ := got[0].(map[string]any)["i"].(float64)
	assert.Equal(t, 0, int(idx0))
	assert.Equal(t, "x", got[0].(map[string]any)["v"])
	idx1, _ := got[1].(map[string]any)["i"].(float64)
	assert.Equal(t, 1, int(idx1))
	assert.Equal(t, "y", got[1].(map[string]any)["v"])
}

// TestNewMapItemExecutor verifies the exported constructor for map item executors.
func TestNewMapItemExecutor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		itemValue any
		wantValue any
		name      string
		wantIndex float64
		idx       int
	}{
		{
			name:      "index_zero",
			idx:       0,
			itemValue: "first",
			wantIndex: 0,
			wantValue: "first",
		},
		{
			name:      "index_ten",
			idx:       10,
			itemValue: map[string]any{"key": "val"},
			wantIndex: 10,
			wantValue: map[string]any{"key": "val"},
		},
	}

	sm := &asl.StateMachine{
		StartAt: "S",
		States: map[string]*asl.State{
			"S": {Type: "Pass", End: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parent := asl.NewExecutor(sm, nil, nil)
			mapExec := parent.NewMapItemExecutor(sm, tt.idx, tt.itemValue)
			require.NotNil(t, mapExec)

			ctx := mapExec.BuildContextObject()
			mapCtx, ok := ctx["Map"].(map[string]any)
			require.True(t, ok, "expected Map key in context, got %v", ctx)

			item, ok := mapCtx["Item"].(map[string]any)
			require.True(t, ok)

			gotIndex, _ := item["Index"].(float64)
			assert.Equal(t, int(tt.wantIndex), int(gotIndex))
			assert.Equal(t, tt.wantValue, item["Value"])
		})
	}
}

// TestMapItemContext_NestedMap verifies that nested Map states correctly inject
// the inner map's item context (overriding the outer one).
func TestMapItemContext_NestedMap(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "OuterMap",
		"States": {
			"OuterMap": {
				"Type": "Map",
				"ItemsPath": "$.groups",
				"Iterator": {
					"StartAt": "InnerMap",
					"States": {
						"InnerMap": {
							"Type": "Map",
							"ItemsPath": "$.members",
							"Iterator": {
								"StartAt": "Tag",
								"States": {
									"Tag": {
										"Type": "Pass",
										"Parameters": {
											"innerIdx.$": "$$.Map.Item.Index",
											"member.$": "$$.Map.Item.Value"
										},
										"End": true
									}
								}
							},
							"End": true
						}
					}
				},
				"End": true
			}
		}
	}`

	input := `{"groups":[
		{"members":["alice","bob"]},
		{"members":["carol"]}
	]}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	result, execErr := exec.Execute(context.Background(), "nested-exec", input)
	require.NoError(t, execErr)
	require.Empty(t, result.Error)

	outer, ok := result.Output.([]any)
	require.True(t, ok)
	require.Len(t, outer, 2)

	// First group: [alice, bob].
	group0, ok := outer[0].([]any)
	require.True(t, ok)
	require.Len(t, group0, 2)
	innerIdx00, _ := group0[0].(map[string]any)["innerIdx"].(float64)
	assert.Equal(t, 0, int(innerIdx00))
	assert.Equal(t, "alice", group0[0].(map[string]any)["member"])
	innerIdx01, _ := group0[1].(map[string]any)["innerIdx"].(float64)
	assert.Equal(t, 1, int(innerIdx01))
	assert.Equal(t, "bob", group0[1].(map[string]any)["member"])

	// Second group: [carol].
	group1, ok := outer[1].([]any)
	require.True(t, ok)
	require.Len(t, group1, 1)
	innerIdx10, _ := group1[0].(map[string]any)["innerIdx"].(float64)
	assert.Equal(t, 0, int(innerIdx10))
	assert.Equal(t, "carol", group1[0].(map[string]any)["member"])
}

// TestMapItemContext_EmptyArray verifies that an empty input array produces
// an empty output without injecting any context.
func TestMapItemContext_EmptyArray(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Map",
		"States": {
			"Map": {
				"Type": "Map",
				"ItemsPath": "$.items",
				"Iterator": {
					"StartAt": "S",
					"States": {
						"S": {
							"Type": "Pass",
							"Parameters": {
								"idx.$": "$$.Map.Item.Index"
							},
							"End": true
						}
					}
				},
				"End": true
			}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)
	result, execErr := exec.Execute(context.Background(), "empty-exec", `{"items":[]}`)
	require.NoError(t, execErr)
	require.Empty(t, result.Error)

	got, ok := result.Output.([]any)
	require.True(t, ok)
	assert.Empty(t, got)
}
