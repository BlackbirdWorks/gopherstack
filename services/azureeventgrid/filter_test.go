package azureeventgrid_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eg "github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

const filterEvent = `{
  "id": "1", "subject": "/blobServices/default/containers/Photos/blobs/cat.PNG",
  "eventType": "Microsoft.Storage.BlobCreated", "dataVersion": "1",
  "data": {"size": 42, "tier": "Hot", "ok": true, "tags": ["a", "b"], "nested": {"n": 7}, "none": null}
}`

func decode(t *testing.T, raw string) map[string]any {
	t.Helper()

	var m map[string]any

	require.NoError(t, json.Unmarshal([]byte(raw), &m))

	return m
}

func af(op, key string, values ...any) eg.AdvancedFilter {
	return eg.AdvancedFilter{OperatorType: op, Key: key, Values: values}
}

func val(op, key string, v any) eg.AdvancedFilter {
	return eg.AdvancedFilter{OperatorType: op, Key: key, Value: v}
}

func TestFilterMatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter eg.Filter
		want   bool
	}{
		{name: "empty", filter: eg.Filter{}, want: true},
		{
			name:   "type hit",
			filter: eg.Filter{IncludedEventTypes: []string{"x", "Microsoft.Storage.BlobCreated"}},
			want:   true,
		},
		{name: "type miss", filter: eg.Filter{IncludedEventTypes: []string{"other"}}, want: false},
		{name: "type all", filter: eg.Filter{IncludedEventTypes: []string{"all"}}, want: true},
		{name: "prefix ci", filter: eg.Filter{SubjectBeginsWith: "/BLOBSERVICES"}, want: true},
		{
			name:   "prefix cs miss",
			filter: eg.Filter{SubjectBeginsWith: "/BLOBSERVICES", IsSubjectCaseSensitive: true},
			want:   false,
		},
		{name: "suffix ci", filter: eg.Filter{SubjectEndsWith: ".png"}, want: true},
		{
			name:   "suffix cs miss",
			filter: eg.Filter{SubjectEndsWith: ".png", IsSubjectCaseSensitive: true},
			want:   false,
		},
		{
			name: "string in",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("StringIn", "data.tier", "hot", "cool")},
			},
			want: true,
		},
		{
			name: "string in miss",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("StringIn", "data.tier", "cool")},
			},
			want: false,
		},
		{
			name: "string not in",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("StringNotIn", "data.tier", "cool")},
			},
			want: true,
		},
		{
			name: "string contains",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("StringContains", "subject", "photos")},
			},
			want: true,
		},
		{
			name: "string not contains",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("StringNotContains", "subject", "photos")},
			},
			want: false,
		},
		{
			name: "string begins",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{
					af("StringBeginsWith", "eventType", "microsoft.storage"),
				},
			},
			want: true,
		},
		{
			name: "string not begins",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{
					af("StringNotBeginsWith", "eventType", "microsoft.storage"),
				},
			},
			want: false,
		},
		{
			name: "string ends",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("StringEndsWith", "eventType", "created")},
			},
			want: true,
		},
		{
			name: "string not ends",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("StringNotEndsWith", "eventType", "created")},
			},
			want: false,
		},
		{
			name: "number in",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("NumberIn", "data.size", 1.0, 42.0)},
			},
			want: true,
		},
		{
			name: "number not in",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("NumberNotIn", "data.size", 42.0)},
			},
			want: false,
		},
		{
			name: "number lt",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{
					val("NumberLessThan", "data.size", 50.0),
				},
			},
			want: true,
		},
		{
			name: "number lt miss",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{
					val("NumberLessThan", "data.size", 42.0),
				},
			},
			want: false,
		},
		{
			name: "number lte",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{
					val("NumberLessThanOrEquals", "data.size", 42.0),
				},
			},
			want: true,
		},
		{
			name: "number gt",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{
					val("NumberGreaterThan", "data.size", 41.0),
				},
			},
			want: true,
		},
		{
			name: "number gte miss",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{
					val("NumberGreaterThanOrEquals", "data.size", 43.0),
				},
			},
			want: false,
		},
		{
			name: "number range",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("NumberInRange", "data.size", []any{40.0, 50.0})},
			},
			want: true,
		},
		{
			name: "number not in range",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{
					af("NumberNotInRange", "data.size", []any{40.0, 50.0}),
				},
			},
			want: false,
		},
		{
			name: "bool equals",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{
					val("BoolEquals", "data.ok", true),
				},
			},
			want: true,
		},
		{
			name: "bool equals miss",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{
					val("BoolEquals", "data.ok", false),
				},
			},
			want: false,
		},
		{
			name: "is null or undefined missing",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("IsNullOrUndefined", "data.absent")},
			},
			want: true,
		},
		{
			name: "is null or undefined null",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("IsNullOrUndefined", "data.none")},
			},
			want: true,
		},
		{
			name: "is null or undefined present",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("IsNullOrUndefined", "data.size")},
			},
			want: false,
		},
		{
			name: "is not null",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("IsNotNull", "data.size")},
			},
			want: true,
		},
		{
			name: "missing key positive",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("StringIn", "data.absent", "x")},
			},
			want: false,
		},
		{
			name: "missing key negative",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("StringNotIn", "data.absent", "x")},
			},
			want: true,
		},
		{
			name: "nested slash path",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("NumberIn", "data/nested/n", 7.0)},
			},
			want: true,
		},
		{
			name: "array off",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("StringIn", "data.tags", "a")},
			},
			want: false,
		},
		{
			name: "array on",
			filter: eg.Filter{
				EnableAdvancedFilteringOnArrays: true,
				AdvancedFilters:                 []eg.AdvancedFilter{af("StringIn", "data.tags", "a")},
			},
			want: true,
		},
		{
			name: "array on not in",
			filter: eg.Filter{
				EnableAdvancedFilteringOnArrays: true,
				AdvancedFilters:                 []eg.AdvancedFilter{af("StringNotIn", "data.tags", "b")},
			},
			want: false,
		},
		{
			name: "all must hold",
			filter: eg.Filter{
				SubjectEndsWith: ".png",
				AdvancedFilters: []eg.AdvancedFilter{af("StringIn", "data.tier", "cool")},
			},
			want: false,
		},
		{
			name: "type mismatch",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("NumberIn", "data.tier", 1.0)},
			},
			want: false,
		},
	}

	ev := decode(t, filterEvent)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.filter.Matches(ev))
		})
	}
}

func TestFilterCloudEventAttributes(t *testing.T) {
	t.Parallel()

	ev := decode(t, `{"specversion":"1.0","id":"1","source":"/s","type":"my.type","subject":"a/b","data":{"x":1}}`)

	tests := []struct {
		name   string
		filter eg.Filter
		want   bool
	}{
		{
			name:   "type via eventType",
			filter: eg.Filter{IncludedEventTypes: []string{"my.type"}},
			want:   true,
		},
		{name: "type miss", filter: eg.Filter{IncludedEventTypes: []string{"nope"}}, want: false},
		{
			name: "source key",
			filter: eg.Filter{
				AdvancedFilters: []eg.AdvancedFilter{af("StringIn", "source", "/s")},
			},
			want: true,
		},
		{name: "subject", filter: eg.Filter{SubjectBeginsWith: "a/"}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.filter.Matches(ev))
		})
	}
}
