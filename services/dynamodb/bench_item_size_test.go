package dynamodb_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func BenchmarkCalculateItemSize(b *testing.B) {
	item := map[string]any{
		"pk":    map[string]any{"S": "cust#1"},
		"sk":    map[string]any{"S": "order#00001"},
		"val":   map[string]any{"N": "123"},
		"gsipk": map[string]any{"S": "g1"},
		"data":  map[string]any{"S": "payload-payload-payload-payload"},
		"flag":  map[string]any{"BOOL": true},
	}

	b.ReportAllocs()

	for b.Loop() {
		_, _ = dynamodb.CalculateItemSize(item)
	}
}
