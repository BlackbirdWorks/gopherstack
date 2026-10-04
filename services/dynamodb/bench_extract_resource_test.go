package dynamodb_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// BenchmarkExtractResource measures ExtractResource, called on every request.
func BenchmarkExtractResource(b *testing.B) {
	db := dynamodb.NewInMemoryDB()
	h := dynamodb.NewHandler(db)
	e := echo.New()

	item := make(map[string]any, 20)
	for i := range 20 {
		item[fmt.Sprintf("attr%d", i)] = map[string]any{"S": fmt.Sprintf("value-%d", i)}
	}

	body := map[string]any{
		"TableName": "BenchTable",
		"Item":      item,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		b.Fatalf("marshal request body: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		c := e.NewContext(req, httptest.NewRecorder())

		if got := h.ExtractResource(c); got != "BenchTable" {
			b.Fatalf("ExtractResource: got %q", got)
		}
	}
}
