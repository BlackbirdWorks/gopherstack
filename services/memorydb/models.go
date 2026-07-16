package memorydb

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// tagEntry is a key/value tag pair.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// errorResponse is the standard JSON error response body.
type errorResponse = service.JSONErrorResponse

// -- DescribeParameters request/response types --------------------------------
