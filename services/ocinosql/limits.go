package ocinosql

import (
	"encoding/json"
	"strconv"
)

// Service limits from Oracle's "Oracle NoSQL Database Cloud Service Limits"
// (docs.oracle.com/en/cloud/paas/nosql-cloud/fkdyw/).
const (
	maxIndexes  = 5
	maxKeyBytes = 64
	maxRowBytes = 512 * 1024
	maxQueryLen = 10 * 1024
)

func valueBytes(v any) int {
	switch x := v.(type) {
	case string:
		return len(x)
	case json.Number:
		return len(x.String())
	case map[string]any:
		n := 0
		for _, e := range x {
			n += valueBytes(e)
		}

		return n
	case []any:
		n := 0
		for _, e := range x {
			n += valueBytes(e)
		}

		return n
	}

	return 0
}

func checkRowSize(pk []string, row map[string]any) *APIError {
	keyBytes := 0
	for _, c := range pk {
		keyBytes += len(keyPart(row[c]))
	}

	if keyBytes > maxKeyBytes {
		return errLimit("the primary key exceeds " + strconv.Itoa(maxKeyBytes) + " bytes")
	}

	if valueBytes(row) > maxRowBytes {
		return errLimit("the row exceeds " + strconv.Itoa(maxRowBytes) + " bytes")
	}

	return nil
}
