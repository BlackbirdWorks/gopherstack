package gcpartifactregistry

import (
	"encoding/json"
	"os"
	"sort"
	"time"
)

func sortStrings(s []string) { sort.Strings(s) }

func removeAll(path string) { _ = os.RemoveAll(path) }

func timeFromNanos(n int64) time.Time { return time.Unix(0, n).UTC() }

func marshalJSON(v any) ([]byte, error) { return json.Marshal(v) }
