package gcpartifactregistry

import (
	"encoding/json/v2"
	"os"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func sortStrings(s []string) { sort.Strings(s) }

func removeAll(path string) { _ = os.RemoveAll(path) }

func timeFromNanos(n int64) time.Time { return time.Unix(0, n).UTC() }

func marshalJSON(v any) ([]byte, error) { return json.Marshal(v, jsonx.V1()) }
