package azureappconfig

import (
	"sort"
	"strconv"
	"strings"
)

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func unquoteETag(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "W/")

	return strings.Trim(v, `"`)
}

func sortStrings(s []string) { sort.Strings(s) }
