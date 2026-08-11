package codebuild

import (
	"encoding/json"
	"sort"
)

// wireTags is the wire representation of CodeBuild's tags field: a JSON
// array of {"key":...,"value":...} objects, not a JSON object (verified
// against codebuild@v1.72.4 serializers.go:4655, deserializers.go:10190).
// Internally it behaves as a plain map for backend convenience; only
// (Un)MarshalJSON differ from map[string]string's default encoding.
type wireTags map[string]string

type wireTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (t wireTags) MarshalJSON() ([]byte, error) {
	items := make([]wireTag, 0, len(t))
	for k, v := range t {
		items = append(items, wireTag{Key: k, Value: v})
	}

	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })

	return json.Marshal(items)
}

func (t *wireTags) UnmarshalJSON(data []byte) error {
	var items []wireTag
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}

	m := make(map[string]string, len(items))
	for _, it := range items {
		m[it.Key] = it.Value
	}

	*t = m

	return nil
}
