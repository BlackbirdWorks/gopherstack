package filters

import mobyclient "github.com/moby/moby/client"

// KeyValuePair describes a single filter term and value.
type KeyValuePair struct {
	Key   string
	Value string
}

// Args is a compatibility wrapper over the Moby client filters type.
type Args struct {
	inner mobyclient.Filters
}

// NewArgs builds a filter set from the provided key/value pairs.
func NewArgs(pairs ...KeyValuePair) Args {
	args := Args{inner: make(mobyclient.Filters)}
	for _, pair := range pairs {
		args.inner.Add(pair.Key, pair.Value)
	}

	return args
}

// Get returns all values currently recorded for a filter key.
func (a Args) Get(key string) []string {
	values := a.inner[key]
	if len(values) == 0 {
		return nil
	}

	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}

	return result
}

// Moby returns the underlying Moby filters representation.
func (a Args) Moby() mobyclient.Filters {
	if a.inner == nil {
		return make(mobyclient.Filters)
	}

	return a.inner
}
