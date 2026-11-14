package ociqueue

// MatchFilter compiles a consumer group filter and evaluates it against message attributes.
func MatchFilter(filter string, attrs map[string]string) (bool, error) {
	m, err := compileFilter(filter)
	if err != nil {
		return false, err
	}

	return m(attrs), nil
}
