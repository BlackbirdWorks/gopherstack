package lambda

import (
	"errors"
	"fmt"
	"slices"
)

// Runtime catalog filters accepted by GET /_aws/lambda/runtimes.
const (
	RuntimeFilterSupported  = "supported"
	RuntimeFilterDeprecated = "deprecated"
	RuntimeFilterAll        = "all"
)

// ErrInvalidRuntimeFilter is returned for a filter other than supported, deprecated or all.
var ErrInvalidRuntimeFilter = errors.New("invalid runtime filter")

// RuntimeIdentifiers lists Lambda runtime identifiers by filter: supported
// (runnable here; the default for ""), deprecated, or all. The result is sorted.
func RuntimeIdentifiers(filter string) ([]string, error) {
	var out []string

	switch filter {
	case "", RuntimeFilterSupported:
		for r := range runtimeBaseImages {
			out = append(out, r)
		}
	case RuntimeFilterDeprecated:
		for r := range deprecatedRuntimes {
			if !isRunnableRuntime(r) {
				out = append(out, r)
			}
		}
	case RuntimeFilterAll:
		for r := range runtimeBaseImages {
			out = append(out, r)
		}

		for r := range deprecatedRuntimes {
			if !isRunnableRuntime(r) {
				out = append(out, r)
			}
		}
	default:
		return nil, fmt.Errorf("%w: %q", ErrInvalidRuntimeFilter, filter)
	}

	slices.Sort(out)

	return out, nil
}

func isRunnableRuntime(r string) bool {
	_, ok := runtimeBaseImages[r]

	return ok
}
