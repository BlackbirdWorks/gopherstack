package transcribe

import (
	"bufio"
	"bytes"
	"context"
	"strings"
)

const (
	filterMethodMask   = "mask"
	filterMethodRemove = "remove"
	filterMethodTag    = "tag"
	maskedWord         = "***"
	maxFilterFileBytes = 1 << 20
	maxFilterWords     = 50000
)

// withFilterLocked resolves the job's vocabulary filter; the caller holds b.mu.
func (b *InMemoryBackend) withFilterLocked(spec jobSpec) jobSpec {
	if spec.FilterName == "" {
		return spec
	}

	if f, ok := b.vocabularyFilters.Get(spec.FilterName); ok {
		spec.FilterWords = append([]string(nil), f.Words...)
		spec.FilterURI = f.VocabularyFilterFileURI
	}

	return spec
}

func (s jobSpec) filterSet(ctx context.Context, deps pipelineDeps) map[string]struct{} {
	if s.FilterName == "" {
		return nil
	}

	set := make(map[string]struct{}, len(s.FilterWords))
	for _, w := range s.FilterWords {
		set[strings.ToLower(w)] = struct{}{}
	}

	if s.FilterURI != "" && deps.reader != nil {
		if bucket, key, ok := parseMediaURI(s.FilterURI); ok {
			if data, err := readFilterFile(ctx, deps.reader, bucket, key); err == nil {
				for _, w := range parseFilterFile(data) {
					set[w] = struct{}{}
				}
			}
		}
	}

	return set
}

func readFilterFile(ctx context.Context, r MediaReader, bucket, key string) ([]byte, error) {
	if lr, ok := r.(LimitedMediaReader); ok {
		return lr.GetObjectLimit(ctx, bucket, key, maxFilterFileBytes)
	}

	return r.GetObject(ctx, bucket, key)
}

// parseFilterFile reads one word per line; oversized files and lines beyond the caps are ignored.
func parseFilterFile(data []byte) []string {
	if len(data) > maxFilterFileBytes {
		return nil
	}

	var out []string

	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() && len(out) < maxFilterWords {
		if w := strings.ToLower(strings.TrimSpace(sc.Text())); w != "" {
			out = append(out, w)
		}
	}

	return out
}

// applyVocabularyFilter masks, removes or tags the words that match the filter set exactly (case folded).
func applyVocabularyFilter(channels [][]Word, set map[string]struct{}, method string) [][]Word {
	if len(set) == 0 {
		return channels
	}

	out := make([][]Word, len(channels))

	for c, words := range channels {
		kept := make([]Word, 0, len(words))

		for _, w := range words {
			if _, hit := set[strings.ToLower(w.Text)]; !hit {
				kept = append(kept, w)

				continue
			}

			switch method {
			case filterMethodRemove:
				continue
			case filterMethodTag:
				w.Filtered = true
			default:
				w.Text = maskedWord
			}

			kept = append(kept, w)
		}

		out[c] = kept
	}

	return out
}
