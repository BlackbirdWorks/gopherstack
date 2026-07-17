package pipes

import (
	"context"
	"maps"
)

// EnrichmentHTTPParameters holds HTTP parameters for enrichment calls.
type EnrichmentHTTPParameters struct {
	HeaderParameters      map[string]string `json:"HeaderParameters,omitempty"`
	QueryStringParameters map[string]string `json:"QueryStringParameters,omitempty"`
	PathParameterValues   []string          `json:"PathParameterValues,omitempty"`
}

// EnrichmentParameters holds enrichment-specific configuration.
type EnrichmentParameters struct {
	HTTPParameters *EnrichmentHTTPParameters `json:"HttpParameters,omitempty"`
	InputTemplate  string                    `json:"InputTemplate,omitempty"`
}

func cloneEnrichmentParameters(src *EnrichmentParameters) *EnrichmentParameters {
	ep := *src
	if src.HTTPParameters != nil {
		hp := *src.HTTPParameters
		hp.HeaderParameters = maps.Clone(src.HTTPParameters.HeaderParameters)
		hp.PathParameterValues = append([]string(nil), src.HTTPParameters.PathParameterValues...)
		hp.QueryStringParameters = maps.Clone(src.HTTPParameters.QueryStringParameters)
		ep.HTTPParameters = &hp
	}

	return &ep
}

// RecordEnrichmentCall increments the enrichment invocation counter for a pipe.
func (b *InMemoryBackend) RecordEnrichmentCall(ctx context.Context, pipeName string) {
	b.mu.Lock("RecordEnrichmentCall")
	defer b.mu.Unlock()

	t := b.enrichmentCountsTable(getRegion(ctx, b.region))

	var count int64
	if c, ok := t.Get(pipeName); ok {
		count = c.Count
	}

	t.Put(&enrichmentCounter{Name: pipeName, Count: count + 1})
}

// GetEnrichmentCallCount returns the number of enrichment calls for a pipe.
func (b *InMemoryBackend) GetEnrichmentCallCount(ctx context.Context, pipeName string) int64 {
	b.mu.RLock("GetEnrichmentCallCount")
	defer b.mu.RUnlock()

	c, ok := b.enrichmentCountsTable(getRegion(ctx, b.region)).Get(pipeName)
	if !ok {
		return 0
	}

	return c.Count
}
