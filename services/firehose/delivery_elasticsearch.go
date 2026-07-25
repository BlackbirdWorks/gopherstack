package firehose

import "context"

// deliverToElasticsearch bulk-indexes records into a legacy Elasticsearch cluster using the
// same OpenSearch-compatible bulk API as deliverToOpenSearch (Elasticsearch and OpenSearch
// share an on-the-wire bulk protocol; only the Firehose destination-configuration shape
// differs between the two AWS API families).
func (b *InMemoryBackend) deliverToElasticsearch(
	ctx context.Context,
	records [][]byte,
	dest *ElasticsearchDestinationDescription,
	streamARN string,
) {
	b.deliverToOpenSearch(ctx, records, &OpenSearchDestinationDescription{
		ClusterEndpoint: dest.ClusterEndpoint,
		IndexName:       dest.IndexName,
		RetryOptions:    dest.RetryOptions,
	}, streamARN)
}
