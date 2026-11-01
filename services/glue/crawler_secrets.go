package glue

import "context"

// CrawlerSecrets resolves the Secrets Manager secrets that connections name in
// SECRET_ID.
type CrawlerSecrets interface {
	SecretString(ctx context.Context, region, secretID string) (string, error)
}

// SetSecretsBackend wires Secrets Manager so crawlers authenticate JDBC and
// MongoDB connections through SECRET_ID.
func (b *InMemoryBackend) SetSecretsBackend(s CrawlerSecrets) {
	b.mu.Lock("SetSecretsBackend")
	defer b.mu.Unlock()

	b.crawlerSecrets = s
}

func (b *InMemoryBackend) currentCrawlerSecrets() CrawlerSecrets {
	b.mu.RLock("currentCrawlerSecrets")
	defer b.mu.RUnlock()

	return b.crawlerSecrets
}
