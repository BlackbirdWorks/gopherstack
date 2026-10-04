package applicationautoscaling

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// siblingServices is matched structurally against *CLI to avoid an import
// cycle; see services/grafana/cross_service.go for the reference pattern.
type siblingServices interface {
	GetDynamoDBHandler() service.Registerable
}

// SetAppConfig records ctx.Config so the DynamoDB backend can be resolved lazily.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.appConfig = cfg
}

// dynamoDBBackend returns the DynamoDB backend, if wired.
func (b *InMemoryBackend) dynamoDBBackend() (ddbbackend.StorageBackend, bool) {
	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetDynamoDBHandler().(*ddbbackend.DynamoDBHandler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	return h.Backend, true
}
