package opensearch

import "fmt"

// Insight entity type constants, matching types.InsightEntityType /
// types.InsightFeedbackEntityType.
const (
	insightEntityTypeAccount    = "Account"
	insightEntityTypeDomainName = "DomainName"
)

// ValidateInsightEntity checks an InsightEntity/InsightFeedbackEntity's
// Type/Value against real backend state: a DomainName entity must reference a
// domain that actually exists, matching how every other domain-scoped
// operation in this backend errors on an unknown domain. An Account entity is
// accepted for any non-empty account ID -- this backend has no concept of a
// second account to validate against.
//
// ListInsights/DescribeInsightDetails/InsightFeedback have no analytics
// engine behind them (this emulator generates no real insights), so beyond
// this entity validation they report "nothing found" rather than fabricating
// data -- see handler_insights.go.
func (b *InMemoryBackend) ValidateInsightEntity(entityType, entityValue string) error {
	if entityValue == "" {
		return fmt.Errorf("%w: Entity.Value is required", ErrInvalidParameter)
	}

	switch entityType {
	case insightEntityTypeAccount:
		return nil
	case insightEntityTypeDomainName:
		b.mu.RLock("ValidateInsightEntity")
		defer b.mu.RUnlock()

		d, exists := b.domains.Get(entityValue)
		if !exists || deleteWindowElapsed(d, b.clock()) {
			return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, entityValue)
		}

		return nil
	default:
		return fmt.Errorf("%w: Entity.Type must be Account or DomainName", ErrInvalidParameter)
	}
}
