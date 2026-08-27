package sns

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreatePlatformEndpoint registers a device token as an endpoint for a platform application.
// AWS deduplication behaviour: if an endpoint with the same token already exists under this
// platform application, the existing endpoint ARN is returned instead of creating a new one.
// After creation, an EventEndpointCreated event is fired to the platform application's configured
// event topic, if any.
func (b *InMemoryBackend) CreatePlatformEndpoint(
	platformApplicationArn, token string,
	attributes map[string]string,
) (*PlatformEndpoint, error) {
	var (
		ep      *PlatformEndpoint
		err     error
		created bool
	)

	func() {
		b.mu.Lock("CreatePlatformEndpoint")
		defer b.mu.Unlock()

		app, exists := b.platformApplications.Get(platformApplicationArn)
		if !exists {
			err = ErrPlatformApplicationNotFound

			return
		}

		// Dedup: return the existing endpoint when the same token is already registered
		// under this platform application (mirrors AWS CreatePlatformEndpoint behaviour).
		for _, existing := range b.platformEndpoints.All() {
			if existing.PlatformApplicationArn == platformApplicationArn &&
				existing.Attributes["Token"] == token {
				ep = existing

				return
			}
		}

		// Derive the platform and app name from the platform application ARN.
		// ARN format: arn:aws:sns:{region}:{accountID}:app/{Platform}/{AppName}
		parts := strings.Split(app.PlatformApplicationArn, ":")
		resource := parts[len(parts)-1] // "app/{Platform}/{AppName}"
		resourceParts := strings.SplitN(resource, "/", platformARNResourceParts)

		if len(resourceParts) != platformARNResourceParts {
			err = fmt.Errorf(
				"%w: malformed platform application ARN: %s",
				ErrInvalidParameter,
				platformApplicationArn,
			)

			return
		}

		platform := resourceParts[1]
		appName := resourceParts[2]

		appRegion := arnRegion(platformApplicationArn)
		if appRegion == "" {
			appRegion = b.region
		}
		endpointArn := arn.Build("sns", appRegion, b.accountID,
			"endpoint/"+platform+"/"+appName+"/"+uuid.NewString())

		// Allocate with room for Token and Enabled (endpointExtraAttrs) beyond caller-supplied attrs.
		attrs := make(map[string]string, len(attributes)+endpointExtraAttrs)
		maps.Copy(attrs, attributes)
		attrs["Token"] = token
		attrs["Enabled"] = "true"

		newEP := &PlatformEndpoint{
			EndpointArn:            endpointArn,
			PlatformApplicationArn: platformApplicationArn,
			Attributes:             attrs,
			CreationTimestamp:      time.Now().UTC(),
		}
		b.platformEndpoints.Put(newEP)

		ep = newEP
		created = true
	}()

	if err != nil {
		return nil, err
	}

	if created {
		// Fire endpoint-created event to the configured topic (best-effort, non-blocking).
		b.fireEndpointEvent(platformApplicationArn, "EventEndpointCreated", map[string]string{
			eventTypeKey:   "EndpointCreated",
			endpointArnKey: ep.EndpointArn,
			"Token":        token,
		})
	}

	return ep, nil
}

// GetEndpointAttributes returns the attributes of a platform endpoint.
func (b *InMemoryBackend) GetEndpointAttributes(endpointArn string) (map[string]string, error) {
	b.mu.RLock("GetEndpointAttributes")
	defer b.mu.RUnlock()

	ep, exists := b.platformEndpoints.Get(endpointArn)
	if !exists {
		return nil, ErrEndpointNotFound
	}

	attrs := make(map[string]string, len(ep.Attributes))
	maps.Copy(attrs, ep.Attributes)

	return attrs, nil
}

// SetEndpointAttributes updates attributes on a platform endpoint.
// After the update, an EventEndpointUpdated event is fired to the platform
// application's configured event topic, if any.
func (b *InMemoryBackend) SetEndpointAttributes(
	endpointArn string,
	attributes map[string]string,
) error {
	var (
		platformAppArn string
		err            error
	)

	func() {
		b.mu.Lock("SetEndpointAttributes")
		defer b.mu.Unlock()

		ep, exists := b.platformEndpoints.Get(endpointArn)
		if !exists {
			err = ErrEndpointNotFound

			return
		}

		maps.Copy(ep.Attributes, attributes)
		platformAppArn = ep.PlatformApplicationArn
	}()

	if err != nil {
		return err
	}

	b.fireEndpointEvent(platformAppArn, "EventEndpointUpdated", map[string]string{
		eventTypeKey:   "EndpointUpdated",
		endpointArnKey: endpointArn,
	})

	return nil
}

// ListEndpointsByPlatformApplication returns a page of endpoints for a platform application.
func (b *InMemoryBackend) ListEndpointsByPlatformApplication(
	platformApplicationArn, nextToken string,
) ([]PlatformEndpoint, string, error) {
	b.mu.RLock("ListEndpointsByPlatformApplication")
	defer b.mu.RUnlock()

	if !b.platformApplications.Has(platformApplicationArn) {
		return nil, "", ErrPlatformApplicationNotFound
	}

	all := b.sortedEndpoints()
	filtered := make([]PlatformEndpoint, 0, len(all))

	for _, ep := range all {
		if ep.PlatformApplicationArn == platformApplicationArn {
			filtered = append(filtered, ep)
		}
	}

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	eps, next := paginate(filtered, offset, pageSize)

	return eps, next, nil
}

// DeleteEndpoint removes a platform endpoint by ARN.
// After deletion, an EventEndpointDeleted event is fired to the platform
// application's configured event topic, if any.
func (b *InMemoryBackend) DeleteEndpoint(endpointArn string) error {
	var (
		platformAppArn string
		err            error
	)

	func() {
		b.mu.Lock("DeleteEndpoint")
		defer b.mu.Unlock()

		ep, exists := b.platformEndpoints.Get(endpointArn)
		if !exists {
			err = ErrEndpointNotFound

			return
		}

		platformAppArn = ep.PlatformApplicationArn
		b.platformEndpoints.Delete(endpointArn)
	}()

	if err != nil {
		return err
	}

	b.fireEndpointEvent(platformAppArn, "EventEndpointDeleted", map[string]string{
		eventTypeKey:   "EndpointDeleted",
		endpointArnKey: endpointArn,
	})

	return nil
}

// sortedEndpoints returns platform endpoints sorted by ARN. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedEndpoints() []PlatformEndpoint {
	eps := make([]PlatformEndpoint, 0, b.platformEndpoints.Len())
	for _, ep := range b.platformEndpoints.All() {
		eps = append(eps, *ep)
	}

	sort.Slice(eps, func(i, j int) bool {
		return eps[i].EndpointArn < eps[j].EndpointArn
	})

	return eps
}

// fireEndpointEvent publishes an endpoint lifecycle event notification to the
// SNS topic configured in the platform application's event attribute (e.g.
// "EventEndpointCreated"). This is best-effort and non-blocking; errors are
// silently discarded so that endpoint operations always succeed regardless of
// whether the event topic exists.
func (b *InMemoryBackend) fireEndpointEvent(appArn, eventAttr string, payload map[string]string) {
	var topicArn string

	func() {
		b.mu.RLock("fireEndpointEvent")
		defer b.mu.RUnlock()

		app, exists := b.platformApplications.Get(appArn)
		if exists {
			topicArn = app.Attributes[eventAttr]
		}
	}()

	if topicArn == "" {
		return
	}

	msg, _ := json.Marshal(payload)
	_, _ = b.Publish(topicArn, string(msg), "", "", nil)
}
