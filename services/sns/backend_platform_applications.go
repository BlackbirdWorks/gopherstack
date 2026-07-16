package sns

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreatePlatformApplication creates a new SNS platform application using the backend's default region.
func (b *InMemoryBackend) CreatePlatformApplication(
	name, platform string,
	attributes map[string]string,
) (*PlatformApplication, error) {
	return b.CreatePlatformApplicationInRegion(name, platform, b.region, attributes)
}

// CreatePlatformApplicationInRegion creates a new SNS platform application (e.g. GCM, APNS)
// with the ARN scoped to the specified region.
func (b *InMemoryBackend) CreatePlatformApplicationInRegion(
	name, platform, region string,
	attributes map[string]string,
) (*PlatformApplication, error) {
	if region == "" {
		region = b.region
	}
	if strings.ContainsAny(name, "/") || strings.ContainsAny(platform, "/") {
		return nil, fmt.Errorf("%w: Name and Platform must not contain '/'", ErrInvalidParameter)
	}

	// Validate platform is one of the known AWS SNS platforms.
	// FCM is the Firebase Cloud Messaging platform (successor to GCM).
	// APNS_VOIP and APNS_VOIP_SANDBOX support Apple VoIP push notifications.
	validPlatforms := map[string]bool{
		"GCM": true, "FCM": true, "APNS": true, "APNS_SANDBOX": true,
		"APNS_VOIP": true, "APNS_VOIP_SANDBOX": true,
		"ADM": true, "BAIDU": true, "WNS": true, "MPNS": true,
	}
	if !validPlatforms[platform] {
		return nil, fmt.Errorf(
			"%w: Platform must be one of GCM, FCM, APNS, APNS_SANDBOX, APNS_VOIP, APNS_VOIP_SANDBOX, ADM, BAIDU, WNS, MPNS",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreatePlatformApplicationInRegion")
	defer b.mu.Unlock()

	appArn := arn.Build("sns", region, b.accountID, "app/"+platform+"/"+name)

	if b.platformApplications.Has(appArn) {
		return nil, ErrPlatformApplicationAlreadyExists
	}

	attrs := make(map[string]string, len(attributes)+1)
	maps.Copy(attrs, attributes)

	// AWS always returns Enabled=true for newly created platform applications.
	if attrs["Enabled"] == "" {
		attrs["Enabled"] = "true"
	}

	app := &PlatformApplication{
		PlatformApplicationArn: appArn,
		Attributes:             attrs,
		CreationTimestamp:      time.Now().UTC(),
	}
	b.platformApplications.Put(app)

	return app, nil
}

// GetPlatformApplicationAttributes returns the attributes of a platform application.
// In addition to stored attributes, computed statistics are returned:
//   - Enabled: always "true" for the application itself (not to be confused with endpoint Enabled).
//   - EndpointActive: the number of enabled platform endpoints for this application.
//   - EndpointDisabled: the number of disabled platform endpoints.
func (b *InMemoryBackend) GetPlatformApplicationAttributes(
	platformApplicationArn string,
) (map[string]string, error) {
	b.mu.RLock("GetPlatformApplicationAttributes")
	defer b.mu.RUnlock()

	app, exists := b.platformApplications.Get(platformApplicationArn)
	if !exists {
		return nil, ErrPlatformApplicationNotFound
	}

	// Count active and disabled endpoints for this application.
	var activeCount, disabledCount int
	for _, ep := range b.platformEndpoints.All() {
		if ep.PlatformApplicationArn != platformApplicationArn {
			continue
		}

		if ep.Attributes["Enabled"] == boolFalseStr {
			disabledCount++
		} else {
			activeCount++
		}
	}

	const computedCountFields = 2
	attrs := make(map[string]string, len(app.Attributes)+computedCountFields)
	maps.Copy(attrs, app.Attributes)

	// AWS always returns these computed counts.
	attrs["EndpointActive"] = strconv.Itoa(activeCount)
	attrs["EndpointDisabled"] = strconv.Itoa(disabledCount)

	return attrs, nil
}

// SetPlatformApplicationAttributes updates attributes on a platform application.
func (b *InMemoryBackend) SetPlatformApplicationAttributes(
	platformApplicationArn string,
	attributes map[string]string,
) error {
	b.mu.Lock("SetPlatformApplicationAttributes")
	defer b.mu.Unlock()

	app, exists := b.platformApplications.Get(platformApplicationArn)
	if !exists {
		return ErrPlatformApplicationNotFound
	}

	maps.Copy(app.Attributes, attributes)

	return nil
}

// ListPlatformApplications returns a page of platform applications and the next pagination token.
func (b *InMemoryBackend) ListPlatformApplications(
	nextToken string,
) ([]PlatformApplication, string, error) {
	b.mu.RLock("ListPlatformApplications")
	defer b.mu.RUnlock()

	all := b.sortedPlatformApplications()

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	apps, next := paginate(all, offset, pageSize)

	return apps, next, nil
}

// DeletePlatformApplication removes a platform application and its endpoints by ARN.
func (b *InMemoryBackend) DeletePlatformApplication(platformApplicationArn string) error {
	b.mu.Lock("DeletePlatformApplication")
	defer b.mu.Unlock()

	if !b.platformApplications.Has(platformApplicationArn) {
		return ErrPlatformApplicationNotFound
	}

	b.platformApplications.Delete(platformApplicationArn)

	// Remove all endpoints associated with this platform application. Collect
	// keys first rather than deleting from within Table.Range's own iteration.
	var toDelete []string

	for _, ep := range b.platformEndpoints.All() {
		if ep.PlatformApplicationArn == platformApplicationArn {
			toDelete = append(toDelete, ep.EndpointArn)
		}
	}

	for _, endpointArn := range toDelete {
		b.platformEndpoints.Delete(endpointArn)
	}

	return nil
}

// sortedPlatformApplications returns platform applications sorted by ARN. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedPlatformApplications() []PlatformApplication {
	apps := make([]PlatformApplication, 0, b.platformApplications.Len())
	for _, a := range b.platformApplications.All() {
		apps = append(apps, *a)
	}

	sort.Slice(apps, func(i, j int) bool {
		return apps[i].PlatformApplicationArn < apps[j].PlatformApplicationArn
	})

	return apps
}

// ListAllPlatformApplications returns all platform applications sorted by ARN.
func (b *InMemoryBackend) ListAllPlatformApplications() []PlatformApplication {
	b.mu.RLock("ListAllPlatformApplications")
	defer b.mu.RUnlock()

	apps := make([]PlatformApplication, 0, b.platformApplications.Len())
	for _, app := range b.platformApplications.All() {
		apps = append(apps, *app)
	}

	sort.Slice(apps, func(i, j int) bool {
		return apps[i].PlatformApplicationArn < apps[j].PlatformApplicationArn
	})

	return apps
}
