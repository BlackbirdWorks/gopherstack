package quicksight

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	embedURLSessionLifetime = 10 * time.Hour

	embedURLFormat = "https://%s.quicksight.aws.amazon.com/embed/%s/%s" +
		"?code=%s&identityprovider=quicksight&isauthcode=true&sessionExpirationTimestamp=%d"

	userArnNamespaceAndName = 2

	// UserIdentifier union member names, as serialized by the real SDK
	// (a smithy union: exactly one of {"Email":..}, {"UserArn":..},
	// {"UserName":..} is present in the request body).
	identityKindEmail    = "Email"
	identityKindUserArn  = "UserArn"
	identityKindUserName = "UserName"
)

// generateEmbedURL builds a signed-looking, single-use embed URL derived from the
// given resource path (which callers construct from the request's identifying
// fields, e.g. dashboard/session id), a freshly generated auth code, and an
// expiry timestamp. Every call produces a distinct URL, matching how QuickSight's
// real embed URLs are single-use and time-limited.
func (b *InMemoryBackend) generateEmbedURL(resourcePath string) string {
	authCode := uuid.New().String()
	expiresAt := time.Now().UTC().Add(embedURLSessionLifetime).Unix()

	return fmt.Sprintf(embedURLFormat, b.region, b.accountID, resourcePath, authCode, expiresAt)
}

// experienceResourceHint extracts a short, human-readable hint (e.g. a dashboard
// or topic ID) from an embedding ExperienceConfiguration body, for inclusion in
// the generated embed URL's path. Returns "console" if nothing more specific is
// found, matching the full-console embedding experience.
func experienceResourceHint(experienceConfiguration map[string]any) string {
	for _, key := range []string{"Dashboard", "DashboardVisual", "QSearchBar", "QuickSightConsole", "Console"} {
		exp, isMap := experienceConfiguration[key].(map[string]any)
		if !isMap {
			continue
		}

		for _, idKey := range []string{"InitialDashboardId", "DashboardId"} {
			if id, hasID := exp[idKey].(string); hasID && id != "" {
				return "dashboards/" + id
			}
		}
	}

	return "console"
}

// ---- Embed URLs ----

func (b *InMemoryBackend) GenerateEmbedURLForAnonymousUser(
	accountID, namespace string,
	authorizedResourceArns []string,
	experienceConfiguration map[string]any,
) (string, string, error) {
	if len(authorizedResourceArns) == 0 {
		return "", "", ErrValidation
	}

	b.mu.RLock("GenerateEmbedURLForAnonymousUser")
	defer b.mu.RUnlock()

	if _, ok := b.namespaces[nsKey(accountID, namespace)]; !ok {
		return "", "", ErrNamespaceNotFound
	}

	sessionID := uuid.New().String()
	anonymousUserArn := fmt.Sprintf(
		"arn:aws:quicksight:%s:%s:user/%s/embed-session-%s", b.region, accountID, namespace, sessionID,
	)

	return b.generateEmbedURL(experienceResourceHint(experienceConfiguration)), anonymousUserArn, nil
}

func (b *InMemoryBackend) GenerateEmbedURLForRegisteredUser(
	accountID, userArn string,
	experienceConfiguration map[string]any,
) (string, error) {
	if userArn == "" {
		return "", ErrValidation
	}

	b.mu.RLock("GenerateEmbedURLForRegisteredUser")
	defer b.mu.RUnlock()

	if namespace, userName, ok := parseUserArn(userArn); ok {
		if _, exists := b.users[userKey(accountID, namespace, userName)]; !exists {
			return "", ErrUserNotFound
		}
	}

	return b.generateEmbedURL(experienceResourceHint(experienceConfiguration)), nil
}

// GenerateEmbedURLForRegisteredUserWithIdentity generates an embed URL for
// identity-enhanced (IAM Identity Center) sessions, which authenticate via the
// caller's signing credentials rather than an explicit UserArn/accountID lookup.
func (b *InMemoryBackend) GenerateEmbedURLForRegisteredUserWithIdentity(
	_ string,
	experienceConfiguration map[string]any,
) (string, error) {
	b.mu.RLock("GenerateEmbedURLForRegisteredUserWithIdentity")
	defer b.mu.RUnlock()

	return b.generateEmbedURL(experienceResourceHint(experienceConfiguration)), nil
}

func (b *InMemoryBackend) GetDashboardEmbedURL(accountID, dashboardID, identityType string) (string, error) {
	if dashboardID == "" || identityType == "" {
		return "", ErrValidation
	}

	b.mu.RLock("GetDashboardEmbedURL")
	defer b.mu.RUnlock()

	if _, ok := b.dashboards[dashboardKey(accountID, dashboardID)]; !ok {
		return "", ErrDashboardNotFound
	}

	return b.generateEmbedURL("dashboards/" + dashboardID), nil
}

// GetSessionEmbedURL generates a console-session embed URL. Unlike the other
// embed ops, it isn't scoped to any single stored resource, so accountID is only
// used (via b.accountID) to build the URL itself.
func (b *InMemoryBackend) GetSessionEmbedURL(_, entryPoint string) (string, error) {
	b.mu.RLock("GetSessionEmbedURL")
	defer b.mu.RUnlock()

	hint := "console"
	if entryPoint != "" {
		hint = strings.TrimPrefix(entryPoint, "/")
	}

	return b.generateEmbedURL(hint), nil
}

// GenerateIdentityContext mints an opaque identity-context token for the given
// user identity, for use as the ContextAssertion parameter of an STS AssumeRole
// call. Like the embed-URL family, GetIdentityContext is stateless: nothing is
// persisted, a fresh token is minted on every call, mirroring generateEmbedURL's
// signed-looking, single-use token pattern.
func (b *InMemoryBackend) GenerateIdentityContext(
	accountID, namespace, userIdentifierKind, userIdentifierValue, contextRegion string,
) (string, error) {
	if userIdentifierValue == "" {
		return "", ErrValidation
	}
	if (userIdentifierKind == identityKindEmail || userIdentifierKind == identityKindUserName) && namespace == "" {
		return "", ErrValidation
	}

	b.mu.RLock("GenerateIdentityContext")
	defer b.mu.RUnlock()

	if namespace != "" {
		if _, ok := b.namespaces[nsKey(accountID, namespace)]; !ok {
			return "", ErrNamespaceNotFound
		}
	}

	region := contextRegion
	if region == "" {
		region = b.region
	}

	token := fmt.Sprintf(
		"quicksight-identity-context.%s.%s.%s.%s",
		accountID, region, userIdentifierKind+":"+userIdentifierValue, uuid.New().String(),
	)

	return token, nil
}

// parseUserArn extracts the namespace and user name from a QuickSight user ARN of
// the form "arn:aws:quicksight:region:account:user/namespace/userName". Returns
// ok=false if the ARN doesn't match that shape (e.g. it's an IAM user/role ARN).
func parseUserArn(userArn string) (string, string, bool) {
	_, rest, found := strings.Cut(userArn, ":user/")
	if !found {
		return "", "", false
	}

	parts := strings.SplitN(rest, "/", userArnNamespaceAndName)
	if len(parts) != userArnNamespaceAndName || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}

	return parts[0], parts[1], true
}
