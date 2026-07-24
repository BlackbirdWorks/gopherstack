package verifiedpermissions

import (
	"fmt"
	"strings"
	"time"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// identitySourceARN builds the ARN for an identity source.
func identitySourceARN(accountID, policyStoreID, sourceID string) string {
	resource := fmt.Sprintf("identity-source/%s/%s", policyStoreID, sourceID)

	return arn.Build("verifiedpermissions", "", accountID, resource)
}

// cognitoIssuerFromUserPoolArn derives the OIDC issuer URL that real AWS
// computes for a Cognito-backed identity source from the user pool's ARN
// (arn:aws:cognito-idp:<region>:<account>:userpool/<poolId>), returning
// "https://cognito-idp.<region>.amazonaws.com/<poolId>" -- see the
// CognitoUserPoolConfigurationDetail/Item.Issuer doc in the SDK, which is a
// required response field Verified Permissions always sets even though
// CreateIdentitySource/UpdateIdentitySource callers never provide it
// directly. Returns "" if userPoolArn cannot be parsed as an ARN.
func cognitoIssuerFromUserPoolArn(userPoolArn string) string {
	parsed, err := awsarn.Parse(userPoolArn)
	if err != nil {
		return ""
	}

	poolID := strings.TrimPrefix(parsed.Resource, "userpool/")

	return fmt.Sprintf("https://cognito-idp.%s.amazonaws.com/%s", parsed.Region, poolID)
}

// cloneIdentitySource returns a deep copy of an IdentitySource.
func cloneIdentitySource(is *IdentitySource) *IdentitySource {
	cp := *is

	if len(is.ClientIDs) > 0 {
		cp.ClientIDs = make([]string, len(is.ClientIDs))
		copy(cp.ClientIDs, is.ClientIDs)
	}

	if is.CognitoGroupConfig != nil {
		cfg := *is.CognitoGroupConfig
		cp.CognitoGroupConfig = &cfg
	}

	if is.OIDCGroupConfig != nil {
		cfg := *is.OIDCGroupConfig
		cp.OIDCGroupConfig = &cfg
	}

	if is.OIDCTokenSelection != nil {
		sel := *is.OIDCTokenSelection
		if len(is.OIDCTokenSelection.Audiences) > 0 {
			sel.Audiences = make([]string, len(is.OIDCTokenSelection.Audiences))
			copy(sel.Audiences, is.OIDCTokenSelection.Audiences)
		}
		cp.OIDCTokenSelection = &sel
	}

	return &cp
}

func identitySourceKey(policyStoreID, identitySourceID string) string {
	return policyStoreID + "/" + identitySourceID
}

// CreateIdentitySource creates a new identity source in the given policy
// store. A non-empty clientToken makes the call idempotent for eight hours,
// same semantics as CreatePolicyStore's ClientToken.
func (b *InMemoryBackend) CreateIdentitySource(
	policyStoreID, principalEntityType string,
	cfg IdentitySourceConfig,
	clientToken string,
) (*IdentitySource, error) {
	b.mu.Lock("CreateIdentitySource")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	fingerprint := identitySourceFingerprint(policyStoreID, principalEntityType, cfg)

	existingID, err := b.checkClientToken("CreateIdentitySource", clientToken, fingerprint)
	if err != nil {
		return nil, err
	}

	if existingID != "" {
		if existing, ok := b.identitySources.Get(identitySourceKey(policyStoreID, existingID)); ok {
			return cloneIdentitySource(existing), nil
		}
	}

	id := uuid.NewString()
	now := time.Now()

	is := &IdentitySource{
		IdentitySourceID:    id,
		PolicyStoreID:       policyStoreID,
		PrincipalEntityType: principalEntityType,
		CreatedDate:         now,
		LastUpdated:         now,
	}

	applyIdentitySourceConfig(is, cfg)

	b.identitySources.Put(is)
	b.arnIndex[identitySourceARN(b.accountID, policyStoreID, id)] = arnKindIdentitySource + ":" + policyStoreID + ":" + id
	b.recordClientToken("CreateIdentitySource", clientToken, fingerprint, id)

	return cloneIdentitySource(is), nil
}

// identitySourceFingerprint deterministically encodes a CreateIdentitySource
// call's parameters for ClientToken idempotency.
func identitySourceFingerprint(policyStoreID, principalEntityType string, cfg IdentitySourceConfig) string {
	return strings.Join([]string{
		policyStoreID, principalEntityType, cfg.UserPoolArn, strings.Join(cfg.ClientIDs, ","),
		cfg.CognitoGroupEntityType, cfg.Issuer, cfg.EntityIDPrefix, cfg.OIDCGroupClaim,
		cfg.OIDCGroupEntityType, cfg.TokenType, cfg.PrincipalIDClaim, strings.Join(cfg.Audiences, ","),
	}, "\x00")
}

// GetIdentitySource returns the identity source with the given ID.
func (b *InMemoryBackend) GetIdentitySource(policyStoreID, identitySourceID string) (*IdentitySource, error) {
	b.mu.RLock("GetIdentitySource")
	defer b.mu.RUnlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	is, ok := b.identitySources.Get(identitySourceKey(policyStoreID, identitySourceID))
	if !ok {
		return nil, fmt.Errorf("%w: identity source %s not found", ErrIdentitySourceNotFound, identitySourceID)
	}

	return cloneIdentitySource(is), nil
}

// DeleteIdentitySource removes an identity source from the given policy store.
func (b *InMemoryBackend) DeleteIdentitySource(policyStoreID, identitySourceID string) error {
	b.mu.Lock("DeleteIdentitySource")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if !b.identitySources.Has(identitySourceKey(policyStoreID, identitySourceID)) {
		return fmt.Errorf("%w: identity source %s not found", ErrIdentitySourceNotFound, identitySourceID)
	}

	resourceARN := identitySourceARN(b.accountID, policyStoreID, identitySourceID)
	delete(b.arnIndex, resourceARN)
	delete(b.resourceTags, resourceARN)
	b.identitySources.Delete(identitySourceKey(policyStoreID, identitySourceID))

	return nil
}

// ListIdentitySources returns all identity sources for a policy store sorted
// by creation date. principalEntityTypes mirrors the wire "filters" list
// (each element's principalEntityType); when non-empty, only identity
// sources whose PrincipalEntityType matches one of them are returned (an OR
// across filters, matching AWS's ListIdentitySourcesInput.Filters semantics).
func (b *InMemoryBackend) ListIdentitySources(
	policyStoreID, nextToken string,
	maxResults int,
	principalEntityTypes []string,
) ([]IdentitySource, string, error) {
	b.mu.RLock("ListIdentitySources")
	defer b.mu.RUnlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, "", fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	sources := b.identitySourcesByStore.Get(policyStoreID)
	if len(principalEntityTypes) > 0 {
		allowed := make(map[string]bool, len(principalEntityTypes))
		for _, t := range principalEntityTypes {
			allowed[t] = true
		}

		filtered := make([]*IdentitySource, 0, len(sources))

		for _, is := range sources {
			if allowed[is.PrincipalEntityType] {
				filtered = append(filtered, is)
			}
		}

		sources = filtered
	}

	page, tok := listByPolicyStore(sources, nextToken, maxResults,
		func(is *IdentitySource) IdentitySource { return *cloneIdentitySource(is) },
		func(is IdentitySource) time.Time { return is.CreatedDate },
		func(is IdentitySource) string { return is.IdentitySourceID },
	)

	return page, tok, nil
}

// UpdateIdentitySource updates the configuration and principal entity type of an identity source.
func (b *InMemoryBackend) UpdateIdentitySource(
	policyStoreID, identitySourceID, principalEntityType string,
	cfg IdentitySourceConfig,
) (*IdentitySource, error) {
	b.mu.Lock("UpdateIdentitySource")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	is, ok := b.identitySources.Get(identitySourceKey(policyStoreID, identitySourceID))
	if !ok {
		return nil, fmt.Errorf("%w: identity source %s not found", ErrIdentitySourceNotFound, identitySourceID)
	}

	// Clear old config before applying new one to avoid stale fields.
	is.UserPoolArn = ""
	is.ClientIDs = nil
	is.CognitoGroupConfig = nil
	is.OpenIDIssuer = ""
	is.EntityIDPrefix = ""
	is.OIDCGroupConfig = nil
	is.OIDCTokenSelection = nil

	applyIdentitySourceConfig(is, cfg)

	if principalEntityType != "" {
		is.PrincipalEntityType = principalEntityType
	}

	is.LastUpdated = time.Now()

	return cloneIdentitySource(is), nil
}

// applyIdentitySourceConfig writes cfg fields into is.
func applyIdentitySourceConfig(is *IdentitySource, cfg IdentitySourceConfig) {
	if cfg.UserPoolArn != "" {
		is.UserPoolArn = cfg.UserPoolArn

		cloned := make([]string, len(cfg.ClientIDs))
		copy(cloned, cfg.ClientIDs)
		is.ClientIDs = cloned

		if cfg.CognitoGroupEntityType != "" {
			is.CognitoGroupConfig = &CognitoGroupConfig{GroupEntityType: cfg.CognitoGroupEntityType}
		}
	} else if cfg.Issuer != "" {
		is.OpenIDIssuer = cfg.Issuer
		is.EntityIDPrefix = cfg.EntityIDPrefix

		if cfg.OIDCGroupClaim != "" || cfg.OIDCGroupEntityType != "" {
			is.OIDCGroupConfig = &OIDCGroupConfig{
				GroupClaim:      cfg.OIDCGroupClaim,
				GroupEntityType: cfg.OIDCGroupEntityType,
			}
		}

		if cfg.TokenType != "" || cfg.PrincipalIDClaim != "" || len(cfg.Audiences) > 0 {
			aud := make([]string, len(cfg.Audiences))
			copy(aud, cfg.Audiences)
			is.OIDCTokenSelection = &OIDCTokenSelection{
				TokenType:        cfg.TokenType,
				PrincipalIDClaim: cfg.PrincipalIDClaim,
				Audiences:        aud,
			}
		}
	}
}
