package iam

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// insertSorted inserts v into a sorted []string slice in lexicographic order.
// Uses binary search for O(log n) position, O(n) element shift.
func insertSorted(s []string, v string) []string {
	i, _ := slices.BinarySearch(s, v)

	return slices.Insert(s, i, v)
}

// deleteSorted removes the first occurrence of v from a sorted []string slice.
// Uses binary search; no-op if v is not present.
func deleteSorted(s []string, v string) []string {
	i, found := slices.BinarySearch(s, v)
	if !found {
		return s
	}

	return slices.Delete(s, i, i+1)
}

// pageFromSortedNames paginates over a pre-sorted name slice, building the value page
// by looking up each name via lookup (typically a [store.Table.Get] method value).
// Marker is a base64-encoded integer index (opaque to callers) enabling O(1) position
// resolution without scanning all names.
func pageFromSortedNames[T any](
	names []string,
	lookup func(string) (*T, bool),
	marker string,
	limit, defaultLimit int,
) page.Page[T] {
	if limit <= 0 {
		limit = defaultLimit
	}

	start := page.DecodeToken(marker)
	if start >= len(names) {
		return page.Page[T]{Data: []T{}}
	}

	end := start + limit
	var next string

	if end < len(names) {
		next = page.EncodeToken(end)
	} else {
		end = len(names)
	}

	window := names[start:end]
	data := make([]T, 0, len(window))

	for _, name := range window {
		if v, ok := lookup(name); ok {
			data = append(data, *v)
		}
	}

	return page.Page[T]{Data: data, Next: next}
}

// credentialReportHeader is the CSV header for the IAM credential report.
const credentialReportHeader = "user,arn,user_creation_time,password_enabled,password_last_used," +
	"password_last_changed,password_next_rotation,mfa_active," +
	"access_key_1_active,access_key_1_last_rotated,access_key_1_last_used_date," +
	"access_key_1_last_used_region,access_key_1_last_used_service," +
	"access_key_2_active,access_key_2_last_rotated,access_key_2_last_used_date," +
	"access_key_2_last_used_region,access_key_2_last_used_service," +
	"cert_1_active,cert_1_last_rotated,cert_2_active,cert_2_last_rotated"

// UpdateServiceSpecificCredential updates the status of a service-specific credential.
func (b *InMemoryBackend) UpdateServiceSpecificCredential(
	userName, credentialID, status string,
) error {
	b.mu.Lock("UpdateServiceSpecificCredential")
	defer b.mu.Unlock()

	if _, exists := b.users.Get(userName); !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	cred, exists := b.serviceSpecificCreds.Get(credentialID)
	if !exists {
		return fmt.Errorf(
			"%w: service-specific credential %q not found",
			ErrPolicyNotFound,
			credentialID,
		)
	}

	if cred.UserName != userName {
		return fmt.Errorf(
			"%w: credential %q does not belong to user %q",
			ErrPolicyNotFound,
			credentialID,
			userName,
		)
	}

	cred.Status = status
	b.serviceSpecificCreds.Put(cred)

	return nil
}

// GetAccountSummary returns comprehensive account summary counts.
func (b *InMemoryBackend) GetAccountSummary() AccountSummary {
	b.mu.RLock("GetAccountSummary")
	defer b.mu.RUnlock()

	totalKeys, activeKeys := b.accessKeyCountLocked()
	attachedPolicies := 0

	for _, arns := range b.userPolicies {
		attachedPolicies += len(arns)
	}

	for _, arns := range b.rolePolicies {
		attachedPolicies += len(arns)
	}

	for _, arns := range b.groupPolicies {
		attachedPolicies += len(arns)
	}

	return AccountSummary{
		Users:             b.users.Len(),
		Groups:            b.groups.Len(),
		Roles:             b.roles.Len(),
		Policies:          b.policies.Len(),
		InstanceProfiles:  b.instanceProfiles.Len(),
		SAMLProviders:     b.samlProviders.Len(),
		MFADevices:        b.virtualMFADevices.Len(),
		AccessKeysPerUser: totalKeys,
		ActiveAccessKeys:  activeKeys,
		AttachedPolicies:  attachedPolicies,
		AccountAliases:    len(b.accountAliases),
		OIDCProviders:     b.oidcProviders.Len(),
	}
}

// accessKeyCountLocked returns total and active access key counts.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) accessKeyCountLocked() (int, int) {
	var total, active int

	for _, ak := range b.accessKeys.All() {
		total++
		if ak.Status == accessKeyStatusActive {
			active++
		}
	}

	return total, active
}

// credReportConsts holds string literals used in credential report CSV rows.
const (
	credNoInfo  = "no_information"
	credFalse   = "false"
	credTrue    = "true"
	credCsvCols = 22 // 8 fixed + 5 per key × 2 + 4 cert fields
)

// credKeyFields returns the 5 CSV fields for one access key slot (or N/A placeholders).
func credKeyFields(ak *AccessKey) []string {
	if ak == nil {
		return []string{credFalse, notApplicable, notApplicable, notApplicable, notApplicable}
	}

	active := credFalse
	if ak.Status == accessKeyStatusActive {
		active = credTrue
	}

	rotated := ak.CreateDate.UTC().Format(time.RFC3339)

	lastUsedDate := notApplicable
	lastUsedRegion := notApplicable
	lastUsedService := notApplicable

	if ak.LastUsedDate != nil {
		lastUsedDate = ak.LastUsedDate.UTC().Format(time.RFC3339)
		if ak.LastUsedRegion != "" {
			lastUsedRegion = ak.LastUsedRegion
		}

		if ak.LastUsedServiceName != "" {
			lastUsedService = ak.LastUsedServiceName
		}
	}

	return []string{active, rotated, lastUsedDate, lastUsedRegion, lastUsedService}
}

// credUserMFAActive returns "true" if the user has at least one active MFA device.
func credUserMFAActive(
	userName string,
	links map[string]string,
	devices *store.Table[VirtualMFADevice],
) string {
	for serial, owner := range links {
		if owner != userName {
			continue
		}

		if dev, ok := devices.Get(serial); ok && dev.Status == MFAStatusEnabled {
			return credTrue
		}
	}

	return credFalse
}

// GetCredentialReport generates a realistic base64-encoded CSV credential report.
// Each user row reflects actual login-profile and access-key state.
func (b *InMemoryBackend) GetCredentialReport() string {
	b.mu.RLock("GetCredentialReport")
	defer b.mu.RUnlock()

	users := sortedUsers(b.users)
	const extraRows = 2
	lines := make([]string, 0, extraRows+len(users))
	lines = append(lines, credentialReportHeader)

	// Root account row (always present in real AWS).
	rootArn := "arn:aws:iam::" + b.accountID + ":root"
	lines = append(lines, strings.Join([]string{
		"<root_account>", rootArn, time.Now().UTC().Format(time.RFC3339),
		notApplicable, credNoInfo, notApplicable, notApplicable, credFalse,
		credFalse, notApplicable, notApplicable, notApplicable, notApplicable,
		credFalse, notApplicable, notApplicable, notApplicable, notApplicable,
		credFalse, notApplicable, credFalse, notApplicable,
	}, ","))

	c := b.comp()
	c.mu.Lock()
	mfaLinks := maps.Clone(c.mfaUserLinks)
	c.mu.Unlock()

	for _, u := range users {
		lines = append(lines, b.credUserRow(u, mfaLinks))
	}

	return strings.Join(lines, "\n")
}

// credUserRow builds the CSV row for a single IAM user.
func (b *InMemoryBackend) credUserRow(u User, mfaLinks map[string]string) string {
	createdAt := u.CreateDate.UTC().Format(time.RFC3339)

	passwordEnabled := credFalse
	if _, has := b.loginProfiles.Get(u.UserName); has {
		passwordEnabled = credTrue
	}

	var userKeys []AccessKey
	userKeysList := b.userAccessKeys[u.UserName]
	for _, id := range userKeysList {
		if ak, ok := b.accessKeys.Get(id); ok {
			userKeys = append(userKeys, *ak)
		}
	}

	sort.Slice(userKeys, func(i, j int) bool {
		return userKeys[i].CreateDate.Before(userKeys[j].CreateDate)
	})

	keyAt := func(idx int) *AccessKey {
		if idx < len(userKeys) {
			return &userKeys[idx]
		}

		return nil
	}

	mfaActive := credUserMFAActive(u.UserName, mfaLinks, b.virtualMFADevices)

	row := make([]string, 0, credCsvCols)
	row = append(row, u.UserName, u.Arn, createdAt,
		passwordEnabled, credNoInfo, notApplicable, notApplicable,
		mfaActive)
	row = append(row, credKeyFields(keyAt(0))...)
	row = append(row, credKeyFields(keyAt(1))...)
	row = append(row, credFalse, notApplicable, credFalse, notApplicable)

	return strings.Join(row, ",")
}
