package identitystore

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	maxUserNameLength = 128
	maxPhotosPerUser  = 3
)

const (
	userStatusEnabled  = "ENABLED"
	userStatusDisabled = "DISABLED"
)

// attrUserNameKey is the lower-cased attribute path for username lookups.
const attrUserNameKey = "username"

// ----------------------------------------
// User operations
// ----------------------------------------

// CreateUserRequest holds the parameters for creating a user.
//
// ExternalIDs is NOT wire-reachable: the real CreateUserRequest smithy shape
// has no ExternalIds member (see the doc comment on the wire-facing
// createUserRequest in handler_users.go for the full member list this was
// verified against, and for the gopherstack-invented-field bug this field
// used to enable before this pass). It is kept here only as a Go-level
// convenience for tests that seed backend state directly (e.g.
// persistence_test.go's seedFullState), mirroring how User.ExternalIDs is
// otherwise reachable only through UpdateUser's AttributeOperations.
type CreateUserRequest struct {
	UserName      string        `json:"UserName"`
	DisplayName   string        `json:"DisplayName"`
	NickName      string        `json:"NickName"`
	Title         string        `json:"Title"`
	ProfileURL    string        `json:"ProfileUrl"`
	Locale        string        `json:"Locale"`
	PreferredLang string        `json:"PreferredLanguage"`
	Timezone      string        `json:"Timezone"`
	UserType      string        `json:"UserType"`
	Birthdate     string        `json:"Birthdate"`
	Website       string        `json:"Website"`
	Name          *Name         `json:"Name"`
	Emails        []Email       `json:"Emails"`
	Addresses     []Address     `json:"Addresses"`
	PhoneNumbers  []PhoneNumber `json:"PhoneNumbers"`
	Photos        []Photo       `json:"Photos"`
	Roles         []Role        `json:"Roles"`
	ExternalIDs   []ExternalID  `json:"ExternalIds"`
}

// CreateUser creates a new user in the identity store.
func (b *InMemoryBackend) CreateUser(ctx context.Context, storeID string, req *CreateUserRequest) (*User, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if err := validateUserPayloadStrings(req); err != nil {
		return nil, err
	}

	if len(req.UserName) > maxUserNameLength {
		return nil, fmt.Errorf("%w: UserName must not exceed 128 characters", ErrValidation)
	}

	if req.UserName != "" {
		if existing := b.usersByUserName.Get(storeKey(region, storeID) + "#" + req.UserName); len(existing) > 0 {
			return nil, fmt.Errorf("%w: user with UserName %q already exists", ErrConflict, req.UserName)
		}
	}

	// Primary-email uniqueness: usersByPrimaryEmail is documented (see
	// store_setup.go) as a uniqueness-constraint index mirroring
	// usersByUserName, but no call site ever actually checked it before this
	// pass -- a real gap, not merely a documentation mismatch (see
	// PARITY.md). GetUserId's AlternateIdentifier.UniqueAttribute is
	// documented as supporting exactly "userName" and "emails.value" (see
	// botocore's GetUserIdRequest.AlternateIdentifier doc string); a
	// same-store duplicate primary email would make that lookup ambiguous,
	// and ConflictException's own doc string ("would violate an existing
	// uniqueness claim in the identity store") is written in the plural,
	// not scoped to UserName alone.
	if email := userPrimaryEmail(req.Emails); email != "" {
		if existing := b.usersByPrimaryEmail.Get(storeKey(region, storeID) + "#" + email); len(existing) > 0 {
			return nil, fmt.Errorf("%w: user with primary email %q already exists", ErrConflict, email)
		}
	}

	if len(req.Photos) > maxPhotosPerUser {
		return nil, fmt.Errorf("%w: Photos must not exceed %d items", ErrValidation, maxPhotosPerUser)
	}

	if req.Birthdate != "" && !isValidBirthdate(req.Birthdate) {
		return nil, fmt.Errorf("%w: Birthdate must be in YYYY-MM-DD format", ErrValidation)
	}

	userID := b.generateID()
	now := epochTime(time.Now().UTC())
	callerARN := b.simulatedCallerARN()
	user := &User{
		UserID:          userID,
		IdentityStoreID: storeID,
		UserName:        req.UserName,
		DisplayName:     req.DisplayName,
		NickName:        req.NickName,
		Title:           req.Title,
		ProfileURL:      req.ProfileURL,
		Locale:          req.Locale,
		PreferredLang:   req.PreferredLang,
		Timezone:        req.Timezone,
		UserType:        req.UserType,
		Birthdate:       req.Birthdate,
		Website:         req.Website,
		UserStatus:      userStatusEnabled,
		Name:            req.Name,
		Emails:          req.Emails,
		Addresses:       req.Addresses,
		PhoneNumbers:    req.PhoneNumbers,
		Photos:          req.Photos,
		Roles:           req.Roles,
		ExternalIDs:     req.ExternalIDs,
		region:          region,
		CreatedAt:       now,
		CreatedBy:       callerARN,
		UpdatedAt:       now,
		UpdatedBy:       callerARN,
	}

	b.users.Put(user)

	return copyUser(user), nil
}

// DescribeUser returns a user by ID.
func (b *InMemoryBackend) DescribeUser(ctx context.Context, storeID, userID string) (*User, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeUser")
	defer b.mu.RUnlock()

	user, ok := b.users.Get(regionKey(region, userID))
	if !ok || user.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userID)
	}

	return copyUser(user), nil
}

// ListUsers lists all users for the given identity store, sorted by UserID.
func (b *InMemoryBackend) ListUsers(ctx context.Context, storeID string) []*User {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	matches := b.usersByStore.Get(storeKey(region, storeID))
	result := make([]*User, 0, len(matches))

	for _, u := range matches {
		result = append(result, copyUser(u))
	}

	slices.SortFunc(result, func(a, b *User) int { return strings.Compare(a.UserID, b.UserID) })

	return result
}

// UpdateUser applies attribute operations to a user.
func (b *InMemoryBackend) UpdateUser(ctx context.Context, storeID, userID string, ops []attributeOperation) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	id := regionKey(region, userID)

	user, ok := b.users.Get(id)
	if !ok || user.IdentityStoreID != storeID {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userID)
	}

	for _, op := range ops {
		if err := validateAttributeOperation(op); err != nil {
			return err
		}
	}

	if err := b.validateUsernameRename(region, storeID, user.UserName, ops); err != nil {
		return err
	}

	if err := b.validateEmailRename(region, storeID, user.UserID, user.Emails, ops); err != nil {
		return err
	}

	// Delete before mutating any indexed field (UserName, primary email) so
	// the byUserName/byPrimaryEmail indexes are updated against the value's
	// pre-mutation state -- store.Table.Put re-derives the OLD index key from
	// the live pointer already in the table, so mutating in place first would
	// leave a stale entry under the old key (see pkgs/store package doc).
	b.users.Delete(id)

	for _, op := range ops {
		applyUserAttribute(user, op.AttributePath, op.AttributeValue)
	}

	user.UpdatedAt = epochTime(time.Now().UTC())
	user.UpdatedBy = b.simulatedCallerARN()

	b.users.Put(user)

	return nil
}

// validateUsernameRename checks that no username-rename operation would produce a conflict.
func (b *InMemoryBackend) validateUsernameRename(region, storeID, oldName string, ops []attributeOperation) error {
	for _, op := range ops {
		if strings.ToLower(op.AttributePath) != attrUserNameKey {
			continue
		}

		newName, _ := op.AttributeValue.(string)
		if newName == "" || newName == oldName {
			continue
		}

		if existing := b.usersByUserName.Get(storeKey(region, storeID) + "#" + newName); len(existing) > 0 {
			return fmt.Errorf("%w: user with UserName %q already exists", ErrConflict, newName)
		}
	}

	return nil
}

// validateEmailRename checks that an "emails" attribute operation would not
// produce a primary-email conflict with a different user in the same store.
// Mirrors validateUsernameRename's shape; see CreateUser's primary-email
// uniqueness comment for why this check exists.
func (b *InMemoryBackend) validateEmailRename(
	region, storeID, userID string, currentEmails []Email, ops []attributeOperation,
) error {
	newEmails := currentEmails
	touched := false

	for _, op := range ops {
		if strings.ToLower(op.AttributePath) != attrEmails {
			continue
		}

		newEmails = parseEmails(op.AttributeValue)
		touched = true
	}

	if !touched {
		return nil
	}

	email := userPrimaryEmail(newEmails)
	if email == "" {
		return nil
	}

	for _, other := range b.usersByPrimaryEmail.Get(storeKey(region, storeID) + "#" + email) {
		if other.UserID != userID {
			return fmt.Errorf("%w: user with primary email %q already exists", ErrConflict, email)
		}
	}

	return nil
}

// applyUserAttribute applies a single attribute operation to a user.
func applyUserAttribute(user *User, path string, value any) {
	strVal, _ := value.(string)

	if applyUserScalarAttribute(user, path, strVal) {
		return
	}

	if applyUserSliceAttribute(user, path, value) {
		return
	}

	applyUserNameAttribute(user, path, strVal)
}

// applyUserScalarAttribute sets simple string attributes on a user.
// Returns true when the path was handled.
func applyUserScalarAttribute(user *User, path, strVal string) bool {
	switch strings.ToLower(path) {
	case attrDisplayName:
		user.DisplayName = strVal
	case attrUserNameKey:
		user.UserName = strVal
	case attrNickName:
		user.NickName = strVal
	case attrTitle:
		user.Title = strVal
	case attrProfileURL:
		user.ProfileURL = strVal
	case attrLocale:
		user.Locale = strVal
	case attrPreferredLanguage:
		user.PreferredLang = strVal
	case attrTimezone:
		user.Timezone = strVal
	case attrUserType:
		user.UserType = strVal
	case attrBirthdate:
		user.Birthdate = strVal
	case attrWebsite:
		user.Website = strVal
	case attrUserStatus:
		if isValidUserStatus(strVal) {
			user.UserStatus = strVal
		}
	default:
		return false
	}

	return true
}

// applyUserSliceAttribute sets multi-value attributes on a user.
// Returns true when the path was handled.
func applyUserSliceAttribute(user *User, path string, value any) bool {
	switch strings.ToLower(path) {
	case attrEmails:
		user.Emails = parseEmails(value)
	case attrAddresses:
		user.Addresses = parseAddresses(value)
	case attrPhoneNumbers:
		user.PhoneNumbers = parsePhoneNumbers(value)
	case attrPhotos:
		user.Photos = parsePhotos(value)
	case attrRoles:
		user.Roles = parseRoles(value)
	case attrExternalIDs:
		user.ExternalIDs = parseExternalIDs(value)
	default:
		return false
	}

	return true
}

// isValidUserStatus reports whether s is a recognized UserStatus value.
func isValidUserStatus(s string) bool {
	return s == userStatusEnabled || s == userStatusDisabled
}

// birthdateLen is the required length of a YYYY-MM-DD birthdate string.
const birthdateLen = 10

// isValidBirthdate reports whether s matches the YYYY-MM-DD format.
func isValidBirthdate(s string) bool {
	if len(s) != birthdateLen {
		return false
	}

	for i, c := range s {
		switch i {
		case 4, 7: //nolint:mnd // fixed hyphen positions in YYYY-MM-DD
			if c != '-' {
				return false
			}
		default:
			if c < '0' || c > '9' {
				return false
			}
		}
	}

	return true
}

// applyUserNameAttribute applies name sub-fields to a user.
func applyUserNameAttribute(user *User, path, strVal string) {
	switch strings.ToLower(path) {
	case attrNameGivenName:
		ensureUserName(user)
		user.Name.GivenName = strVal
	case attrNameFamilyName:
		ensureUserName(user)
		user.Name.FamilyName = strVal
	case attrNameMiddleName:
		ensureUserName(user)
		user.Name.MiddleName = strVal
	case attrNameFormatted:
		ensureUserName(user)
		user.Name.Formatted = strVal
	case attrNameHonorificPrefix:
		ensureUserName(user)
		user.Name.HonorificPrefix = strVal
	case attrNameHonorificSuffix:
		ensureUserName(user)
		user.Name.HonorificSuffix = strVal
	}
}

// ensureUserName initialises user.Name if it is nil.
func ensureUserName(user *User) {
	if user.Name == nil {
		user.Name = &Name{}
	}
}

func parseEmails(value any) []Email {
	switch v := value.(type) {
	case nil:
		return nil
	case []Email:
		return append([]Email(nil), v...)
	case []any:
		emails := make([]Email, 0, len(v))
		for _, entry := range v {
			emailMap, ok := entry.(map[string]any)
			if !ok {
				continue
			}

			emails = append(emails, Email{
				Value:   valueAsString(emailMap["Value"]),
				Type:    valueAsString(emailMap["Type"]),
				Primary: valueAsBool(emailMap["Primary"]),
			})
		}

		return emails
	default:
		return nil
	}
}

func parseAddresses(value any) []Address {
	switch v := value.(type) {
	case nil:
		return nil
	case []Address:
		return append([]Address(nil), v...)
	case []any:
		addresses := make([]Address, 0, len(v))
		for _, entry := range v {
			addressMap, ok := entry.(map[string]any)
			if !ok {
				continue
			}

			addresses = append(addresses, Address{
				Formatted:     valueAsString(addressMap["Formatted"]),
				StreetAddress: valueAsString(addressMap["StreetAddress"]),
				Locality:      valueAsString(addressMap["Locality"]),
				Region:        valueAsString(addressMap["Region"]),
				PostalCode:    valueAsString(addressMap["PostalCode"]),
				Country:       valueAsString(addressMap["Country"]),
				Type:          valueAsString(addressMap["Type"]),
				Primary:       valueAsBool(addressMap["Primary"]),
			})
		}

		return addresses
	default:
		return nil
	}
}

func parsePhoneNumbers(value any) []PhoneNumber {
	switch v := value.(type) {
	case nil:
		return nil
	case []PhoneNumber:
		return append([]PhoneNumber(nil), v...)
	case []any:
		numbers := make([]PhoneNumber, 0, len(v))
		for _, entry := range v {
			numberMap, ok := entry.(map[string]any)
			if !ok {
				continue
			}

			numbers = append(numbers, PhoneNumber{
				Value:   valueAsString(numberMap["Value"]),
				Type:    valueAsString(numberMap["Type"]),
				Primary: valueAsBool(numberMap["Primary"]),
			})
		}

		return numbers
	default:
		return nil
	}
}

func parsePhotos(value any) []Photo {
	switch v := value.(type) {
	case nil:
		return nil
	case []Photo:
		return append([]Photo(nil), v...)
	case []any:
		photos := make([]Photo, 0, len(v))
		for _, entry := range v {
			m, ok := entry.(map[string]any)
			if !ok {
				continue
			}

			photos = append(photos, Photo{
				Value:   valueAsString(m["Value"]),
				Display: valueAsString(m["Display"]),
				Type:    valueAsString(m["Type"]),
				Primary: valueAsBool(m["Primary"]),
			})
		}

		return photos
	default:
		return nil
	}
}

func parseRoles(value any) []Role {
	switch v := value.(type) {
	case nil:
		return nil
	case []Role:
		return append([]Role(nil), v...)
	case []any:
		roles := make([]Role, 0, len(v))
		for _, entry := range v {
			m, ok := entry.(map[string]any)
			if !ok {
				continue
			}

			roles = append(roles, Role{
				Value:   valueAsString(m["Value"]),
				Type:    valueAsString(m["Type"]),
				Primary: valueAsBool(m["Primary"]),
			})
		}

		return roles
	default:
		return nil
	}
}

func parseExternalIDs(value any) []ExternalID {
	switch v := value.(type) {
	case nil:
		return nil
	case []ExternalID:
		return append([]ExternalID(nil), v...)
	case []any:
		ids := make([]ExternalID, 0, len(v))
		for _, entry := range v {
			m, ok := entry.(map[string]any)
			if !ok {
				continue
			}

			ids = append(ids, ExternalID{
				Issuer: valueAsString(m["Issuer"]),
				ID:     valueAsString(m["Id"]),
			})
		}

		return ids
	default:
		return nil
	}
}

func valueAsString(value any) string {
	str, _ := value.(string)

	return str
}

func valueAsBool(value any) bool {
	boolean, _ := value.(bool)

	return boolean
}

// userPrimaryEmail returns the Value of the first email marked Primary, or the
// first email Value if none are marked, or "" if the slice is empty.
func userPrimaryEmail(emails []Email) string {
	for _, e := range emails {
		if e.Primary {
			return e.Value
		}
	}

	if len(emails) > 0 {
		return emails[0].Value
	}

	return ""
}

// applyUserFilters returns only the users matching all provided filters.
func applyUserFilters(users []*User, filters []ListFilter) []*User {
	if len(filters) == 0 {
		return users
	}

	result := make([]*User, 0, len(users))

	for _, u := range users {
		if userMatchesFilters(u, filters) {
			result = append(result, u)
		}
	}

	return result
}

// userMatchesFilter reports whether u satisfies a single filter.
func userMatchesFilter(u *User, f ListFilter) bool {
	attr := strings.ToLower(f.AttributePath)
	if attr == "emails.value" || attr == "phonenumbers.value" {
		return matchUserMultiValueFilter(u, f)
	}

	return matchUserSingleValueFilter(u, f)
}

// matchUserMultiValueFilter checks filters on multi-value fields (emails, phoneNumbers).
// Returns true when the filter matched and passed; returns false when either the filter
// did not apply (wrong attribute path) or the value was not found.
func matchUserMultiValueFilter(u *User, f ListFilter) bool {
	switch strings.ToLower(f.AttributePath) {
	case "emails.value":
		for _, e := range u.Emails {
			if e.Value == f.AttributeValue {
				return true
			}
		}

		return false
	case "phonenumbers.value":
		for _, p := range u.PhoneNumbers {
			if p.Value == f.AttributeValue {
				return true
			}
		}

		return false
	}

	return false
}

// matchUserSingleValueFilter checks filters on simple scalar fields. An
// unrecognized AttributePath matches NO user rather than every user -- a
// prior revision returned true from this switch's implicit default, meaning
// a typo'd or unsupported filter path silently matched the entire user
// list instead of being treated as a no-match (see PARITY.md gap history).
func matchUserSingleValueFilter(u *User, f ListFilter) bool {
	switch strings.ToLower(f.AttributePath) {
	case attrUserNameKey:
		return u.UserName == f.AttributeValue
	case attrDisplayName:
		return u.DisplayName == f.AttributeValue
	case attrNameGivenName:
		return u.Name != nil && u.Name.GivenName == f.AttributeValue
	case attrNameFamilyName:
		return u.Name != nil && u.Name.FamilyName == f.AttributeValue
	case attrTitle:
		return u.Title == f.AttributeValue
	case attrNickName:
		return u.NickName == f.AttributeValue
	case attrUserType:
		return u.UserType == f.AttributeValue
	case attrPreferredLanguage:
		return u.PreferredLang == f.AttributeValue
	case attrLocale:
		return u.Locale == f.AttributeValue
	case attrTimezone:
		return u.Timezone == f.AttributeValue
	}

	return false
}

// userMatchesFilters reports whether u satisfies every filter in the slice.
func userMatchesFilters(u *User, filters []ListFilter) bool {
	for _, f := range filters {
		if !userMatchesFilter(u, f) {
			return false
		}
	}

	return true
}

// DeleteUser removes a user from the identity store.
func (b *InMemoryBackend) DeleteUser(ctx context.Context, storeID, userID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	id := regionKey(region, userID)

	user, ok := b.users.Get(id)
	if !ok || user.IdentityStoreID != storeID {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userID)
	}

	b.users.Delete(id)

	// Use the byMember index for O(1) cascade membership deletion. Clone the
	// index-owned slice before the delete loop, since Table.Delete mutates
	// the same index's backing storage as it removes each entry.
	memberKey := storeKey(region, storeID) + "#" + userID

	for _, m := range slices.Clone(b.membershipsByMember.Get(memberKey)) {
		b.memberships.Delete(regionKey(region, m.MembershipID))
	}

	return nil
}

// GetUserID looks up a user ID by alternate identifier (UserName, email, or ExternalId).
func (b *InMemoryBackend) GetUserID(ctx context.Context, storeID, attrPath, attrValue string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetUserID")
	defer b.mu.RUnlock()

	uid, found := b.resolveUserByAttr(region, storeID, attrPath, attrValue)
	if !found {
		return "", fmt.Errorf("%w: no user found with %s=%q", ErrUserNotFound, attrPath, attrValue)
	}

	return uid, nil
}

// resolveUserByAttr returns the user ID matching the given attribute path and value.
func (b *InMemoryBackend) resolveUserByAttr(region, storeID, attrPath, attrValue string) (string, bool) {
	switch {
	case strings.EqualFold(attrPath, attrUserNameKey):
		matches := b.usersByUserName.Get(storeKey(region, storeID) + "#" + attrValue)
		if len(matches) == 0 {
			return "", false
		}

		return matches[0].UserID, true
	case strings.EqualFold(attrPath, "emails.value"):
		return b.resolveUserByEmail(region, storeID, attrValue)
	case strings.EqualFold(attrPath, "externalid"):
		return b.resolveUserByExternalID(region, storeID, attrValue)
	}

	return "", false
}

// resolveUserByEmail returns the user ID matching the given email address.
func (b *InMemoryBackend) resolveUserByEmail(region, storeID, email string) (string, bool) {
	// Fast path via primary-email index.
	if matches := b.usersByPrimaryEmail.Get(storeKey(region, storeID) + "#" + email); len(matches) > 0 {
		return matches[0].UserID, true
	}

	// Slow path: scan all non-primary emails for users in this store.
	for _, u := range b.usersByStore.Get(storeKey(region, storeID)) {
		for _, e := range u.Emails {
			if e.Value == email {
				return u.UserID, true
			}
		}
	}

	return "", false
}

// resolveUserByExternalID returns the user ID whose ExternalIDs contain both the given Issuer and Id.
// The compound argument is Issuer+externalIDSep+Id as encoded by extractAlternateIdentifier.
func (b *InMemoryBackend) resolveUserByExternalID(region, storeID, compound string) (string, bool) {
	issuer, extID := splitExternalIDCompound(compound)

	for _, u := range b.usersByStore.Get(storeKey(region, storeID)) {
		for _, ext := range u.ExternalIDs {
			if ext.Issuer == issuer && ext.ID == extID {
				return u.UserID, true
			}
		}
	}

	return "", false
}

func copyUser(u *User) *User {
	if u == nil {
		return nil
	}
	cp := *u
	if u.Name != nil {
		n := *u.Name
		cp.Name = &n
	}
	cp.Emails = append([]Email(nil), u.Emails...)
	cp.Addresses = append([]Address(nil), u.Addresses...)
	cp.PhoneNumbers = append([]PhoneNumber(nil), u.PhoneNumbers...)
	cp.Photos = append([]Photo(nil), u.Photos...)
	cp.Roles = append([]Role(nil), u.Roles...)
	cp.ExternalIDs = append([]ExternalID(nil), u.ExternalIDs...)

	return &cp
}
