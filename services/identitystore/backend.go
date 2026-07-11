package identitystore

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// maxIsMemberInGroupsIDs is the maximum number of GroupIds allowed per IsMemberInGroups request.
const maxIsMemberInGroupsIDs = 100

// defaultMaxResults is the page size used when MaxResults is 0 or > maxListPageSize.
const (
	defaultMaxResults = 100
	// maxListPageSize is the upper bound a caller may request per page.
	maxListPageSize = 100
	// attrUserNameKey is the lower-cased attribute path for username lookups.
	attrUserNameKey = "username"
)

// Sentinel errors.
var (
	// ErrUserNotFound is returned when a user is not found.
	ErrUserNotFound = errors.New("ResourceNotFoundException")
	// ErrGroupNotFound is returned when a group is not found.
	ErrGroupNotFound = errors.New("ResourceNotFoundException")
	// ErrMembershipNotFound is returned when a membership is not found.
	ErrMembershipNotFound = errors.New("ResourceNotFoundException")
	// ErrConflict is returned when a resource already exists.
	ErrConflict = errors.New("ConflictException")
	// ErrValidation is returned when request validation fails.
	ErrValidation = errors.New("ValidationException")
)

const (
	maxUserNameLength    = 128
	maxDisplayNameLength = 1024
	maxPhotosPerUser     = 3
)

const (
	userStatusEnabled  = "ENABLED"
	userStatusDisabled = "DISABLED"
)

// ----------------------------------------
// Domain models
// ----------------------------------------

// Name holds a user's name components.
type Name struct {
	Formatted       string `json:"Formatted,omitempty"`
	FamilyName      string `json:"FamilyName,omitempty"`
	GivenName       string `json:"GivenName,omitempty"`
	MiddleName      string `json:"MiddleName,omitempty"`
	HonorificPrefix string `json:"HonorificPrefix,omitempty"`
	HonorificSuffix string `json:"HonorificSuffix,omitempty"`
}

// Email holds email address information for a user.
type Email struct {
	Value   string `json:"Value,omitempty"`
	Type    string `json:"Type,omitempty"`
	Primary bool   `json:"Primary,omitempty"`
}

// Address holds address information for a user.
type Address struct {
	Formatted     string `json:"Formatted,omitempty"`
	StreetAddress string `json:"StreetAddress,omitempty"`
	Locality      string `json:"Locality,omitempty"`
	Region        string `json:"Region,omitempty"`
	PostalCode    string `json:"PostalCode,omitempty"`
	Country       string `json:"Country,omitempty"`
	Type          string `json:"Type,omitempty"`
	Primary       bool   `json:"Primary,omitempty"`
}

// PhoneNumber holds phone number information for a user.
type PhoneNumber struct {
	Value   string `json:"Value,omitempty"`
	Type    string `json:"Type,omitempty"`
	Primary bool   `json:"Primary,omitempty"`
}

// Photo holds photo information for a user. Users can have up to 3 photos.
type Photo struct {
	Value   string `json:"Value"`
	Display string `json:"Display,omitempty"`
	Type    string `json:"Type,omitempty"`
	Primary bool   `json:"Primary,omitempty"`
}

// Role holds role information for a user.
type Role struct {
	Value   string `json:"Value"`
	Type    string `json:"Type,omitempty"`
	Primary bool   `json:"Primary,omitempty"`
}

// ExternalID holds an external identity for a user (e.g. from SAML/SCIM providers).
type ExternalID struct {
	Issuer string `json:"Issuer"`
	ID     string `json:"Id"`
}

// User represents an identity store user.
// Field order below is fieldalignment-optimal (computed on an isolated
// scratch copy per this repo's fieldalignment-fix protocol, then hand-
// applied here) rather than the semantic grouping a human would pick; do not
// reorder without re-running fieldalignment.
type User struct {
	Name            *Name  `json:"Name,omitempty"`
	UserType        string `json:"UserType,omitempty"`
	Website         string `json:"Website,omitempty"`
	DisplayName     string `json:"DisplayName,omitempty"`
	NickName        string `json:"NickName,omitempty"`
	Title           string `json:"Title,omitempty"`
	ProfileURL      string `json:"ProfileUrl,omitempty"`
	Locale          string `json:"Locale,omitempty"`
	PreferredLang   string `json:"PreferredLanguage,omitempty"`
	Timezone        string `json:"Timezone,omitempty"`
	UserID          string `json:"UserId"`
	UserName        string `json:"UserName,omitempty"`
	UserStatus      string `json:"UserStatus,omitempty"`
	Birthdate       string `json:"Birthdate,omitempty"`
	IdentityStoreID string `json:"IdentityStoreId"`
	// region is hidden from JSON (unexported): it is used only to derive the
	// store.Table composite primary key and index keys below, mirroring
	// services/emr's Cluster.region. See store_setup.go and persistence.go.
	region       string
	Addresses    []Address     `json:"Addresses,omitempty"`
	PhoneNumbers []PhoneNumber `json:"PhoneNumbers,omitempty"`
	Photos       []Photo       `json:"Photos,omitempty"`
	Roles        []Role        `json:"Roles,omitempty"`
	ExternalIDs  []ExternalID  `json:"ExternalIds,omitempty"`
	Emails       []Email       `json:"Emails,omitempty"`
}

// Group represents an identity store group.
// Field order below is fieldalignment-optimal; see the note on User.
type Group struct {
	GroupID         string `json:"GroupId"`
	IdentityStoreID string `json:"IdentityStoreId"`
	DisplayName     string `json:"DisplayName,omitempty"`
	Description     string `json:"Description,omitempty"`
	// region is hidden from JSON (unexported); see User.region.
	region      string
	ExternalIDs []ExternalID `json:"ExternalIds,omitempty"`
}

// MemberID holds a membership member reference.
type MemberID struct {
	UserID string `json:"UserId,omitempty"`
}

// GroupMembership represents a group membership record.
type GroupMembership struct {
	MembershipID    string   `json:"MembershipId"`
	IdentityStoreID string   `json:"IdentityStoreId"`
	GroupID         string   `json:"GroupId"`
	MemberID        MemberID `json:"MemberId"`
	// region is hidden from JSON (unexported); see User.region.
	region string
}

// GroupMembershipExistence is the result item for IsMemberInGroups.
type GroupMembershipExistence struct {
	GroupID          string   `json:"GroupId"`
	MemberID         MemberID `json:"MemberId"`
	MembershipExists bool     `json:"MembershipExists"`
}

// ----------------------------------------
// InMemoryBackend
// ----------------------------------------

// InMemoryBackend is the in-memory store for the Identity Store service.
//
// Users, groups, and memberships were previously nested by region
// (map[region]map[id]*T, with IdentityStoreID carried as a field for
// per-store filtering within a region). Phase 3.3 flattens each into a
// single *store.Table keyed by the composite "region#id" string (regionKey
// in this file), with secondary *store.Index lookups replacing the old
// per-region reverse-lookup maps -- see store_setup.go for every key
// function and index, and persistence.go for how the hidden `region` field
// each value type carries is round-tripped through Snapshot/Restore.
// Callers must hold b.mu.
type InMemoryBackend struct {
	users                    *store.Table[User]
	usersByStore             *store.Index[User]
	usersByUserName          *store.Index[User]
	usersByPrimaryEmail      *store.Index[User]
	groups                   *store.Table[Group]
	groupsByStore            *store.Index[Group]
	groupsByDisplayName      *store.Index[Group]
	memberships              *store.Table[GroupMembership]
	membershipsByGroup       *store.Index[GroupMembership]
	membershipsByMember      *store.Index[GroupMembership]
	membershipsByGroupMember *store.Index[GroupMembership]
	registry                 *store.Registry
	mu                       *lockmetrics.RWMutex
	accountID                string
	region                   string
	counter                  int
}

// NewInMemoryBackend creates a new InMemoryBackend with the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if accountID == "" {
		accountID = config.DefaultAccountID
	}

	if region == "" {
		region = config.DefaultRegion
	}

	b := &InMemoryBackend{
		accountID: accountID,
		region:    region,
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New("identitystore"),
	}

	registerAllTables(b)

	return b
}

// regionKey builds the composite store.Table primary key ("region#id") for
// the resources flattened from this backend's old region-nested maps.
func regionKey(region, id string) string { return region + "#" + id }

// storeKey builds the composite index key ("region#storeID") isolating an
// identity store within a region -- the shared prefix for every
// byStore/byUserName/byDisplayName/byGroup/byMember/byGroupMember index key
// in store_setup.go.
func storeKey(region, storeID string) string { return region + "#" + storeID }

// Region returns the backend default region.
func (b *InMemoryBackend) Region() string { return b.region }

// generateID creates a UUID-format unique ID matching the AWS Identity Store format.
func (b *InMemoryBackend) generateID() string {
	return uuid.New().String()
}

// ----------------------------------------
// User operations
// ----------------------------------------

// CreateUserRequest holds the parameters for creating a user.
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

// CreateGroupRequest holds the parameters for creating a group.
type CreateGroupRequest struct {
	DisplayName string       `json:"DisplayName"`
	Description string       `json:"Description"`
	ExternalIDs []ExternalID `json:"ExternalIds"`
}

// CreateUser creates a new user in the identity store.
func (b *InMemoryBackend) CreateUser(ctx context.Context, storeID string, req *CreateUserRequest) (*User, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if len(req.UserName) > maxUserNameLength {
		return nil, fmt.Errorf("%w: UserName must not exceed 128 characters", ErrValidation)
	}

	if req.UserName != "" {
		if existing := b.usersByUserName.Get(storeKey(region, storeID) + "#" + req.UserName); len(existing) > 0 {
			return nil, fmt.Errorf("%w: user with UserName %q already exists", ErrConflict, req.UserName)
		}
	}

	if len(req.Photos) > maxPhotosPerUser {
		return nil, fmt.Errorf("%w: Photos must not exceed %d items", ErrValidation, maxPhotosPerUser)
	}

	if req.Birthdate != "" && !isValidBirthdate(req.Birthdate) {
		return nil, fmt.Errorf("%w: Birthdate must be in YYYY-MM-DD format", ErrValidation)
	}

	userID := b.generateID()
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

	if err := b.validateUsernameRename(region, storeID, user.UserName, ops); err != nil {
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

// attrDisplayName is the normalized attribute path for a group's display name.
const attrDisplayName = "displayname"

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
	case "nickname":
		user.NickName = strVal
	case "title":
		user.Title = strVal
	case "profileurl":
		user.ProfileURL = strVal
	case "locale":
		user.Locale = strVal
	case "preferredlanguage":
		user.PreferredLang = strVal
	case "timezone":
		user.Timezone = strVal
	case "usertype":
		user.UserType = strVal
	case "birthdate":
		user.Birthdate = strVal
	case "website":
		user.Website = strVal
	case "userstatus":
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
	case "emails":
		user.Emails = parseEmails(value)
	case "addresses":
		user.Addresses = parseAddresses(value)
	case "phonenumbers":
		user.PhoneNumbers = parsePhoneNumbers(value)
	case "photos":
		user.Photos = parsePhotos(value)
	case "roles":
		user.Roles = parseRoles(value)
	case "externalids":
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
	case "name.givenname":
		ensureUserName(user)
		user.Name.GivenName = strVal
	case "name.familyname":
		ensureUserName(user)
		user.Name.FamilyName = strVal
	case "name.middlename":
		ensureUserName(user)
		user.Name.MiddleName = strVal
	case "name.formatted":
		ensureUserName(user)
		user.Name.Formatted = strVal
	case "name.honorificprefix":
		ensureUserName(user)
		user.Name.HonorificPrefix = strVal
	case "name.honorificsuffix":
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

// paginateSlice returns a single page of items from a slice and the next-page
// token. maxResults <= 0 means "use default". A nil token is returned when
// there are no more pages.
func paginateSlice[T any](items []T, maxResults int32, nextToken string) ([]T, *string) {
	if len(items) == 0 {
		return items, nil
	}

	// Decode the offset from the cursor token.
	offset := 0
	if nextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(nextToken); decErr == nil {
			if n, atoiErr := strconv.Atoi(string(decoded)); atoiErr == nil && n >= 0 {
				offset = n
			}
		}
	}

	if offset >= len(items) {
		return nil, nil
	}

	items = items[offset:]

	limit := int(maxResults)
	if limit <= 0 || limit > maxListPageSize {
		limit = defaultMaxResults
	}

	if limit >= len(items) {
		return items, nil
	}

	page := items[:limit]
	encoded := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset + limit)))

	return page, &encoded
}

// ListFilter is a single attribute-equality filter for list operations.
type ListFilter struct {
	AttributePath  string `json:"AttributePath"`
	AttributeValue string `json:"AttributeValue"`
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

// matchUserSingleValueFilter checks filters on simple scalar fields.
func matchUserSingleValueFilter(u *User, f ListFilter) bool {
	switch strings.ToLower(f.AttributePath) {
	case attrUserNameKey:
		return u.UserName == f.AttributeValue
	case attrDisplayName:
		return u.DisplayName == f.AttributeValue
	case "name.givenname":
		return u.Name != nil && u.Name.GivenName == f.AttributeValue
	case "name.familyname":
		return u.Name != nil && u.Name.FamilyName == f.AttributeValue
	case "title":
		return u.Title == f.AttributeValue
	case "nickname":
		return u.NickName == f.AttributeValue
	case "usertype":
		return u.UserType == f.AttributeValue
	case "preferredlanguage":
		return u.PreferredLang == f.AttributeValue
	case "locale":
		return u.Locale == f.AttributeValue
	case "timezone":
		return u.Timezone == f.AttributeValue
	}

	return true
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

// applyGroupFilters returns only the groups matching all provided filters.
func applyGroupFilters(groups []*Group, filters []ListFilter) []*Group {
	if len(filters) == 0 {
		return groups
	}

	result := make([]*Group, 0, len(groups))

	for _, g := range groups {
		if groupMatchesFilters(g, filters) {
			result = append(result, g)
		}
	}

	return result
}

// groupMatchesFilters reports whether g satisfies every filter in the slice.
func groupMatchesFilters(g *Group, filters []ListFilter) bool {
	for _, f := range filters {
		switch strings.ToLower(f.AttributePath) {
		case attrDisplayName:
			if g.DisplayName != f.AttributeValue {
				return false
			}
		case "description":
			if g.Description != f.AttributeValue {
				return false
			}
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

// splitExternalIDCompound splits a compound ExternalId key (encoded by extractAlternateIdentifier)
// into its Issuer and Id parts. Both Issuer and Id must match for a lookup to succeed.
func splitExternalIDCompound(compound string) (string, string) {
	if i := strings.IndexByte(compound, externalIDSep[0]); i >= 0 {
		return compound[:i], compound[i+1:]
	}

	return "", compound
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

// resolveGroupByExternalID returns the group ID whose ExternalIDs contain both the given Issuer and Id.
// The compound argument is Issuer+externalIDSep+Id as encoded by extractAlternateIdentifier.
func (b *InMemoryBackend) resolveGroupByExternalID(region, storeID, compound string) (string, bool) {
	issuer, extID := splitExternalIDCompound(compound)

	for _, g := range b.groupsByStore.Get(storeKey(region, storeID)) {
		for _, ext := range g.ExternalIDs {
			if ext.Issuer == issuer && ext.ID == extID {
				return g.GroupID, true
			}
		}
	}

	return "", false
}

// ----------------------------------------
// Group operations
// ----------------------------------------

// CreateGroup creates a new group in the identity store.
func (b *InMemoryBackend) CreateGroup(ctx context.Context, storeID string, req *CreateGroupRequest) (*Group, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	// Check uniqueness by DisplayName using index.
	if req.DisplayName != "" {
		if existing := b.groupsByDisplayName.Get(storeKey(region, storeID) + "#" + req.DisplayName); len(existing) > 0 {
			return nil, fmt.Errorf("%w: group with DisplayName %q already exists", ErrConflict, req.DisplayName)
		}
	}

	if len(req.DisplayName) > maxDisplayNameLength {
		return nil, fmt.Errorf("%w: DisplayName must not exceed 1024 characters", ErrValidation)
	}

	groupID := b.generateID()
	group := &Group{
		GroupID:         groupID,
		IdentityStoreID: storeID,
		DisplayName:     req.DisplayName,
		Description:     req.Description,
		ExternalIDs:     req.ExternalIDs,
		region:          region,
	}

	b.groups.Put(group)

	return copyGroup(group), nil
}

// DescribeGroup returns a group by ID.
func (b *InMemoryBackend) DescribeGroup(ctx context.Context, storeID, groupID string) (*Group, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeGroup")
	defer b.mu.RUnlock()

	group, ok := b.groups.Get(regionKey(region, groupID))
	if !ok || group.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	return copyGroup(group), nil
}

// ListGroups lists all groups for the given identity store, sorted by GroupID.
func (b *InMemoryBackend) ListGroups(ctx context.Context, storeID string) []*Group {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	matches := b.groupsByStore.Get(storeKey(region, storeID))
	result := make([]*Group, 0, len(matches))

	for _, g := range matches {
		result = append(result, copyGroup(g))
	}

	slices.SortFunc(result, func(a, b *Group) int { return strings.Compare(a.GroupID, b.GroupID) })

	return result
}

// UpdateGroup applies attribute operations to a group.
func (b *InMemoryBackend) UpdateGroup(ctx context.Context, storeID, groupID string, ops []attributeOperation) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	id := regionKey(region, groupID)

	group, ok := b.groups.Get(id)
	if !ok || group.IdentityStoreID != storeID {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	if err := b.validateGroupOps(region, storeID, group.DisplayName, ops); err != nil {
		return err
	}

	// Delete before mutating the indexed DisplayName field -- see the same
	// hazard note in UpdateUser above.
	b.groups.Delete(id)

	applyGroupAttributes(group, ops)

	b.groups.Put(group)

	return nil
}

// validateGroupOps checks for display-name conflicts before applying group updates.
func (b *InMemoryBackend) validateGroupOps(region, storeID, currentDisplayName string, ops []attributeOperation) error {
	for _, op := range ops {
		if strings.ToLower(op.AttributePath) != attrDisplayName {
			continue
		}

		newName, _ := op.AttributeValue.(string)
		if newName == "" || newName == currentDisplayName {
			continue
		}

		if existing := b.groupsByDisplayName.Get(storeKey(region, storeID) + "#" + newName); len(existing) > 0 {
			return fmt.Errorf("%w: group with DisplayName %q already exists", ErrConflict, newName)
		}
	}

	return nil
}

// applyGroupAttributes applies each attribute operation to the group in place.
func applyGroupAttributes(group *Group, ops []attributeOperation) {
	for _, op := range ops {
		switch strings.ToLower(op.AttributePath) {
		case attrDisplayName:
			if s, isStr := op.AttributeValue.(string); isStr {
				group.DisplayName = s
			}
		case "description":
			if s, isStr := op.AttributeValue.(string); isStr {
				group.Description = s
			}
		}
	}
}

// DeleteGroup removes a group from the identity store.
func (b *InMemoryBackend) DeleteGroup(ctx context.Context, storeID, groupID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	id := regionKey(region, groupID)

	group, ok := b.groups.Get(id)
	if !ok || group.IdentityStoreID != storeID {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	b.groups.Delete(id)

	// Remove associated memberships via the byGroup index. Clone the
	// index-owned slice before the delete loop, since Table.Delete mutates
	// the same index's backing storage as it removes each entry.
	groupKey := storeKey(region, storeID) + "#" + groupID

	for _, m := range slices.Clone(b.membershipsByGroup.Get(groupKey)) {
		b.memberships.Delete(regionKey(region, m.MembershipID))
	}

	return nil
}

// GetGroupID looks up a group ID by alternate identifier (DisplayName or ExternalId).
func (b *InMemoryBackend) GetGroupID(ctx context.Context, storeID, attrPath, attrValue string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetGroupID")
	defer b.mu.RUnlock()

	switch {
	case strings.EqualFold(attrPath, "displayName"):
		if matches := b.groupsByDisplayName.Get(storeKey(region, storeID) + "#" + attrValue); len(matches) > 0 {
			return matches[0].GroupID, nil
		}
	case strings.EqualFold(attrPath, "ExternalId"):
		if gid, ok := b.resolveGroupByExternalID(region, storeID, attrValue); ok {
			return gid, nil
		}
	}

	return "", fmt.Errorf("%w: no group found with %s=%q", ErrGroupNotFound, attrPath, attrValue)
}

// ----------------------------------------
// Membership operations
// ----------------------------------------

// CreateGroupMembership creates a membership between a user and a group.
func (b *InMemoryBackend) CreateGroupMembership(
	ctx context.Context, storeID, groupID string, memberID MemberID,
) (*GroupMembership, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateGroupMembership")
	defer b.mu.Unlock()

	// Validate group exists.
	group, ok := b.groups.Get(regionKey(region, groupID))
	if !ok || group.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	// Validate user exists.
	if memberID.UserID != "" {
		user, userOK := b.users.Get(regionKey(region, memberID.UserID))
		if !userOK || user.IdentityStoreID != storeID {
			return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, memberID.UserID)
		}
	}

	// Check for duplicate membership using the byGroupMember index.
	groupMemberKey := storeKey(region, storeID) + "#" + groupID + "#" + memberID.UserID
	if existing := b.membershipsByGroupMember.Get(groupMemberKey); len(existing) > 0 {
		return nil, fmt.Errorf("%w: membership already exists", ErrConflict)
	}

	membershipID := b.generateID()
	membership := &GroupMembership{
		MembershipID:    membershipID,
		IdentityStoreID: storeID,
		GroupID:         groupID,
		MemberID:        memberID,
		region:          region,
	}

	b.memberships.Put(membership)

	return copyMembership(membership), nil
}

// DescribeGroupMembership returns a membership by ID.
func (b *InMemoryBackend) DescribeGroupMembership(
	ctx context.Context, storeID, membershipID string,
) (*GroupMembership, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeGroupMembership")
	defer b.mu.RUnlock()

	m, ok := b.memberships.Get(regionKey(region, membershipID))
	if !ok || m.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: membership %q not found", ErrMembershipNotFound, membershipID)
	}

	return copyMembership(m), nil
}

// ListGroupMemberships lists all memberships for a group, sorted by MembershipID.
func (b *InMemoryBackend) ListGroupMemberships(ctx context.Context, storeID, groupID string) []*GroupMembership {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListGroupMemberships")
	defer b.mu.RUnlock()

	matches := b.membershipsByGroup.Get(storeKey(region, storeID) + "#" + groupID)
	result := make([]*GroupMembership, 0, len(matches))

	for _, m := range matches {
		result = append(result, copyMembership(m))
	}

	slices.SortFunc(result, func(a, b *GroupMembership) int {
		return strings.Compare(a.MembershipID, b.MembershipID)
	})

	return result
}

// DeleteGroupMembership removes a membership.
func (b *InMemoryBackend) DeleteGroupMembership(ctx context.Context, storeID, membershipID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteGroupMembership")
	defer b.mu.Unlock()

	id := regionKey(region, membershipID)

	m, ok := b.memberships.Get(id)
	if !ok || m.IdentityStoreID != storeID {
		return fmt.Errorf("%w: membership %q not found", ErrMembershipNotFound, membershipID)
	}

	b.memberships.Delete(id)

	return nil
}

// GetGroupMembershipID looks up a membership ID by group and member.
func (b *InMemoryBackend) GetGroupMembershipID(
	ctx context.Context, storeID, groupID string, memberID MemberID,
) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetGroupMembershipID")
	defer b.mu.RUnlock()

	key := storeKey(region, storeID) + "#" + groupID + "#" + memberID.UserID
	if matches := b.membershipsByGroupMember.Get(key); len(matches) > 0 {
		return matches[0].MembershipID, nil
	}

	return "", fmt.Errorf(
		"%w: membership not found for group=%q member=%q",
		ErrMembershipNotFound,
		groupID,
		memberID.UserID,
	)
}

// ListGroupMembershipsForMember lists all group memberships for a given member, sorted by MembershipID.
func (b *InMemoryBackend) ListGroupMembershipsForMember(
	ctx context.Context, storeID string, memberID MemberID,
) []*GroupMembership {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListGroupMembershipsForMember")
	defer b.mu.RUnlock()

	if memberID.UserID == "" {
		return nil
	}

	matches := b.membershipsByMember.Get(storeKey(region, storeID) + "#" + memberID.UserID)
	result := make([]*GroupMembership, 0, len(matches))

	for _, m := range matches {
		result = append(result, copyMembership(m))
	}

	slices.SortFunc(result, func(a, b *GroupMembership) int {
		return strings.Compare(a.MembershipID, b.MembershipID)
	})

	return result
}

// IsMemberInGroups checks which of the given groups contain the specified member.
// Uses the O(1) byGroupMember index instead of scanning all memberships.
func (b *InMemoryBackend) IsMemberInGroups(
	ctx context.Context,
	storeID string,
	memberID MemberID,
	groupIDs []string,
) []GroupMembershipExistence {
	region := getRegion(ctx, b.region)

	b.mu.RLock("IsMemberInGroups")
	defer b.mu.RUnlock()

	result := make([]GroupMembershipExistence, 0, len(groupIDs))

	for _, id := range groupIDs {
		key := storeKey(region, storeID) + "#" + id + "#" + memberID.UserID
		exists := len(b.membershipsByGroupMember.Get(key)) > 0
		result = append(result, GroupMembershipExistence{
			GroupID:          id,
			MemberID:         memberID,
			MembershipExists: exists,
		})
	}

	return result
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

func copyGroup(g *Group) *Group {
	if g == nil {
		return nil
	}
	cp := *g
	cp.ExternalIDs = append([]ExternalID(nil), g.ExternalIDs...)

	return &cp
}

func copyMembership(m *GroupMembership) *GroupMembership {
	if m == nil {
		return nil
	}
	cp := *m

	return &cp
}

// Reset clears all user, group, and membership state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.counter = 0
}
