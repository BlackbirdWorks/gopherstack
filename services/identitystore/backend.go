package identitystore

import (
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

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
type User struct {
	UserID          string        `json:"UserId"`
	IdentityStoreID string        `json:"IdentityStoreId"`
	UserName        string        `json:"UserName,omitempty"`
	DisplayName     string        `json:"DisplayName,omitempty"`
	NickName        string        `json:"NickName,omitempty"`
	Title           string        `json:"Title,omitempty"`
	ProfileURL      string        `json:"ProfileUrl,omitempty"`
	Locale          string        `json:"Locale,omitempty"`
	PreferredLang   string        `json:"PreferredLanguage,omitempty"`
	Timezone        string        `json:"Timezone,omitempty"`
	UserType        string        `json:"UserType,omitempty"`
	Birthdate       string        `json:"Birthdate,omitempty"`
	Website         string        `json:"Website,omitempty"`
	UserStatus      string        `json:"UserStatus,omitempty"`
	Name            *Name         `json:"Name,omitempty"`
	Emails          []Email       `json:"Emails,omitempty"`
	Addresses       []Address     `json:"Addresses,omitempty"`
	PhoneNumbers    []PhoneNumber `json:"PhoneNumbers,omitempty"`
	Photos          []Photo       `json:"Photos,omitempty"`
	Roles           []Role        `json:"Roles,omitempty"`
	ExternalIDs     []ExternalID  `json:"ExternalIds,omitempty"`
}

// Group represents an identity store group.
type Group struct {
	GroupID         string       `json:"GroupId"`
	IdentityStoreID string       `json:"IdentityStoreId"`
	DisplayName     string       `json:"DisplayName,omitempty"`
	Description     string       `json:"Description,omitempty"`
	ExternalIDs     []ExternalID `json:"ExternalIds,omitempty"`
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
type InMemoryBackend struct {
	users             map[string]*User
	groups            map[string]*Group
	memberships       map[string]*GroupMembership
	usersByName       map[string]string   // storeID#username -> userID
	groupsByName      map[string]string   // storeID#displayName -> groupID
	membershipKeys    map[string]string   // storeID#groupID#userID -> membershipID
	usersByEmail      map[string]string   // storeID#email -> userID (primary email)
	membershipsByUser map[string][]string // storeID#userID -> []membershipID
	mu                *lockmetrics.RWMutex
	accountID         string
	region            string
	counter           int
}

// NewInMemoryBackend creates a new InMemoryBackend with the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if accountID == "" {
		accountID = config.DefaultAccountID
	}

	if region == "" {
		region = config.DefaultRegion
	}

	return &InMemoryBackend{
		accountID:         accountID,
		region:            region,
		users:             make(map[string]*User),
		groups:            make(map[string]*Group),
		memberships:       make(map[string]*GroupMembership),
		usersByName:       make(map[string]string),
		groupsByName:      make(map[string]string),
		membershipKeys:    make(map[string]string),
		usersByEmail:      make(map[string]string),
		membershipsByUser: make(map[string][]string),
		mu:                lockmetrics.New("identitystore"),
	}
}

// Region returns the backend region.
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
func (b *InMemoryBackend) CreateUser(storeID string, req *CreateUserRequest) (*User, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if len(req.UserName) > maxUserNameLength {
		return nil, fmt.Errorf("%w: UserName must not exceed 128 characters", ErrValidation)
	}

	if req.UserName != "" {
		if _, exists := b.usersByName[storeID+"#"+req.UserName]; exists {
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
	}

	b.users[userID] = user

	if req.UserName != "" {
		b.usersByName[storeID+"#"+req.UserName] = userID
	}

	// Index primary email for O(1) GetUserID by email.
	if pe := userPrimaryEmail(req.Emails); pe != "" {
		b.usersByEmail[storeID+"#"+pe] = userID
	}

	return copyUser(user), nil
}

// DescribeUser returns a user by ID.
func (b *InMemoryBackend) DescribeUser(storeID, userID string) (*User, error) {
	b.mu.RLock("DescribeUser")
	defer b.mu.RUnlock()

	user, ok := b.users[userID]
	if !ok || user.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userID)
	}

	return copyUser(user), nil
}

// ListUsers lists all users for the given identity store, sorted by UserID.
func (b *InMemoryBackend) ListUsers(storeID string) []*User {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	result := make([]*User, 0, len(b.users))

	for _, u := range b.users {
		if u.IdentityStoreID == storeID {
			result = append(result, copyUser(u))
		}
	}

	slices.SortFunc(result, func(a, b *User) int { return strings.Compare(a.UserID, b.UserID) })

	return result
}

// UpdateUser applies attribute operations to a user.
func (b *InMemoryBackend) UpdateUser(storeID, userID string, ops []attributeOperation) error {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	user, ok := b.users[userID]
	if !ok || user.IdentityStoreID != storeID {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userID)
	}

	oldUserName := user.UserName
	oldEmail := userPrimaryEmail(user.Emails)

	if err := b.validateUsernameRename(storeID, oldUserName, ops); err != nil {
		return err
	}

	for _, op := range ops {
		applyUserAttribute(user, op.AttributePath, op.AttributeValue)
	}

	b.updateUserNameIndex(storeID, userID, oldUserName, user.UserName)
	b.updateEmailIndex(storeID, userID, oldEmail, userPrimaryEmail(user.Emails))

	return nil
}

// validateUsernameRename checks that no username-rename operation would produce a conflict.
func (b *InMemoryBackend) validateUsernameRename(storeID, oldName string, ops []attributeOperation) error {
	for _, op := range ops {
		if strings.ToLower(op.AttributePath) != attrUserNameKey {
			continue
		}

		newName, _ := op.AttributeValue.(string)
		if newName == "" || newName == oldName {
			continue
		}

		if _, exists := b.usersByName[storeID+"#"+newName]; exists {
			return fmt.Errorf("%w: user with UserName %q already exists", ErrConflict, newName)
		}
	}

	return nil
}

// updateUserNameIndex maintains the usersByName index when a username changes.
func (b *InMemoryBackend) updateUserNameIndex(storeID, userID, oldName, newName string) {
	if oldName == newName {
		return
	}

	if oldName != "" {
		delete(b.usersByName, storeID+"#"+oldName)
	}

	if newName != "" {
		b.usersByName[storeID+"#"+newName] = userID
	}
}

// updateEmailIndex maintains the usersByEmail index when a primary email changes.
func (b *InMemoryBackend) updateEmailIndex(storeID, userID, oldEmail, newEmail string) {
	if oldEmail == newEmail {
		return
	}

	if oldEmail != "" {
		delete(b.usersByEmail, storeID+"#"+oldEmail)
	}

	if newEmail != "" {
		b.usersByEmail[storeID+"#"+newEmail] = userID
	}
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
func (b *InMemoryBackend) DeleteUser(storeID, userID string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	user, ok := b.users[userID]
	if !ok || user.IdentityStoreID != storeID {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userID)
	}

	if user.UserName != "" {
		delete(b.usersByName, storeID+"#"+user.UserName)
	}

	// Remove primary email from index.
	if pe := userPrimaryEmail(user.Emails); pe != "" {
		delete(b.usersByEmail, storeID+"#"+pe)
	}

	delete(b.users, userID)

	// Use the inverted index for O(1) cascade membership deletion.
	userKey := storeID + "#" + userID
	for _, id := range b.membershipsByUser[userKey] {
		if m, exists := b.memberships[id]; exists {
			delete(b.membershipKeys, storeID+"#"+m.GroupID+"#"+userID)
			delete(b.memberships, id)
		}
	}

	delete(b.membershipsByUser, userKey)

	return nil
}

// GetUserID looks up a user ID by alternate identifier (UserName, email, or ExternalId).
func (b *InMemoryBackend) GetUserID(storeID, attrPath, attrValue string) (string, error) {
	b.mu.RLock("GetUserID")
	defer b.mu.RUnlock()

	uid, found := b.resolveUserByAttr(storeID, attrPath, attrValue)
	if !found {
		return "", fmt.Errorf("%w: no user found with %s=%q", ErrUserNotFound, attrPath, attrValue)
	}

	return uid, nil
}

// resolveUserByAttr returns the user ID matching the given attribute path and value.
func (b *InMemoryBackend) resolveUserByAttr(storeID, attrPath, attrValue string) (string, bool) {
	switch {
	case strings.EqualFold(attrPath, attrUserNameKey):
		uid, ok := b.usersByName[storeID+"#"+attrValue]

		return uid, ok
	case strings.EqualFold(attrPath, "emails.value"):
		return b.resolveUserByEmail(storeID, attrValue)
	case strings.EqualFold(attrPath, "externalid"):
		return b.resolveUserByExternalID(storeID, attrValue)
	}

	return "", false
}

// resolveUserByEmail returns the user ID matching the given email address.
func (b *InMemoryBackend) resolveUserByEmail(storeID, email string) (string, bool) {
	// Fast path via primary-email index.
	if uid, ok := b.usersByEmail[storeID+"#"+email]; ok {
		return uid, true
	}

	// Slow path: scan all non-primary emails.
	for _, u := range b.users {
		if u.IdentityStoreID != storeID {
			continue
		}

		for _, e := range u.Emails {
			if e.Value == email {
				return u.UserID, true
			}
		}
	}

	return "", false
}

// resolveUserByExternalID returns the user ID whose ExternalIDs contain the given ID.
func (b *InMemoryBackend) resolveUserByExternalID(storeID, extID string) (string, bool) {
	for _, u := range b.users {
		if u.IdentityStoreID != storeID {
			continue
		}

		for _, ext := range u.ExternalIDs {
			if ext.ID == extID {
				return u.UserID, true
			}
		}
	}

	return "", false
}

// ----------------------------------------
// Group operations
// ----------------------------------------

// CreateGroup creates a new group in the identity store.
func (b *InMemoryBackend) CreateGroup(storeID string, req *CreateGroupRequest) (*Group, error) {
	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	// Check uniqueness by DisplayName using index.
	if req.DisplayName != "" {
		if _, exists := b.groupsByName[storeID+"#"+req.DisplayName]; exists {
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
	}

	b.groups[groupID] = group

	if req.DisplayName != "" {
		b.groupsByName[storeID+"#"+req.DisplayName] = groupID
	}

	return copyGroup(group), nil
}

// DescribeGroup returns a group by ID.
func (b *InMemoryBackend) DescribeGroup(storeID, groupID string) (*Group, error) {
	b.mu.RLock("DescribeGroup")
	defer b.mu.RUnlock()

	group, ok := b.groups[groupID]
	if !ok || group.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	return copyGroup(group), nil
}

// ListGroups lists all groups for the given identity store, sorted by GroupID.
func (b *InMemoryBackend) ListGroups(storeID string) []*Group {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	result := make([]*Group, 0, len(b.groups))

	for _, g := range b.groups {
		if g.IdentityStoreID == storeID {
			result = append(result, copyGroup(g))
		}
	}

	slices.SortFunc(result, func(a, b *Group) int { return strings.Compare(a.GroupID, b.GroupID) })

	return result
}

// UpdateGroup applies attribute operations to a group.
func (b *InMemoryBackend) UpdateGroup(storeID, groupID string, ops []attributeOperation) error {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	group, ok := b.groups[groupID]
	if !ok || group.IdentityStoreID != storeID {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	if err := b.validateGroupOps(storeID, group.DisplayName, ops); err != nil {
		return err
	}

	oldDisplayName := group.DisplayName

	applyGroupAttributes(group, ops)

	b.updateGroupDisplayNameIndex(storeID, groupID, oldDisplayName, group.DisplayName)

	return nil
}

// validateGroupOps checks for display-name conflicts before applying group updates.
func (b *InMemoryBackend) validateGroupOps(storeID, currentDisplayName string, ops []attributeOperation) error {
	for _, op := range ops {
		if strings.ToLower(op.AttributePath) != attrDisplayName {
			continue
		}

		newName, _ := op.AttributeValue.(string)
		if newName == "" || newName == currentDisplayName {
			continue
		}

		if _, exists := b.groupsByName[storeID+"#"+newName]; exists {
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

// updateGroupDisplayNameIndex maintains the groupsByName index when a display name changes.
func (b *InMemoryBackend) updateGroupDisplayNameIndex(storeID, groupID, oldName, newName string) {
	if oldName == newName {
		return
	}

	if oldName != "" {
		delete(b.groupsByName, storeID+"#"+oldName)
	}

	if newName != "" {
		b.groupsByName[storeID+"#"+newName] = groupID
	}
}

// DeleteGroup removes a group from the identity store.
func (b *InMemoryBackend) DeleteGroup(storeID, groupID string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	group, ok := b.groups[groupID]
	if !ok || group.IdentityStoreID != storeID {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	if group.DisplayName != "" {
		delete(b.groupsByName, storeID+"#"+group.DisplayName)
	}

	delete(b.groups, groupID)

	// Remove associated memberships. Collect IDs first to avoid map mutation during iteration.
	var toDelete []string

	for id, m := range b.memberships {
		if m.IdentityStoreID == storeID && m.GroupID == groupID {
			toDelete = append(toDelete, id)
		}
	}

	for _, id := range toDelete {
		m := b.memberships[id]
		delete(b.membershipKeys, storeID+"#"+groupID+"#"+m.MemberID.UserID)
		delete(b.memberships, id)

		// Remove from per-user inverted index.
		userKey := storeID + "#" + m.MemberID.UserID
		updated := make([]string, 0, len(b.membershipsByUser[userKey]))
		for _, mid := range b.membershipsByUser[userKey] {
			if mid != id {
				updated = append(updated, mid)
			}
		}

		if len(updated) == 0 {
			delete(b.membershipsByUser, userKey)
		} else {
			b.membershipsByUser[userKey] = updated
		}
	}

	return nil
}

// GetGroupID looks up a group ID by alternate identifier (DisplayName).
func (b *InMemoryBackend) GetGroupID(storeID, attrPath, attrValue string) (string, error) {
	b.mu.RLock("GetGroupID")
	defer b.mu.RUnlock()

	if strings.EqualFold(attrPath, "displayName") {
		if gid, ok := b.groupsByName[storeID+"#"+attrValue]; ok {
			return gid, nil
		}
	}

	return "", fmt.Errorf("%w: no group found with %s=%q", ErrGroupNotFound, attrPath, attrValue)
}

// ----------------------------------------
// Membership operations
// ----------------------------------------

// CreateGroupMembership creates a membership between a user and a group.
func (b *InMemoryBackend) CreateGroupMembership(storeID, groupID string, memberID MemberID) (*GroupMembership, error) {
	b.mu.Lock("CreateGroupMembership")
	defer b.mu.Unlock()

	// Validate group exists.
	group, ok := b.groups[groupID]
	if !ok || group.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	// Validate user exists.
	if memberID.UserID != "" {
		user, userOK := b.users[memberID.UserID]
		if !userOK || user.IdentityStoreID != storeID {
			return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, memberID.UserID)
		}
	}

	// Check for duplicate membership using index.
	key := storeID + "#" + groupID + "#" + memberID.UserID
	if _, exists := b.membershipKeys[key]; exists {
		return nil, fmt.Errorf("%w: membership already exists", ErrConflict)
	}

	membershipID := b.generateID()
	membership := &GroupMembership{
		MembershipID:    membershipID,
		IdentityStoreID: storeID,
		GroupID:         groupID,
		MemberID:        memberID,
	}

	b.memberships[membershipID] = membership
	b.membershipKeys[key] = membershipID

	// Maintain inverted index for O(1) cascade deletes on user removal.
	userKey := storeID + "#" + memberID.UserID
	b.membershipsByUser[userKey] = append(b.membershipsByUser[userKey], membershipID)

	return copyMembership(membership), nil
}

// DescribeGroupMembership returns a membership by ID.
func (b *InMemoryBackend) DescribeGroupMembership(storeID, membershipID string) (*GroupMembership, error) {
	b.mu.RLock("DescribeGroupMembership")
	defer b.mu.RUnlock()

	m, ok := b.memberships[membershipID]
	if !ok || m.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: membership %q not found", ErrMembershipNotFound, membershipID)
	}

	return copyMembership(m), nil
}

// ListGroupMemberships lists all memberships for a group, sorted by MembershipID.
func (b *InMemoryBackend) ListGroupMemberships(storeID, groupID string) []*GroupMembership {
	b.mu.RLock("ListGroupMemberships")
	defer b.mu.RUnlock()

	result := make([]*GroupMembership, 0, len(b.memberships))

	for _, m := range b.memberships {
		if m.IdentityStoreID == storeID && m.GroupID == groupID {
			result = append(result, copyMembership(m))
		}
	}

	slices.SortFunc(result, func(a, b *GroupMembership) int {
		return strings.Compare(a.MembershipID, b.MembershipID)
	})

	return result
}

// DeleteGroupMembership removes a membership.
func (b *InMemoryBackend) DeleteGroupMembership(storeID, membershipID string) error {
	b.mu.Lock("DeleteGroupMembership")
	defer b.mu.Unlock()

	m, ok := b.memberships[membershipID]
	if !ok || m.IdentityStoreID != storeID {
		return fmt.Errorf("%w: membership %q not found", ErrMembershipNotFound, membershipID)
	}

	delete(b.membershipKeys, storeID+"#"+m.GroupID+"#"+m.MemberID.UserID)
	delete(b.memberships, membershipID)

	// Remove from inverted index.
	userKey := storeID + "#" + m.MemberID.UserID
	ids := b.membershipsByUser[userKey]
	updated := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != membershipID {
			updated = append(updated, id)
		}
	}

	if len(updated) == 0 {
		delete(b.membershipsByUser, userKey)
	} else {
		b.membershipsByUser[userKey] = updated
	}

	return nil
}

// GetGroupMembershipID looks up a membership ID by group and member.
func (b *InMemoryBackend) GetGroupMembershipID(storeID, groupID string, memberID MemberID) (string, error) {
	b.mu.RLock("GetGroupMembershipID")
	defer b.mu.RUnlock()

	key := storeID + "#" + groupID + "#" + memberID.UserID
	if mid, ok := b.membershipKeys[key]; ok {
		return mid, nil
	}

	return "", fmt.Errorf(
		"%w: membership not found for group=%q member=%q",
		ErrMembershipNotFound,
		groupID,
		memberID.UserID,
	)
}

// ListGroupMembershipsForMember lists all group memberships for a given member, sorted by MembershipID.
func (b *InMemoryBackend) ListGroupMembershipsForMember(storeID string, memberID MemberID) []*GroupMembership {
	b.mu.RLock("ListGroupMembershipsForMember")
	defer b.mu.RUnlock()

	if memberID.UserID == "" {
		return nil
	}

	userKey := storeID + "#" + memberID.UserID
	ids := b.membershipsByUser[userKey]
	result := make([]*GroupMembership, 0, len(ids))

	for _, id := range ids {
		if m, ok := b.memberships[id]; ok {
			result = append(result, copyMembership(m))
		}
	}

	slices.SortFunc(result, func(a, b *GroupMembership) int {
		return strings.Compare(a.MembershipID, b.MembershipID)
	})

	return result
}

// IsMemberInGroups checks which of the given groups contain the specified member.
// Uses the O(1) membershipKeys index instead of scanning all memberships.
func (b *InMemoryBackend) IsMemberInGroups(
	storeID string,
	memberID MemberID,
	groupIDs []string,
) []GroupMembershipExistence {
	b.mu.RLock("IsMemberInGroups")
	defer b.mu.RUnlock()

	result := make([]GroupMembershipExistence, 0, len(groupIDs))

	for _, id := range groupIDs {
		key := storeID + "#" + id + "#" + memberID.UserID
		_, exists := b.membershipKeys[key]
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

func (b *InMemoryBackend) rebuildIndexes() {
	b.usersByName = make(map[string]string, len(b.users))
	b.groupsByName = make(map[string]string, len(b.groups))
	b.membershipKeys = make(map[string]string, len(b.memberships))
	b.usersByEmail = make(map[string]string, len(b.users))
	b.membershipsByUser = make(map[string][]string, len(b.memberships))

	for id, u := range b.users {
		if u.UserName != "" {
			b.usersByName[u.IdentityStoreID+"#"+u.UserName] = id
		}
		if pe := userPrimaryEmail(u.Emails); pe != "" {
			b.usersByEmail[u.IdentityStoreID+"#"+pe] = id
		}
	}
	for id, g := range b.groups {
		if g.DisplayName != "" {
			b.groupsByName[g.IdentityStoreID+"#"+g.DisplayName] = id
		}
	}
	for id, m := range b.memberships {
		key := m.IdentityStoreID + "#" + m.GroupID + "#" + m.MemberID.UserID
		b.membershipKeys[key] = id

		userKey := m.IdentityStoreID + "#" + m.MemberID.UserID
		b.membershipsByUser[userKey] = append(b.membershipsByUser[userKey], id)
	}
}

// Reset clears all user, group, and membership state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.users = make(map[string]*User)
	b.groups = make(map[string]*Group)
	b.memberships = make(map[string]*GroupMembership)
	b.usersByName = make(map[string]string)
	b.groupsByName = make(map[string]string)
	b.membershipKeys = make(map[string]string)
	b.usersByEmail = make(map[string]string)
	b.membershipsByUser = make(map[string][]string)
	b.counter = 0
}
