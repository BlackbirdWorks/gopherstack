package identitystore

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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
	Name            *Name         `json:"Name,omitempty"`
	Emails          []Email       `json:"Emails,omitempty"`
	Addresses       []Address     `json:"Addresses,omitempty"`
	PhoneNumbers    []PhoneNumber `json:"PhoneNumbers,omitempty"`
}

// Group represents an identity store group.
type Group struct {
	GroupID         string `json:"GroupId"`
	IdentityStoreID string `json:"IdentityStoreId"`
	DisplayName     string `json:"DisplayName,omitempty"`
	Description     string `json:"Description,omitempty"`
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
	GroupID          string `json:"GroupId"`
	MembershipExists bool   `json:"MembershipExists"`
}

// ----------------------------------------
// InMemoryBackend
// ----------------------------------------

// InMemoryBackend is the in-memory store for the Identity Store service.
type InMemoryBackend struct {
	users          map[string]*User
	groups         map[string]*Group
	memberships    map[string]*GroupMembership
	usersByName    map[string]string
	groupsByName   map[string]string
	membershipKeys map[string]string
	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
	counter        int
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
		accountID:      accountID,
		region:         region,
		users:          make(map[string]*User),
		groups:         make(map[string]*Group),
		memberships:    make(map[string]*GroupMembership),
		usersByName:    make(map[string]string),
		groupsByName:   make(map[string]string),
		membershipKeys: make(map[string]string),
		mu:             lockmetrics.New("identitystore"),
	}
}

// Region returns the backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// generateID creates a simple sequential unique ID.
func (b *InMemoryBackend) generateID(prefix string) string {
	b.counter++

	return fmt.Sprintf("%s-%08d", prefix, b.counter)
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
	Name          *Name         `json:"Name"`
	Emails        []Email       `json:"Emails"`
	Addresses     []Address     `json:"Addresses"`
	PhoneNumbers  []PhoneNumber `json:"PhoneNumbers"`
}

// CreateGroupRequest holds the parameters for creating a group.
type CreateGroupRequest struct {
	DisplayName string `json:"DisplayName"`
	Description string `json:"Description"`
}

// CreateUser creates a new user in the identity store.
func (b *InMemoryBackend) CreateUser(storeID string, req *CreateUserRequest) (*User, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	// Check uniqueness by UserName using index.
	if req.UserName != "" {
		if _, exists := b.usersByName[storeID+"#"+req.UserName]; exists {
			return nil, fmt.Errorf("%w: user with UserName %q already exists", ErrConflict, req.UserName)
		}
	}

	userID := b.generateID("user")
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
		Name:            req.Name,
		Emails:          req.Emails,
		Addresses:       req.Addresses,
		PhoneNumbers:    req.PhoneNumbers,
	}

	b.users[userID] = user

	if req.UserName != "" {
		b.usersByName[storeID+"#"+req.UserName] = userID
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

// ListUsers lists all users for the given identity store.
func (b *InMemoryBackend) ListUsers(storeID string) []*User {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	result := make([]*User, 0, len(b.users))

	for _, u := range b.users {
		if u.IdentityStoreID == storeID {
			result = append(result, copyUser(u))
		}
	}

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

	// Pre-validate any username rename to avoid partial update on conflict.
	for _, op := range ops {
		if strings.ToLower(op.AttributePath) == "username" {
			newName, _ := op.AttributeValue.(string)
			if newName != "" && newName != oldUserName {
				if _, exists := b.usersByName[storeID+"#"+newName]; exists {
					return fmt.Errorf("%w: user with UserName %q already exists", ErrConflict, newName)
				}
			}
		}
	}

	for _, op := range ops {
		applyUserAttribute(user, op.AttributePath, op.AttributeValue)
	}

	// Maintain usersByName index if UserName changed.
	if oldUserName != user.UserName {
		if oldUserName != "" {
			delete(b.usersByName, storeID+"#"+oldUserName)
		}

		if user.UserName != "" {
			b.usersByName[storeID+"#"+user.UserName] = userID
		}
	}

	return nil
}

// attrDisplayName is the normalized attribute path for a group's display name.
const attrDisplayName = "displayname"

// applyUserAttribute applies a single attribute operation to a user.
func applyUserAttribute(user *User, path string, value any) {
	strVal, _ := value.(string)

	switch strings.ToLower(path) {
	case attrDisplayName:
		user.DisplayName = strVal
	case "username":
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
	case "emails":
		user.Emails = parseEmails(value)
	case "addresses":
		user.Addresses = parseAddresses(value)
	case "phonenumbers":
		user.PhoneNumbers = parsePhoneNumbers(value)
	default:
		applyUserNameAttribute(user, path, strVal)
	}
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

func valueAsString(value any) string {
	str, _ := value.(string)

	return str
}

func valueAsBool(value any) bool {
	boolean, _ := value.(bool)

	return boolean
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

	delete(b.users, userID)

	// Remove associated memberships using the memberships map.
	// We must scan memberships because membershipKeys is keyed by group+user,
	// not by membershipID. Collect IDs to remove first to avoid map mutation during iteration.
	var toDelete []string

	for id, m := range b.memberships {
		if m.IdentityStoreID == storeID && m.MemberID.UserID == userID {
			toDelete = append(toDelete, id)
		}
	}

	for _, id := range toDelete {
		m := b.memberships[id]
		delete(b.membershipKeys, storeID+"#"+m.GroupID+"#"+userID)
		delete(b.memberships, id)
	}

	return nil
}

// GetUserID looks up a user ID by alternate identifier (UserName or email).
func (b *InMemoryBackend) GetUserID(storeID, attrPath, attrValue string) (string, error) {
	b.mu.RLock("GetUserID")
	defer b.mu.RUnlock()

	if strings.EqualFold(attrPath, "username") {
		if uid, ok := b.usersByName[storeID+"#"+attrValue]; ok {
			return uid, nil
		}

		return "", fmt.Errorf("%w: no user found with %s=%q", ErrUserNotFound, attrPath, attrValue)
	}

	for _, u := range b.users {
		if u.IdentityStoreID != storeID {
			continue
		}

		if strings.EqualFold(attrPath, "emails.value") {
			for _, e := range u.Emails {
				if e.Value == attrValue {
					return u.UserID, nil
				}
			}
		}
	}

	return "", fmt.Errorf("%w: no user found with %s=%q", ErrUserNotFound, attrPath, attrValue)
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

	groupID := b.generateID("group")
	group := &Group{
		GroupID:         groupID,
		IdentityStoreID: storeID,
		DisplayName:     req.DisplayName,
		Description:     req.Description,
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

// ListGroups lists all groups for the given identity store.
func (b *InMemoryBackend) ListGroups(storeID string) []*Group {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	result := make([]*Group, 0, len(b.groups))

	for _, g := range b.groups {
		if g.IdentityStoreID == storeID {
			result = append(result, copyGroup(g))
		}
	}

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

	membershipID := b.generateID("membership")
	membership := &GroupMembership{
		MembershipID:    membershipID,
		IdentityStoreID: storeID,
		GroupID:         groupID,
		MemberID:        memberID,
	}

	b.memberships[membershipID] = membership
	b.membershipKeys[key] = membershipID

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

// ListGroupMemberships lists all memberships for a group.
func (b *InMemoryBackend) ListGroupMemberships(storeID, groupID string) []*GroupMembership {
	b.mu.RLock("ListGroupMemberships")
	defer b.mu.RUnlock()

	result := make([]*GroupMembership, 0, len(b.memberships))

	for _, m := range b.memberships {
		if m.IdentityStoreID == storeID && m.GroupID == groupID {
			result = append(result, copyMembership(m))
		}
	}

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

// ListGroupMembershipsForMember lists all group memberships for a given member.
func (b *InMemoryBackend) ListGroupMembershipsForMember(storeID string, memberID MemberID) []*GroupMembership {
	b.mu.RLock("ListGroupMembershipsForMember")
	defer b.mu.RUnlock()

	result := make([]*GroupMembership, 0, len(b.memberships))

	for _, m := range b.memberships {
		if m.IdentityStoreID == storeID && m.MemberID.UserID == memberID.UserID {
			result = append(result, copyMembership(m))
		}
	}

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

	return &cp
}

func copyGroup(g *Group) *Group {
	if g == nil {
		return nil
	}
	cp := *g

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
	for id, u := range b.users {
		if u.UserName != "" {
			b.usersByName[u.IdentityStoreID+"#"+u.UserName] = id
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
	b.counter = 0
}
