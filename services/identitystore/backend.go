package identitystore

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
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

// groupMembershipExistence is the result item for IsMemberInGroups.
type groupMembershipExistence struct {
	GroupID          string `json:"GroupId"`
	MembershipExists bool   `json:"MembershipExists"`
}

// ----------------------------------------
// InMemoryBackend
// ----------------------------------------

// InMemoryBackend is the in-memory store for the Identity Store service.
type InMemoryBackend struct {
	mu          sync.RWMutex
	accountID   string
	region      string
	users       map[string]*User            // userID -> User
	groups      map[string]*Group           // groupID -> Group
	memberships map[string]*GroupMembership // membershipID -> GroupMembership
	counter     int
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
		accountID:   accountID,
		region:      region,
		users:       make(map[string]*User),
		groups:      make(map[string]*Group),
		memberships: make(map[string]*GroupMembership),
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
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check uniqueness by UserName.
	if req.UserName != "" {
		for _, u := range b.users {
			if u.IdentityStoreID == storeID && u.UserName == req.UserName {
				return nil, fmt.Errorf("%w: user with UserName %q already exists", ErrConflict, req.UserName)
			}
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

	return user, nil
}

// DescribeUser returns a user by ID.
func (b *InMemoryBackend) DescribeUser(storeID, userID string) (*User, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	user, ok := b.users[userID]
	if !ok || user.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userID)
	}

	return user, nil
}

// ListUsers lists all users for the given identity store.
func (b *InMemoryBackend) ListUsers(storeID string) []*User {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*User, 0)

	for _, u := range b.users {
		if u.IdentityStoreID == storeID {
			result = append(result, u)
		}
	}

	return result
}

// UpdateUser applies attribute operations to a user.
func (b *InMemoryBackend) UpdateUser(storeID, userID string, ops []attributeOperation) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	user, ok := b.users[userID]
	if !ok || user.IdentityStoreID != storeID {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userID)
	}

	for _, op := range ops {
		applyUserAttribute(user, op.AttributePath, op.AttributeValue)
	}

	return nil
}

// applyUserAttribute applies a single attribute operation to a user.
func applyUserAttribute(user *User, path string, value any) {
	strVal, _ := value.(string)

	switch strings.ToLower(path) {
	case "displayname":
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
	case "name.givenname":
		if user.Name == nil {
			user.Name = &Name{}
		}

		user.Name.GivenName = strVal
	case "name.familyname":
		if user.Name == nil {
			user.Name = &Name{}
		}

		user.Name.FamilyName = strVal
	case "name.middlename":
		if user.Name == nil {
			user.Name = &Name{}
		}

		user.Name.MiddleName = strVal
	case "name.formatted":
		if user.Name == nil {
			user.Name = &Name{}
		}

		user.Name.Formatted = strVal
	case "name.honorificprefix":
		if user.Name == nil {
			user.Name = &Name{}
		}

		user.Name.HonorificPrefix = strVal
	case "name.honorificsuffix":
		if user.Name == nil {
			user.Name = &Name{}
		}

		user.Name.HonorificSuffix = strVal
	}
}

// DeleteUser removes a user from the identity store.
func (b *InMemoryBackend) DeleteUser(storeID, userID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	user, ok := b.users[userID]
	if !ok || user.IdentityStoreID != storeID {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userID)
	}

	delete(b.users, userID)

	// Remove associated memberships.
	for id, m := range b.memberships {
		if m.IdentityStoreID == storeID && m.MemberID.UserID == userID {
			delete(b.memberships, id)
		}
	}

	return nil
}

// GetUserID looks up a user ID by alternate identifier (UserName or email).
func (b *InMemoryBackend) GetUserID(storeID, attrPath, attrValue string) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, u := range b.users {
		if u.IdentityStoreID != storeID {
			continue
		}

		switch strings.ToLower(attrPath) {
		case "username":
			if u.UserName == attrValue {
				return u.UserID, nil
			}
		case "emails.value":
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
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check uniqueness by DisplayName.
	if req.DisplayName != "" {
		for _, g := range b.groups {
			if g.IdentityStoreID == storeID && g.DisplayName == req.DisplayName {
				return nil, fmt.Errorf("%w: group with DisplayName %q already exists", ErrConflict, req.DisplayName)
			}
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

	return group, nil
}

// DescribeGroup returns a group by ID.
func (b *InMemoryBackend) DescribeGroup(storeID, groupID string) (*Group, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group, ok := b.groups[groupID]
	if !ok || group.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	return group, nil
}

// ListGroups lists all groups for the given identity store.
func (b *InMemoryBackend) ListGroups(storeID string) []*Group {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*Group, 0)

	for _, g := range b.groups {
		if g.IdentityStoreID == storeID {
			result = append(result, g)
		}
	}

	return result
}

// UpdateGroup applies attribute operations to a group.
func (b *InMemoryBackend) UpdateGroup(storeID, groupID string, ops []attributeOperation) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	group, ok := b.groups[groupID]
	if !ok || group.IdentityStoreID != storeID {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	for _, op := range ops {
		switch strings.ToLower(op.AttributePath) {
		case "displayname":
			if s, ok := op.AttributeValue.(string); ok {
				group.DisplayName = s
			}
		case "description":
			if s, ok := op.AttributeValue.(string); ok {
				group.Description = s
			}
		}
	}

	return nil
}

// DeleteGroup removes a group from the identity store.
func (b *InMemoryBackend) DeleteGroup(storeID, groupID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	group, ok := b.groups[groupID]
	if !ok || group.IdentityStoreID != storeID {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	delete(b.groups, groupID)

	// Remove associated memberships.
	for id, m := range b.memberships {
		if m.IdentityStoreID == storeID && m.GroupID == groupID {
			delete(b.memberships, id)
		}
	}

	return nil
}

// GetGroupID looks up a group ID by alternate identifier (DisplayName).
func (b *InMemoryBackend) GetGroupID(storeID, attrPath, attrValue string) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, g := range b.groups {
		if g.IdentityStoreID != storeID {
			continue
		}

		if strings.EqualFold(attrPath, "displayName") && g.DisplayName == attrValue {
			return g.GroupID, nil
		}
	}

	return "", fmt.Errorf("%w: no group found with %s=%q", ErrGroupNotFound, attrPath, attrValue)
}

// ----------------------------------------
// Membership operations
// ----------------------------------------

// CreateGroupMembership creates a membership between a user and a group.
func (b *InMemoryBackend) CreateGroupMembership(storeID, groupID string, memberID MemberID) (*GroupMembership, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Validate group exists.
	group, ok := b.groups[groupID]
	if !ok || group.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	// Validate user exists.
	if memberID.UserID != "" {
		user, ok := b.users[memberID.UserID]
		if !ok || user.IdentityStoreID != storeID {
			return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, memberID.UserID)
		}
	}

	// Check for duplicate membership.
	for _, m := range b.memberships {
		if m.IdentityStoreID == storeID && m.GroupID == groupID && m.MemberID.UserID == memberID.UserID {
			return nil, fmt.Errorf("%w: membership already exists", ErrConflict)
		}
	}

	membershipID := b.generateID("membership")
	membership := &GroupMembership{
		MembershipID:    membershipID,
		IdentityStoreID: storeID,
		GroupID:         groupID,
		MemberID:        memberID,
	}

	b.memberships[membershipID] = membership

	return membership, nil
}

// DescribeGroupMembership returns a membership by ID.
func (b *InMemoryBackend) DescribeGroupMembership(storeID, membershipID string) (*GroupMembership, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	m, ok := b.memberships[membershipID]
	if !ok || m.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: membership %q not found", ErrMembershipNotFound, membershipID)
	}

	return m, nil
}

// ListGroupMemberships lists all memberships for a group.
func (b *InMemoryBackend) ListGroupMemberships(storeID, groupID string) []*GroupMembership {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*GroupMembership, 0)

	for _, m := range b.memberships {
		if m.IdentityStoreID == storeID && m.GroupID == groupID {
			result = append(result, m)
		}
	}

	return result
}

// DeleteGroupMembership removes a membership.
func (b *InMemoryBackend) DeleteGroupMembership(storeID, membershipID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	m, ok := b.memberships[membershipID]
	if !ok || m.IdentityStoreID != storeID {
		return fmt.Errorf("%w: membership %q not found", ErrMembershipNotFound, membershipID)
	}

	delete(b.memberships, membershipID)

	return nil
}

// GetGroupMembershipID looks up a membership ID by group and member.
func (b *InMemoryBackend) GetGroupMembershipID(storeID, groupID string, memberID MemberID) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, m := range b.memberships {
		if m.IdentityStoreID == storeID && m.GroupID == groupID && m.MemberID.UserID == memberID.UserID {
			return m.MembershipID, nil
		}
	}

	return "", fmt.Errorf("%w: membership not found for group=%q member=%q", ErrMembershipNotFound, groupID, memberID.UserID)
}

// ListGroupMembershipsForMember lists all group memberships for a given member.
func (b *InMemoryBackend) ListGroupMembershipsForMember(storeID string, memberID MemberID) []*GroupMembership {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*GroupMembership, 0)

	for _, m := range b.memberships {
		if m.IdentityStoreID == storeID && m.MemberID.UserID == memberID.UserID {
			result = append(result, m)
		}
	}

	return result
}

// IsMemberInGroups checks which of the given groups contain the specified member.
func (b *InMemoryBackend) IsMemberInGroups(storeID string, memberID MemberID, groupIDs []string) []groupMembershipExistence {
	b.mu.RLock()
	defer b.mu.RUnlock()

	groupSet := make(map[string]bool, len(groupIDs))
	for _, id := range groupIDs {
		groupSet[id] = false
	}

	for _, m := range b.memberships {
		if m.IdentityStoreID != storeID || m.MemberID.UserID != memberID.UserID {
			continue
		}

		if _, ok := groupSet[m.GroupID]; ok {
			groupSet[m.GroupID] = true
		}
	}

	result := make([]groupMembershipExistence, 0, len(groupIDs))
	for _, id := range groupIDs {
		result = append(result, groupMembershipExistence{
			GroupID:          id,
			MembershipExists: groupSet[id],
		})
	}

	return result
}
