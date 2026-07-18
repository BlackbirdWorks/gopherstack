package identitystore

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

// ListFilter is a single attribute-equality filter for list operations.
type ListFilter struct {
	AttributePath  string `json:"AttributePath"`
	AttributeValue string `json:"AttributeValue"`
}
