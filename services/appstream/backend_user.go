package appstream

import (
	"fmt"
	"time"
)

const (
	userStatusCreated  = "CREATED"
	sessionStateActive = "ACTIVE"
	sessionConnected   = "CONNECTED"
)

type storedUser struct {
	CreatedTime        time.Time `json:"createdTime"`
	UserName           string    `json:"userName"`
	Arn                string    `json:"arn"`
	Email              string    `json:"email"`
	FirstName          string    `json:"firstName"`
	LastName           string    `json:"lastName"`
	AuthenticationType string    `json:"authenticationType"`
	Status             string    `json:"status"`
	Enabled            bool      `json:"enabled"`
}

func (u *storedUser) toUser() *User {
	return &User{
		CreatedTime:        u.CreatedTime,
		UserName:           u.UserName,
		Arn:                u.Arn,
		Email:              u.Email,
		FirstName:          u.FirstName,
		LastName:           u.LastName,
		AuthenticationType: u.AuthenticationType,
		Status:             u.Status,
		Enabled:            u.Enabled,
	}
}

type storedSession struct {
	StartTime          time.Time `json:"startTime"`
	ID                 string    `json:"id"`
	FleetName          string    `json:"fleetName"`
	StackName          string    `json:"stackName"`
	UserID             string    `json:"userId"`
	State              string    `json:"state"`
	ConnectionState    string    `json:"connectionState"`
	AuthenticationType string    `json:"authenticationType"`
}

func (s *storedSession) toSession() *Session {
	return &Session{
		StartTime:          s.StartTime,
		ID:                 s.ID,
		FleetName:          s.FleetName,
		StackName:          s.StackName,
		UserID:             s.UserID,
		State:              s.State,
		ConnectionState:    s.ConnectionState,
		AuthenticationType: s.AuthenticationType,
	}
}

type storedUsageReportSubscription struct {
	S3BucketName string `json:"s3BucketName"`
	Schedule     string `json:"schedule"`
}

func (u *storedUsageReportSubscription) toUsageReportSubscription() *UsageReportSubscription {
	return &UsageReportSubscription{
		S3BucketName: u.S3BucketName,
		Schedule:     u.Schedule,
	}
}

type storedTheme struct {
	CreatedTime time.Time `json:"createdTime"`
	StackName   string    `json:"stackName"`
	State       string    `json:"state"`
}

func (t *storedTheme) toTheme() *Theme {
	return &Theme{
		CreatedTime: t.CreatedTime,
		StackName:   t.StackName,
		State:       t.State,
	}
}

func userKey(userName, authType string) string { return userName + "\x00" + authType }

func (b *InMemoryBackend) userARN(userName, authType string) string {
	return fmt.Sprintf(
		"arn:aws:appstream:%s:%s:user/%s/%s", b.region, b.accountID, authType, userName,
	)
}

func (b *InMemoryBackend) nextSessionID() string {
	b.sessionSeq++
	return fmt.Sprintf("session-%010d", b.sessionSeq)
}

// CreateUser creates a new UserPool user.
func (b *InMemoryBackend) CreateUser(userName, email, firstName, lastName, authType string) (*User, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	key := userKey(userName, authType)
	if _, ok := b.users[key]; ok {
		return nil, ErrAlreadyExists
	}

	u := &storedUser{
		CreatedTime:        time.Now().UTC(),
		UserName:           userName,
		Arn:                b.userARN(userName, authType),
		Email:              email,
		FirstName:          firstName,
		LastName:           lastName,
		AuthenticationType: authType,
		Status:             userStatusCreated,
		Enabled:            true,
	}
	b.users[key] = u

	return u.toUser(), nil
}

// DeleteUser removes a user.
func (b *InMemoryBackend) DeleteUser(userName, authType string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	key := userKey(userName, authType)
	if _, ok := b.users[key]; !ok {
		return ErrNotFound
	}

	delete(b.users, key)
	delete(b.userStackAssoc, key)

	return nil
}

// DescribeUsers returns users, optionally filtered by authentication type.
func (b *InMemoryBackend) DescribeUsers(authType string) ([]*User, error) {
	b.mu.RLock("DescribeUsers")
	defer b.mu.RUnlock()

	var result []*User

	for _, u := range b.users {
		if authType != "" && u.AuthenticationType != authType {
			continue
		}

		result = append(result, u.toUser())
	}

	return result, nil
}

// DisableUser disables a user.
func (b *InMemoryBackend) DisableUser(userName, authType string) error {
	b.mu.Lock("DisableUser")
	defer b.mu.Unlock()

	key := userKey(userName, authType)
	u, ok := b.users[key]
	if !ok {
		return ErrNotFound
	}

	u.Enabled = false

	return nil
}

// EnableUser re-enables a user.
func (b *InMemoryBackend) EnableUser(userName, authType string) error {
	b.mu.Lock("EnableUser")
	defer b.mu.Unlock()

	key := userKey(userName, authType)
	u, ok := b.users[key]
	if !ok {
		return ErrNotFound
	}

	u.Enabled = true

	return nil
}

// BatchAssociateUserStack links users to stacks.
func (b *InMemoryBackend) BatchAssociateUserStack(
	associations []UserStackAssociation,
) ([]UserStackAssociationError, error) {
	b.mu.Lock("BatchAssociateUserStack")
	defer b.mu.Unlock()

	var errs []UserStackAssociationError

	for _, assoc := range associations {
		key := userKey(assoc.UserName, assoc.AuthenticationType)
		if _, ok := b.users[key]; !ok {
			a := assoc
			errs = append(errs, UserStackAssociationError{
				UserStackAssociation: &a,
				ErrorCode:            "USER_NOT_FOUND",
				ErrorMessage:         "User not found",
			})

			continue
		}

		if b.userStackAssoc[key] == nil {
			b.userStackAssoc[key] = make(map[string]bool)
		}

		b.userStackAssoc[key][assoc.StackName] = true
	}

	return errs, nil
}

// BatchDisassociateUserStack unlinks users from stacks.
func (b *InMemoryBackend) BatchDisassociateUserStack(
	associations []UserStackAssociation,
) ([]UserStackAssociationError, error) {
	b.mu.Lock("BatchDisassociateUserStack")
	defer b.mu.Unlock()

	var errs []UserStackAssociationError

	for _, assoc := range associations {
		key := userKey(assoc.UserName, assoc.AuthenticationType)
		if _, ok := b.users[key]; !ok {
			a := assoc
			errs = append(errs, UserStackAssociationError{
				UserStackAssociation: &a,
				ErrorCode:            "USER_NOT_FOUND",
				ErrorMessage:         "User not found",
			})

			continue
		}

		if b.userStackAssoc[key] != nil {
			delete(b.userStackAssoc[key], assoc.StackName)
		}
	}

	return errs, nil
}

// DescribeUserStackAssociations returns user-stack links, optionally filtered.
func (b *InMemoryBackend) DescribeUserStackAssociations(
	stackName, userName, authType string,
) ([]*UserStackAssociation, error) {
	b.mu.RLock("DescribeUserStackAssociations")
	defer b.mu.RUnlock()

	var result []*UserStackAssociation

	for uKey, stacks := range b.userStackAssoc {
		u, ok := b.users[uKey]
		if !ok {
			continue
		}

		if userName != "" && u.UserName != userName {
			continue
		}

		if authType != "" && u.AuthenticationType != authType {
			continue
		}

		for sName := range stacks {
			if stackName != "" && sName != stackName {
				continue
			}

			result = append(result, &UserStackAssociation{
				UserName:           u.UserName,
				StackName:          sName,
				AuthenticationType: u.AuthenticationType,
			})
		}
	}

	return result, nil
}

// DescribeSessions returns sessions filtered by stack, fleet, and/or user.
func (b *InMemoryBackend) DescribeSessions(stackName, fleetName, userID string) ([]*Session, error) {
	b.mu.RLock("DescribeSessions")
	defer b.mu.RUnlock()

	var result []*Session

	for _, s := range b.sessions {
		if stackName != "" && s.StackName != stackName {
			continue
		}

		if fleetName != "" && s.FleetName != fleetName {
			continue
		}

		if userID != "" && s.UserID != userID {
			continue
		}

		result = append(result, s.toSession())
	}

	return result, nil
}

// DrainSessionInstance removes a session.
func (b *InMemoryBackend) DrainSessionInstance(sessionID string) error {
	b.mu.Lock("DrainSessionInstance")
	defer b.mu.Unlock()

	if _, ok := b.sessions[sessionID]; !ok {
		return ErrNotFound
	}

	delete(b.sessions, sessionID)

	return nil
}

// ExpireSession marks a session as expired (and removes it).
func (b *InMemoryBackend) ExpireSession(sessionID string) error {
	b.mu.Lock("ExpireSession")
	defer b.mu.Unlock()

	if _, ok := b.sessions[sessionID]; !ok {
		return ErrNotFound
	}

	delete(b.sessions, sessionID)

	return nil
}

// CreateStreamingURL creates a session and returns a streaming URL.
func (b *InMemoryBackend) CreateStreamingURL(stackName, fleetName, userID string) (string, error) {
	b.mu.Lock("CreateStreamingURL")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackName]; !ok {
		return "", ErrNotFound
	}

	if _, ok := b.fleets[fleetName]; !ok {
		return "", ErrNotFound
	}

	sessionID := b.nextSessionID()
	s := &storedSession{
		StartTime:          time.Now().UTC(),
		ID:                 sessionID,
		FleetName:          fleetName,
		StackName:          stackName,
		UserID:             userID,
		State:              sessionStateActive,
		ConnectionState:    sessionConnected,
		AuthenticationType: "API",
	}
	b.sessions[sessionID] = s

	url := fmt.Sprintf(
		"https://appstream2.%s.aws.amazon.com/authenticate?param=%s", b.region, sessionID,
	)

	return url, nil
}

// CreateUsageReportSubscription creates a usage report subscription.
func (b *InMemoryBackend) CreateUsageReportSubscription(schedule, s3Bucket string) (*UsageReportSubscription, error) {
	b.mu.Lock("CreateUsageReportSubscription")
	defer b.mu.Unlock()

	if b.usageReport != nil {
		return nil, ErrAlreadyExists
	}

	sched := schedule
	if sched == "" {
		sched = "DAILY"
	}

	b.usageReport = &storedUsageReportSubscription{
		S3BucketName: s3Bucket,
		Schedule:     sched,
	}

	return b.usageReport.toUsageReportSubscription(), nil
}

// DeleteUsageReportSubscription removes the usage report subscription.
func (b *InMemoryBackend) DeleteUsageReportSubscription() error {
	b.mu.Lock("DeleteUsageReportSubscription")
	defer b.mu.Unlock()

	if b.usageReport == nil {
		return ErrNotFound
	}

	b.usageReport = nil

	return nil
}

// DescribeUsageReportSubscriptions returns the usage report subscription.
func (b *InMemoryBackend) DescribeUsageReportSubscriptions() ([]*UsageReportSubscription, error) {
	b.mu.RLock("DescribeUsageReportSubscriptions")
	defer b.mu.RUnlock()

	if b.usageReport == nil {
		return []*UsageReportSubscription{}, nil
	}

	return []*UsageReportSubscription{b.usageReport.toUsageReportSubscription()}, nil
}

// CreateThemeForStack creates a theme for a stack.
func (b *InMemoryBackend) CreateThemeForStack(stackName string) (*Theme, error) {
	b.mu.Lock("CreateThemeForStack")
	defer b.mu.Unlock()

	if _, ok := b.stacks[stackName]; !ok {
		return nil, ErrNotFound
	}

	if _, ok := b.themes[stackName]; ok {
		return nil, ErrAlreadyExists
	}

	th := &storedTheme{
		CreatedTime: time.Now().UTC(),
		StackName:   stackName,
		State:       "ENABLED",
	}
	b.themes[stackName] = th

	return th.toTheme(), nil
}

// DeleteThemeForStack removes a stack theme.
func (b *InMemoryBackend) DeleteThemeForStack(stackName string) error {
	b.mu.Lock("DeleteThemeForStack")
	defer b.mu.Unlock()

	if _, ok := b.themes[stackName]; !ok {
		return ErrNotFound
	}

	delete(b.themes, stackName)

	return nil
}

// DescribeThemeForStack returns the theme for a stack.
func (b *InMemoryBackend) DescribeThemeForStack(stackName string) (*Theme, error) {
	b.mu.RLock("DescribeThemeForStack")
	defer b.mu.RUnlock()

	th, ok := b.themes[stackName]
	if !ok {
		return nil, ErrNotFound
	}

	return th.toTheme(), nil
}

// UpdateThemeForStack updates the theme for a stack.
func (b *InMemoryBackend) UpdateThemeForStack(stackName string) (*Theme, error) {
	b.mu.Lock("UpdateThemeForStack")
	defer b.mu.Unlock()

	th, ok := b.themes[stackName]
	if !ok {
		return nil, ErrNotFound
	}

	return th.toTheme(), nil
}
