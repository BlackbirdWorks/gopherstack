package mq

import (
	"fmt"
	"sort"
	"strings"
)

const (
	// maxActiveMQUsers is the maximum number of users per ActiveMQ broker.
	// AWS MQ enforces a hard limit of 250 users per ActiveMQ broker.
	maxActiveMQUsers = 250

	// maxUserGroups is the maximum number of groups a single ActiveMQ user may belong to.
	// AWS MQ enforces a limit of 20 groups per user.
	maxUserGroups = 20

	// minUsernameLen is the minimum length of a broker user username.
	minUsernameLen = 2

	// maxUsernameLen is the maximum length of a broker user username.
	maxUsernameLen = 100

	// minPasswordUniqueChars is the minimum number of distinct characters required
	// in an ActiveMQ broker user password.
	minPasswordUniqueChars = 4
)

// validateActiveMQPassword enforces AWS MQ ActiveMQ password constraints:
// 12-250 characters, no commas/colons/equal signs, at least 4 unique
// characters. See CreateUserRequest.Password in the MQ API reference:
// "must not contain commas, colons, or equal signs (,:=)".
func validateActiveMQPassword(password string) error {
	if len(password) < 12 || len(password) > 250 {
		return fmt.Errorf(
			"%w: ActiveMQ password must be 12-250 characters (got %d)",
			ErrValidation, len(password),
		)
	}

	if strings.ContainsAny(password, ",:=") {
		return fmt.Errorf("%w: ActiveMQ password must not contain commas, colons, or equal signs", ErrValidation)
	}

	unique := make(map[rune]struct{})
	for _, c := range password {
		unique[c] = struct{}{}
	}

	if len(unique) < minPasswordUniqueChars {
		return fmt.Errorf(
			"%w: ActiveMQ password must contain at least %d unique characters (got %d)",
			ErrValidation, minPasswordUniqueChars, len(unique),
		)
	}

	return nil
}

// validateUsername enforces AWS MQ broker username constraints. Per the User
// shape's ActiveMQ documentation, a username may contain only alphanumeric
// characters, dashes, periods, underscores, and tildes (- . _ ~) and must be
// 2-100 characters long. (RabbitMQ additionally forbids the tilde and the
// literal name "guest", but this validator is used before the broker's
// engine type is known, so it accepts the more permissive ActiveMQ charset.)
func validateUsername(username string) error {
	if len(username) < minUsernameLen || len(username) > maxUsernameLen {
		return fmt.Errorf(
			"%w: username must be %d-%d characters (got %d)",
			ErrValidation, minUsernameLen, maxUsernameLen, len(username),
		)
	}

	if !isAlphanumeric(rune(username[0])) {
		return fmt.Errorf("%w: username must start with an alphanumeric character", ErrValidation)
	}

	for _, c := range username {
		if !isAlphanumeric(c) && c != '-' && c != '_' && c != '.' && c != '~' {
			return fmt.Errorf(
				"%w: username must contain only alphanumeric characters, dashes, periods, "+
					"underscores, and tildes, got %q",
				ErrValidation, c,
			)
		}
	}

	return nil
}

// validateActiveMQUserConstraints checks ActiveMQ-specific user creation limits.
func validateActiveMQUserConstraints(currentUsers, groupCount int, password string) error {
	if currentUsers >= maxActiveMQUsers {
		return fmt.Errorf(
			"%w: ActiveMQ broker cannot have more than %d users",
			ErrValidation, maxActiveMQUsers,
		)
	}

	if groupCount > maxUserGroups {
		return fmt.Errorf(
			"%w: user groups must not exceed %d (got %d)",
			ErrValidation, maxUserGroups, groupCount,
		)
	}

	if password != "" {
		return validateActiveMQPassword(password)
	}

	return nil
}

// CreateUser creates a user on a broker. Real Amazon MQ only activates a new
// ActiveMQ broker user on the next reboot (see
// aws-sdk-go-v2/service/mq/types.UserPendingChanges); the user is inserted
// immediately with the requested attributes but marked Pending.PendingChange
// = CREATE, exactly as DescribeUser/ListUsers would show it, until
// promoteBrokerReboot clears the marker.
func (b *InMemoryBackend) CreateUser(
	brokerID, username, password string, groups []string, console, replicationUser bool,
) error {
	if err := validateUsername(username); err != nil {
		return err
	}

	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	if _, ok := br.Users[username]; ok {
		return fmt.Errorf("%w: user %s already exists on broker %s", ErrAlreadyExists, username, brokerID)
	}

	if br.EngineType == EngineTypeActiveMQ {
		if err := validateActiveMQUserConstraints(len(br.Users), len(groups), password); err != nil {
			return err
		}
	}

	br.Users[username] = &User{
		Username:        username,
		Password:        password,
		Groups:          groups,
		Console:         console,
		ReplicationUser: replicationUser,
		Pending:         &UserPendingChanges{PendingChange: ChangeTypeCreate},
	}

	return nil
}

// DescribeUser returns a user from a broker.
func (b *InMemoryBackend) DescribeUser(brokerID, username string) (*User, error) {
	b.mu.RLock("DescribeUser")
	defer b.mu.RUnlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	u, ok := br.Users[username]
	if !ok {
		return nil, fmt.Errorf("%w: user %s not found on broker %s", ErrNotFound, username, brokerID)
	}

	cp := *u

	return &cp, nil
}

// UpdateUser updates a broker user. Console/Groups changes are staged into
// the user's Pending block (see CreateUser) since real Amazon MQ only
// applies them on the next reboot -- DescribeUser's top-level
// consoleAccess/groups keep showing the pre-update values until then. The
// password is applied immediately: it is never echoed back on any wire
// response (DescribeUser/ListUsers never include it), so there is no
// observable "staged vs. live" distinction to model, and
// aws-sdk-go-v2/service/mq/types.UserPendingChanges has no password field to
// stage it into. replicationUser applies immediately for the same reason --
// UserPendingChanges has no replicationUser field either.
func (b *InMemoryBackend) UpdateUser(
	brokerID, username, password string, groups []string, console, replicationUser *bool,
) error {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	u, ok := br.Users[username]
	if !ok {
		return fmt.Errorf("%w: user %s not found on broker %s", ErrNotFound, username, brokerID)
	}

	if password != "" {
		if br.EngineType == EngineTypeActiveMQ {
			if err := validateActiveMQPassword(password); err != nil {
				return err
			}
		}

		u.Password = password
	}

	if groups != nil && br.EngineType == EngineTypeActiveMQ && len(groups) > maxUserGroups {
		return fmt.Errorf(
			"%w: user groups must not exceed %d (got %d)",
			ErrValidation, maxUserGroups, len(groups),
		)
	}

	if replicationUser != nil {
		u.ReplicationUser = *replicationUser
	}

	stageUserPendingChange(u, groups, console)

	return nil
}

// stageUserPendingChange records a not-yet-applied Console/Groups change on
// u.Pending. A user with an already-pending CREATE keeps that PendingChange
// (its Console/Groups are updated in place, since the user has never gone
// live yet); otherwise the change is marked UPDATE. Caller must hold a write
// lock.
func stageUserPendingChange(u *User, groups []string, console *bool) {
	if groups == nil && console == nil {
		return
	}

	if u.Pending == nil {
		u.Pending = &UserPendingChanges{PendingChange: ChangeTypeUpdate}
	}

	if groups != nil {
		u.Pending.Groups = groups
	}

	if console != nil {
		u.Pending.Console = console
	}
}

// DeleteUser stages a broker user for removal. Real Amazon MQ only removes
// an ActiveMQ broker user on the next reboot: the user stays visible via
// DescribeUser/ListUsers with pendingChange=DELETE until then (see
// promoteBrokerReboot), matching how the create/update paths stage into
// User.Pending.
func (b *InMemoryBackend) DeleteUser(brokerID, username string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	u, ok := br.Users[username]
	if !ok {
		return fmt.Errorf("%w: user %s not found on broker %s", ErrNotFound, username, brokerID)
	}

	u.Pending = &UserPendingChanges{PendingChange: ChangeTypeDelete}

	return nil
}

// ListUsers returns all users for a broker.
func (b *InMemoryBackend) ListUsers(brokerID string) ([]UserSummary, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	list := make([]UserSummary, 0, len(br.Users))
	for _, u := range br.Users {
		summary := UserSummary{Username: u.Username}
		if u.Pending != nil {
			summary.PendingChange = u.Pending.PendingChange
		}

		list = append(list, summary)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Username < list[j].Username })

	return list, nil
}

// promoteBrokerUsers applies every broker user's staged Pending change
// (CREATE/UPDATE/DELETE) atomically, as part of promoteBrokerReboot. Caller
// must hold a write lock.
func promoteBrokerUsers(br *Broker) {
	for username, u := range br.Users {
		if u.Pending == nil {
			continue
		}

		switch u.Pending.PendingChange {
		case ChangeTypeDelete:
			delete(br.Users, username)
		default: // ChangeTypeCreate, ChangeTypeUpdate
			if u.Pending.Console != nil {
				u.Console = *u.Pending.Console
			}

			if u.Pending.Groups != nil {
				u.Groups = u.Pending.Groups
			}

			u.Pending = nil
		}
	}
}
