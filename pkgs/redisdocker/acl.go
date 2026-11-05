package redisdocker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// AdminUser is the internal account the emulator uses for control traffic; it is never exposed to API users.
const AdminUser = "gs-admin"

const defaultUser = "default"

// ErrACL is returned for an unusable ACL definition.
var ErrACL = errors.New("invalid ACL user")

// ACLUser is one Redis ACL account derived from an API user.
type ACLUser struct {
	Name         string
	AccessString string
	Passwords    []string
	// NoPassword lets the user in without a password.
	NoPassword bool
}

// ACLPolicy is the full set of accounts a group serves.
type ACLPolicy struct {
	// DefaultPassword protects the "default" account when no listed user is named "default".
	DefaultPassword string
	Users           []ACLUser
	// OpenDefault leaves the "default" account passwordless and unrestricted when no listed user is named "default".
	OpenDefault bool
}

// SetUserArgs translates u into an ACL SETUSER command. The user is reset first so repeated syncs converge, then
// gets its passwords and the rules of its access string ("on ~app:* +@all").
func SetUserArgs(u ACLUser) ([]any, error) {
	if u.Name == "" || strings.ContainsAny(u.Name, " \t\r\n") || u.Name == AdminUser {
		return nil, fmt.Errorf("%w: bad name %q", ErrACL, u.Name)
	}

	args := []any{"ACL", "SETUSER", u.Name, "reset"}

	switch {
	case u.NoPassword:
		args = append(args, "nopass")
	default:
		for _, p := range u.Passwords {
			args = append(args, ">"+p)
		}
	}

	for _, rule := range strings.Fields(u.AccessString) {
		args = append(args, rule)
	}

	return args, nil
}

func (p ACLPolicy) hasDefault() bool {
	return slices.ContainsFunc(p.Users, func(u ACLUser) bool { return u.Name == defaultUser })
}

// commands returns the SETUSER commands for p; accounts are listed before "default" so a failure leaves it intact.
func (p ACLPolicy) commands() ([][]any, error) {
	cmds := make([][]any, 0, len(p.Users)+1)

	for _, u := range p.Users {
		if u.Name == defaultUser {
			continue
		}

		args, err := SetUserArgs(u)
		if err != nil {
			return nil, err
		}

		cmds = append(cmds, args)
	}

	if def, ok := p.defaultCommand(); ok {
		cmds = append(cmds, def)
	}

	return cmds, nil
}

func (p ACLPolicy) defaultCommand() ([]any, bool) {
	if i := slices.IndexFunc(p.Users, func(u ACLUser) bool { return u.Name == defaultUser }); i >= 0 {
		args, err := SetUserArgs(p.Users[i])

		return args, err == nil
	}

	base := []any{"ACL", "SETUSER", defaultUser, "reset"}

	switch {
	case p.DefaultPassword != "":
		return append(base, "on", ">"+p.DefaultPassword, "~*", "&*", "+@all"), true
	case p.OpenDefault:
		return append(base, "on", "nopass", "~*", "&*", "+@all"), true
	default:
		return base, true
	}
}

// names lists the accounts p defines, excluding "default".
func (p ACLPolicy) names() []string {
	var out []string

	for _, u := range p.Users {
		if u.Name != defaultUser {
			out = append(out, u.Name)
		}
	}

	return out
}

// ApplyACL syncs policy onto every node; accounts dropped since the previous sync are deleted.
func (g *Group) ApplyACL(ctx context.Context, policy ACLPolicy) error {
	cmds, err := policy.commands()
	if err != nil {
		return err
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	wanted := policy.names()
	stale := slices.DeleteFunc(slices.Clone(g.aclUsers), func(n string) bool { return slices.Contains(wanted, n) })

	for _, n := range g.nodes {
		if err = g.syncNodeACL(ctx, n, cmds, stale); err != nil {
			return err
		}
	}

	g.aclUsers = wanted

	return nil
}

func (g *Group) syncNodeACL(ctx context.Context, n *node, cmds [][]any, stale []string) error {
	adm := g.dial(n)
	defer func() { _ = adm.Close() }()

	for _, name := range stale {
		if _, err := adm.Do(ctx, "ACL", "DELUSER", name); err != nil {
			return fmt.Errorf("delete acl user: %w", err)
		}
	}

	for _, cmd := range cmds {
		if _, err := adm.Do(ctx, cmd...); err != nil {
			return fmt.Errorf("set acl user %v: %w", cmd[2], redactErr(err, cmd))
		}
	}

	return nil
}

// redactErr drops any password-bearing text from err.
func redactErr(err error, cmd []any) error {
	msg := err.Error()

	for _, a := range cmd {
		if s, ok := a.(string); ok && strings.HasPrefix(s, ">") {
			msg = strings.ReplaceAll(msg, s[1:], "***")
		}
	}

	return fmt.Errorf("%w: %s", ErrACL, msg)
}
