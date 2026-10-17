package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	identitystorebackend "github.com/blackbirdworks/gopherstack/services/identitystore"
	orgbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	ssobackend "github.com/blackbirdworks/gopherstack/services/sso"
	ssoadminbackend "github.com/blackbirdworks/gopherstack/services/ssoadmin"
	ssooidcbackend "github.com/blackbirdworks/gopherstack/services/ssooidc"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	ssoServicePrincipal  = "sso.amazonaws.com"
	ssoRolePath          = "/aws-reserved/sso.amazonaws.com/"
	ssoRolePrefix        = "AWSReservedSSO_"
	ssoInlinePolicyName  = "AwsSSOInlinePolicy"
	ssoRoleHashLen       = 16
	ssoSessionNameMin    = 2
	ssoSessionNameMax    = 64
	ssoPrincipalUser     = "USER"
	ssoPrincipalGroup    = "GROUP"
	ssoTrustPolicyFormat = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"%s"},"Action":["sts:AssumeRole","sts:TagSession"]}]}`
)

var (
	errSSODependencyUnavailable = errors.New("sso: required service is not available")
	ssoSessionNameInvalid       = regexp.MustCompile(`[^\w+=,.@-]`)
)

// ssoIdentity resolves Identity Center users, entitlements and role credentials from the sibling
// services at call time; nothing is captured when the providers initialise.
type ssoIdentity struct {
	byName map[string]service.Registerable
}

func (s ssoIdentity) ssoAdmin() *ssoadminbackend.Handler {
	h, _ := s.byName["SsoAdmin"].(*ssoadminbackend.Handler)

	return h
}

func (s ssoIdentity) identityStore() *identitystorebackend.InMemoryBackend {
	h, ok := s.byName["IdentityStore"].(*identitystorebackend.Handler)
	if !ok {
		return nil
	}

	return h.Backend
}

// users lists every Identity Store user across all Identity Center instances.
func (s ssoIdentity) users() ([]ssooidcbackend.Subject, error) {
	admin, ids := s.ssoAdmin(), s.identityStore()
	if admin == nil || ids == nil {
		return nil, errSSODependencyUnavailable
	}

	var out []ssooidcbackend.Subject

	for _, bk := range admin.RegionBackends() {
		for _, inst := range bk.ListInstances() {
			for _, u := range ids.ListUsers(context.Background(), inst.IdentityStoreID) {
				out = append(out, ssooidcbackend.Subject{
					UserName: u.UserName, UserID: u.UserID, IdentityStoreID: inst.IdentityStoreID,
				})
			}
		}
	}

	return out, nil
}

// ResolveSubject finds userName (case-insensitive) in any instance's identity store; an empty
// userName selects the user whose name sorts first.
func (s ssoIdentity) ResolveSubject(userName string) (ssooidcbackend.Subject, error) {
	users, err := s.users()
	if err != nil {
		return ssooidcbackend.Subject{}, err
	}

	if userName == "" {
		if len(users) == 0 {
			return ssooidcbackend.Subject{}, fmt.Errorf(
				"%w: the identity store has no users",
				ssooidcbackend.ErrUnknownUser,
			)
		}

		return slices.MinFunc(users, func(a, b ssooidcbackend.Subject) int {
			return strings.Compare(a.UserName, b.UserName)
		}), nil
	}

	for _, u := range users {
		if strings.EqualFold(u.UserName, userName) {
			return u, nil
		}
	}

	return ssooidcbackend.Subject{}, fmt.Errorf("%w: %s", ssooidcbackend.ErrUnknownUser, userName)
}

// principals returns the user plus the groups the user belongs to, keyed by principal ID.
func (s ssoIdentity) principals(subject ssooidcbackend.Subject) map[string]string {
	out := map[string]string{subject.UserID: ssoPrincipalUser}

	groups := s.identityStore().ListGroupMembershipsForMember(
		context.Background(), subject.IdentityStoreID, identitystorebackend.MemberID{UserID: subject.UserID},
	)
	for _, g := range groups {
		out[g.GroupID] = ssoPrincipalGroup
	}

	return out
}

// Entitlements lists the permission sets assigned to the user directly or through group membership.
func (s ssoIdentity) Entitlements(subject ssooidcbackend.Subject) ([]ssobackend.Entitlement, error) {
	admin := s.ssoAdmin()
	if admin == nil || s.identityStore() == nil {
		return nil, errSSODependencyUnavailable
	}

	principals := s.principals(subject)

	var out []ssobackend.Entitlement

	for _, bk := range admin.RegionBackends() {
		for _, inst := range bk.ListInstances() {
			if inst.IdentityStoreID == subject.IdentityStoreID {
				out = append(out, instanceEntitlements(bk, inst.InstanceArn, principals)...)
			}
		}
	}

	return out, nil
}

func instanceEntitlements(
	bk ssoadminbackend.StorageBackend, instanceArn string, principals map[string]string,
) []ssobackend.Entitlement {
	var out []ssobackend.Entitlement

	for id, kind := range principals {
		for _, a := range bk.ListAccountAssignmentsForPrincipal(instanceArn, id, kind) {
			ps, err := bk.DescribePermissionSet(instanceArn, a.PermissionSetArn)
			if err != nil {
				continue
			}

			out = append(out, ssobackend.Entitlement{
				AccountID: a.AccountID, PermissionSetArn: ps.PermissionSetArn,
				PermissionSetName: ps.Name, InstanceArn: instanceArn,
			})
		}
	}

	return out
}

// AccountInfo returns the Organizations name and email of accountID, or the ID itself when unknown.
func (s ssoIdentity) AccountInfo(accountID string) ssobackend.AccountInfo {
	if h, ok := s.byName["Organizations"].(*orgbackend.Handler); ok {
		if bk, bok := h.Backend.(*orgbackend.InMemoryBackend); bok {
			if acct, err := bk.DescribeAccount(accountID); err == nil {
				return ssobackend.AccountInfo{Name: acct.Name, Email: acct.Email}
			}
		}
	}

	return ssobackend.AccountInfo{Name: accountID}
}

func ssoRoleName(e ssobackend.Entitlement) string {
	sum := sha256.Sum256([]byte(e.InstanceArn + "|" + e.PermissionSetArn + "|" + e.AccountID))

	return ssoRolePrefix + e.PermissionSetName + "_" + hex.EncodeToString(sum[:])[:ssoRoleHashLen]
}

// ensureRole creates or refreshes the IAM role behind a permission set and returns its ARN.
func (s ssoIdentity) ensureRole(iamBk iambackend.StorageBackend, e ssobackend.Entitlement) (string, error) {
	name := ssoRoleName(e)

	role, err := iamBk.GetRole(name)
	if err != nil {
		role, err = iamBk.CreateRole(name, ssoRolePath, fmt.Sprintf(ssoTrustPolicyFormat, ssoServicePrincipal), "")
		if errors.Is(err, iambackend.ErrRoleAlreadyExists) {
			role, err = iamBk.GetRole(name)
		}

		if err != nil {
			return "", fmt.Errorf("create permission set role: %w", err)
		}
	}

	if err = s.syncPolicies(iamBk, name, e); err != nil {
		return "", err
	}

	return role.Arn, nil
}

// syncPolicies applies the permission set's inline and managed policies; managed policies
// the IAM backend does not know are skipped.
func (s ssoIdentity) syncPolicies(iamBk iambackend.StorageBackend, roleName string, e ssobackend.Entitlement) error {
	admin := s.ssoAdmin()
	if admin == nil {
		return errSSODependencyUnavailable
	}

	ps, err := admin.BackendFor(admin.Backend.Region()).DescribePermissionSet(e.InstanceArn, e.PermissionSetArn)
	if err != nil {
		return fmt.Errorf("describe permission set: %w", err)
	}

	if ps.InlinePolicy != "" {
		if err = iamBk.PutRolePolicy(roleName, ssoInlinePolicyName, ps.InlinePolicy); err != nil {
			return fmt.Errorf("apply inline policy: %w", err)
		}
	}

	for _, mp := range ps.ManagedPolicies {
		if err = iamBk.AttachRolePolicy(roleName, mp.Arn); err != nil && !errors.Is(err, iambackend.ErrPolicyNotFound) {
			return fmt.Errorf("attach managed policy: %w", err)
		}
	}

	return nil
}

// IssueCredentials assumes the permission set's role and returns its short-lived credentials.
func (s ssoIdentity) IssueCredentials(
	e ssobackend.Entitlement, subject ssooidcbackend.Subject,
) (ssobackend.Credentials, error) {
	iamH, iok := s.byName["IAM"].(*iambackend.Handler)
	stsH, sok := s.byName["STS"].(*stsbackend.Handler)

	if !iok || !sok {
		return ssobackend.Credentials{}, errSSODependencyUnavailable
	}

	stsBk, ok := stsH.Backend.(*stsbackend.InMemoryBackend)
	if !ok {
		return ssobackend.Credentials{}, errSSODependencyUnavailable
	}

	roleArn, err := s.ensureRole(iamH.Backend, e)
	if err != nil {
		return ssobackend.Credentials{}, err
	}

	session := ssoSessionNameInvalid.ReplaceAllString(subject.UserName, "-")
	if len(session) < ssoSessionNameMin {
		session += "-sso"
	}

	out, err := stsBk.AssumeRoleForService(ssoServicePrincipal, roleArn, session[:min(len(session), ssoSessionNameMax)])
	if err != nil {
		return ssobackend.Credentials{}, fmt.Errorf("assume permission set role: %w", err)
	}

	c := out.AssumeRoleResult.Credentials

	exp, err := time.Parse(time.RFC3339, c.Expiration)
	if err != nil {
		return ssobackend.Credentials{}, fmt.Errorf("parse credential expiration: %w", err)
	}

	return ssobackend.Credentials{
		AccessKeyID: c.AccessKeyID, SecretAccessKey: c.SecretAccessKey, SessionToken: c.SessionToken, Expiration: exp,
	}, nil
}

// wireSSO connects the OIDC and portal services to the identity services.
func wireSSO(byName map[string]service.Registerable) {
	ids := ssoIdentity{byName: byName}

	oidc, ok := byName["SSOOIDC"].(*ssooidcbackend.Handler)
	if !ok {
		return
	}

	oidc.SetSubjectResolver(ids)

	if portal, pok := byName["SSOPortal"].(*ssobackend.Handler); pok {
		portal.SetTokenSource(oidc.Backend)
		portal.SetDirectory(ids)
	}
}

// registerSSOEndpoints serves the emulator's browser sign-in endpoints for the OIDC flows.
func registerSSOEndpoints(e *echo.Echo, services []service.Registerable) {
	for _, svc := range services {
		h, ok := svc.(*ssooidcbackend.Handler)
		if !ok {
			continue
		}

		for _, method := range []string{http.MethodGet, http.MethodPost} {
			e.Add(method, ssooidcbackend.DevicePath, h.ServeDeviceVerification)
		}

		e.GET(ssooidcbackend.AuthorizePath, h.ServeAuthorize)
	}
}
