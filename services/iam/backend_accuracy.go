package iam

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"
)

// parseJSON is a thin wrapper around json.Unmarshal for readability.
func parseJSON(s string, v any) error {
	return json.Unmarshal([]byte(s), v) //nolint:wrapcheck // thin delegation; callers handle the error
}

// ---- Tag operations on resources (embedded in model) ----

// TagUser merges the given key-value pairs into the user's Tags field.
func (b *InMemoryBackend) TagUser(userName string, tags map[string]string) error {
	b.mu.Lock("TagUser")
	defer b.mu.Unlock()

	u, exists := b.users[userName]
	if !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if u.Tags == nil {
		u.Tags = make(map[string]string, len(tags))
	}

	maps.Copy(u.Tags, tags)
	b.users[userName] = u

	return nil
}

// UntagUser removes the given keys from the user's Tags field.
func (b *InMemoryBackend) UntagUser(userName string, keys []string) error {
	b.mu.Lock("UntagUser")
	defer b.mu.Unlock()

	u, exists := b.users[userName]
	if !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	for _, k := range keys {
		delete(u.Tags, k)
	}

	b.users[userName] = u

	return nil
}

// TagRole merges the given key-value pairs into the role's Tags field.
func (b *InMemoryBackend) TagRole(roleName string, tags map[string]string) error {
	b.mu.Lock("TagRole")
	defer b.mu.Unlock()

	r, exists := b.roles[roleName]
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if r.Tags == nil {
		r.Tags = make(map[string]string, len(tags))
	}

	maps.Copy(r.Tags, tags)

	b.roles[roleName] = r

	return nil
}

// UntagRole removes the given keys from the role's Tags field.
func (b *InMemoryBackend) UntagRole(roleName string, keys []string) error {
	b.mu.Lock("UntagRole")
	defer b.mu.Unlock()

	r, exists := b.roles[roleName]
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	for _, k := range keys {
		delete(r.Tags, k)
	}

	b.roles[roleName] = r

	return nil
}

// TagPolicy merges the given key-value pairs into the policy's Tags field.
func (b *InMemoryBackend) TagPolicy(policyArn string, tags map[string]string) error {
	b.mu.Lock("TagPolicy")
	defer b.mu.Unlock()

	polName, exists := b.policyByARN[policyArn]
	if !exists {
		return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	p, exists := b.policies[polName]
	if !exists {
		return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	if p.Tags == nil {
		p.Tags = make(map[string]string, len(tags))
	}

	maps.Copy(p.Tags, tags)
	b.policies[polName] = p

	return nil
}

// UntagPolicy removes the given keys from the policy's Tags field.
func (b *InMemoryBackend) UntagPolicy(policyArn string, keys []string) error {
	b.mu.Lock("UntagPolicy")
	defer b.mu.Unlock()

	polName, exists := b.policyByARN[policyArn]
	if !exists {
		return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	p, exists := b.policies[polName]
	if !exists {
		return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	for _, k := range keys {
		delete(p.Tags, k)
	}

	b.policies[polName] = p

	return nil
}

// TagGroup merges the given key-value pairs into the group's Tags field.
func (b *InMemoryBackend) TagGroup(groupName string, tags map[string]string) error {
	b.mu.Lock("TagGroup")
	defer b.mu.Unlock()

	g, exists := b.groups[groupName]
	if !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if g.Tags == nil {
		g.Tags = make(map[string]string, len(tags))
	}

	maps.Copy(g.Tags, tags)
	b.groups[groupName] = g

	return nil
}

// UntagGroup removes the given keys from the group's Tags field.
func (b *InMemoryBackend) UntagGroup(groupName string, keys []string) error {
	b.mu.Lock("UntagGroup")
	defer b.mu.Unlock()

	g, exists := b.groups[groupName]
	if !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	for _, k := range keys {
		delete(g.Tags, k)
	}

	b.groups[groupName] = g

	return nil
}

// ---- AccessKey LastUsed tracking ----

// RecordAccessKeyUsage updates the LastUsedDate, LastUsedRegion, and LastUsedServiceName
// for the given access key. Called from the auth layer on each authenticated request.
func (b *InMemoryBackend) RecordAccessKeyUsage(accessKeyID, region, serviceName string) {
	b.mu.Lock("RecordAccessKeyUsage")
	defer b.mu.Unlock()

	ak, exists := b.accessKeys[accessKeyID]
	if !exists {
		return
	}

	now := time.Now().UTC()
	ak.LastUsedDate = &now
	ak.LastUsedRegion = region
	ak.LastUsedServiceName = serviceName
	b.accessKeys[accessKeyID] = ak
}

// ---- Signing Certificates ----

// certIDPrefix is the AWS prefix for signing certificate IDs.
const certIDPrefix = "ASCA"

// certIDBytes is the number of random bytes for signing certificate ID suffix.
const certIDBytes = 8

// newSigningCertID generates a new unique signing certificate ID.
func newSigningCertID() string {
	b := make([]byte, certIDBytes)
	_, _ = rand.Read(b)

	return certIDPrefix + strings.ToUpper(hex.EncodeToString(b))
}

// UploadSigningCertificate stores a new X.509 signing certificate for a user.
func (b *InMemoryBackend) UploadSigningCertificate(userName, body string) (*SigningCertificate, error) {
	b.mu.Lock("UploadSigningCertificate")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if body == "" {
		return nil, fmt.Errorf("%w: certificate body must not be empty", ErrMalformedPolicyDocument)
	}

	cert := SigningCertificate{
		CertificateID:   newSigningCertID(),
		UserName:        userName,
		CertificateBody: body,
		Status:          accessKeyStatusActive,
		UploadDate:      time.Now().UTC(),
	}

	b.signingCertificates[cert.CertificateID] = cert

	return &cert, nil
}

// ListSigningCertificates returns all signing certificates for the given user.
// If userName is empty, all certificates are returned (admin usage).
func (b *InMemoryBackend) ListSigningCertificates(userName string) ([]SigningCertificate, error) {
	b.mu.RLock("ListSigningCertificates")
	defer b.mu.RUnlock()

	if userName != "" {
		if _, exists := b.users[userName]; !exists {
			return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
		}
	}

	var result []SigningCertificate

	for _, cert := range b.signingCertificates {
		if userName == "" || cert.UserName == userName {
			result = append(result, cert)
		}
	}

	return result, nil
}

// UpdateSigningCertificate changes the status of a signing certificate (Active/Inactive).
func (b *InMemoryBackend) UpdateSigningCertificate(certificateID, status string) error {
	b.mu.Lock("UpdateSigningCertificate")
	defer b.mu.Unlock()

	cert, exists := b.signingCertificates[certificateID]
	if !exists {
		return fmt.Errorf("%w: signing certificate %q not found", ErrAccessKeyNotFound, certificateID)
	}

	const inactive = "Inactive"

	if status != accessKeyStatusActive && status != inactive {
		return fmt.Errorf(
			"%w: invalid status %q; must be %s or Inactive",
			ErrInvalidAction, status, accessKeyStatusActive,
		)
	}

	cert.Status = status
	b.signingCertificates[certificateID] = cert

	return nil
}

// DeleteSigningCertificate removes a signing certificate.
func (b *InMemoryBackend) DeleteSigningCertificate(certificateID string) error {
	b.mu.Lock("DeleteSigningCertificate")
	defer b.mu.Unlock()

	if _, exists := b.signingCertificates[certificateID]; !exists {
		return fmt.Errorf("%w: signing certificate %q not found", ErrAccessKeyNotFound, certificateID)
	}

	delete(b.signingCertificates, certificateID)

	return nil
}

// ---- MFA Device Status State Machine ----

// setMFADeviceStatus updates a virtual MFA device's Status field.
// Caller must NOT hold b.mu (acquires write lock).
func (b *InMemoryBackend) setMFADeviceStatus(serialNumber, status string) error {
	b.mu.Lock("setMFADeviceStatus")
	defer b.mu.Unlock()

	dev, exists := b.virtualMFADevices[serialNumber]
	if !exists {
		return fmt.Errorf("%w: virtual MFA device %q not found", ErrInvalidAction, serialNumber)
	}

	dev.Status = status
	b.virtualMFADevices[serialNumber] = dev

	return nil
}

// ---- Permission Boundary enforcement ----

// boundaryDocForUser returns the policy document for the user's permissions boundary, or "".
// Caller must hold b.mu read-locked.
func (b *InMemoryBackend) boundaryDocForUser(userName string) string {
	u, exists := b.users[userName]
	if !exists || u.PermissionsBoundary == "" {
		return ""
	}

	return b.policyDocByARNLocked(u.PermissionsBoundary)
}

// boundaryDocForRole returns the policy document for the role's permissions boundary, or "".
// Caller must hold b.mu read-locked.
func (b *InMemoryBackend) boundaryDocForRole(roleName string) string {
	r, exists := b.roles[roleName]
	if !exists || r.PermissionsBoundary == "" {
		return ""
	}

	return b.policyDocByARNLocked(r.PermissionsBoundary)
}

// policyDocByARNLocked returns the current policy document for a managed policy ARN.
// Caller must hold b.mu read-locked.
func (b *InMemoryBackend) policyDocByARNLocked(policyArn string) string {
	polName, exists := b.policyByARN[policyArn]
	if !exists {
		return ""
	}

	p, exists := b.policies[polName]
	if !exists {
		return ""
	}

	return p.PolicyDocument
}

// ---- Trust Policy Evaluation ----

// trustStatement is a parsed trust policy statement for AssumeRole evaluation.
type trustStatement struct {
	Action    any                       `json:"Action"`
	Condition map[string]map[string]any `json:"Condition,omitempty"`
	Effect    string                    `json:"Effect"`
	Principal trustPrincipal            `json:"Principal"`
}

// trustPrincipal holds the parsed Principal from a trust policy statement.
// AWS Principal can be "*", a single ARN string, or a map of type→ARNs.
type trustPrincipal struct {
	AWS       []string
	Service   []string
	Federated []string
	AllowAll  bool
}

// UnmarshalJSON handles the three AWS Principal forms:
//   - "*"                         → AllowAll=true
//   - "arn:aws:iam::..."          → AWS=[arn]
//   - {"AWS":"...", "Service":"..."} → typed map
func (p *trustPrincipal) UnmarshalJSON(data []byte) error {
	// Try string first.
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		if s == "*" {
			p.AllowAll = true
		} else {
			p.AWS = []string{s}
		}

		return nil
	}

	// Try typed map.
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return err //nolint:wrapcheck // UnmarshalJSON interface; wrapping changes the type
	}

	for k, v := range m {
		var vals []string

		// Value can be a string or []string.
		var single string
		if err := json.Unmarshal(v, &single); err == nil {
			vals = []string{single}
		} else {
			if err2 := json.Unmarshal(v, &vals); err2 != nil {
				continue
			}
		}

		switch strings.ToUpper(k) {
		case "AWS":
			p.AWS = append(p.AWS, vals...)
		case "SERVICE":
			p.Service = append(p.Service, vals...)
		case "FEDERATED":
			p.Federated = append(p.Federated, vals...)
		}
	}

	return nil
}

// EvaluateAssumeRoleTrustPolicy evaluates a role's trust policy against an AssumeRole request.
// Returns true if the principal is allowed to assume the role under the given conditions.
//
// Parameters:
//   - trustPolicyJSON: the role's AssumeRolePolicyDocument
//   - principalARN: the ARN of the entity trying to assume the role
//   - externalID: sts:ExternalId value from the request (empty if not provided)
//   - mfaPresent: whether MFA was used in the request
//   - ctx: additional condition context (e.g. source IP)
func EvaluateAssumeRoleTrustPolicy(
	trustPolicyJSON, principalARN, externalID string, mfaPresent bool, ctx ConditionContext,
) bool {
	if trustPolicyJSON == "" {
		return false
	}

	// Inject STS-specific condition keys into the context.
	if ctx.Extra == nil {
		ctx.Extra = make(map[string]string)
	}

	const (
		mfaKey   = "aws:multifactorauthpresent"
		mfaTrue  = "true"
		mfaFalse = "false"
	)

	ctx.Extra["sts:externalid"] = externalID

	if mfaPresent {
		ctx.Extra[mfaKey] = mfaTrue
	} else {
		ctx.Extra[mfaKey] = mfaFalse
	}

	var doc struct {
		Statement []trustStatement `json:"Statement"`
	}

	expanded := SubstituteVariables(trustPolicyJSON, ctx)

	if err := parseJSON(expanded, &doc); err != nil {
		return false
	}

	for _, stmt := range doc.Statement {
		if !strings.EqualFold(stmt.Effect, "Allow") {
			continue
		}

		if !trustPrincipalMatches(stmt.Principal, principalARN) {
			continue
		}

		if !stmtActionMatchesTrust(stmt.Action, "sts:AssumeRole") {
			continue
		}

		if !conditionMatches(stmt.Condition, ctx) {
			continue
		}

		return true
	}

	return false
}

// trustPrincipalMatches returns true if principalARN is covered by the trust principal.
func trustPrincipalMatches(p trustPrincipal, principalARN string) bool {
	if p.AllowAll {
		return true
	}

	lower := strings.ToLower(principalARN)

	for _, arn := range p.AWS {
		if wildcardMatchCaseInsensitive(arn, lower) {
			return true
		}
	}

	for _, svc := range p.Service {
		if wildcardMatchCaseInsensitive(svc, lower) {
			return true
		}
	}

	for _, fed := range p.Federated {
		if wildcardMatchCaseInsensitive(fed, lower) {
			return true
		}
	}

	return false
}

// stmtActionMatchesTrust checks whether the trust statement Action includes the given action.
func stmtActionMatchesTrust(action any, want string) bool {
	for _, a := range anyStrings(action) {
		if wildcardMatchCaseInsensitive(a, want) {
			return true
		}
	}

	return false
}

// ---- Trust policy Principal validation ----

// validateTrustPolicyPrincipal parses the trust policy and validates the Principal field.
// Allowed shapes: "*", a string ARN, or a map with keys AWS/Service/Federated.
// Returns nil if the policy is empty or valid; returns ErrMalformedPolicyDocument otherwise.
func validateTrustPolicyPrincipal(policyJSON string) error {
	if policyJSON == "" {
		return nil
	}

	var doc struct {
		Statement []struct {
			Principal json.RawMessage `json:"Principal"`
		} `json:"Statement"`
	}

	if err := json.Unmarshal([]byte(policyJSON), &doc); err != nil {
		return nil //nolint:nilerr // JSON validity confirmed upstream; this path handles schema mismatch
	}

	for i, stmt := range doc.Statement {
		if stmt.Principal == nil {
			continue
		}

		var p trustPrincipal
		if err := json.Unmarshal(stmt.Principal, &p); err != nil {
			return fmt.Errorf(
				"%w: statement[%d] Principal has unsupported shape",
				ErrMalformedPolicyDocument, i,
			)
		}

		// Validate ARN format for AWS principals.
		for _, a := range p.AWS {
			if a == "*" {
				continue
			}

			if !strings.HasPrefix(strings.ToLower(a), "arn:aws") {
				return fmt.Errorf(
					"%w: statement[%d] Principal.AWS %q is not a valid ARN",
					ErrMalformedPolicyDocument, i, a,
				)
			}
		}
	}

	return nil
}

// ---- Server Certificates ----

// serverCertIDPrefix is the AWS-style prefix for server certificate IDs.
const serverCertIDPrefix = "ASCA"

// serverCertIDBytes is the number of random bytes for the server certificate ID suffix.
const serverCertIDBytes = 8

func newServerCertID() string {
	b := make([]byte, serverCertIDBytes)
	_, _ = rand.Read(b)

	return serverCertIDPrefix + strings.ToUpper(hex.EncodeToString(b))
}

// UploadServerCertificate stores a new server certificate.
func (b *InMemoryBackend) UploadServerCertificate(name, path, certBody, certChain string) (*ServerCertificate, error) {
	b.mu.Lock("UploadServerCertificate")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ServerCertificateName must not be empty", ErrMalformedPolicyDocument)
	}

	if certBody == "" {
		return nil, fmt.Errorf("%w: CertificateBody must not be empty", ErrMalformedPolicyDocument)
	}

	if _, exists := b.serverCertificates[name]; exists {
		return nil, fmt.Errorf("%w: server certificate %q already exists", ErrUserAlreadyExists, name)
	}

	normalizedPath := normPath(path)
	cert := ServerCertificate{
		UploadDate:            time.Now().UTC(),
		ServerCertificateName: name,
		ServerCertificateID:   newServerCertID(),
		Arn:                   "arn:aws:iam::" + b.accountID + ":server-certificate" + normalizedPath + name,
		Path:                  normalizedPath,
		CertificateBody:       certBody,
		CertificateChain:      certChain,
	}

	b.serverCertificates[name] = cert

	return &cert, nil
}

// GetServerCertificate retrieves a server certificate by name.
func (b *InMemoryBackend) GetServerCertificate(name string) (*ServerCertificate, error) {
	b.mu.RLock("GetServerCertificate")
	defer b.mu.RUnlock()

	cert, exists := b.serverCertificates[name]
	if !exists {
		return nil, fmt.Errorf("%w: server certificate %q not found", ErrUserNotFound, name)
	}

	return &cert, nil
}

// ListServerCertificates returns server certificates, filtered by path prefix if non-empty.
func (b *InMemoryBackend) ListServerCertificates(pathPrefix string) ([]ServerCertificate, error) {
	b.mu.RLock("ListServerCertificates")
	defer b.mu.RUnlock()

	var result []ServerCertificate

	for _, cert := range b.serverCertificates {
		if pathPrefix == "" || strings.HasPrefix(cert.Path, pathPrefix) {
			result = append(result, cert)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ServerCertificateName < result[j].ServerCertificateName
	})

	return result, nil
}

// UpdateServerCertificate renames a server certificate and/or changes its path.
func (b *InMemoryBackend) UpdateServerCertificate(name, newName, newPath string) error {
	b.mu.Lock("UpdateServerCertificate")
	defer b.mu.Unlock()

	cert, exists := b.serverCertificates[name]
	if !exists {
		return fmt.Errorf("%w: server certificate %q not found", ErrUserNotFound, name)
	}

	if newName != "" && newName != name {
		if _, exists := b.serverCertificates[newName]; exists {
			return fmt.Errorf("%w: server certificate %q already exists", ErrUserAlreadyExists, newName)
		}

		delete(b.serverCertificates, name)
		cert.ServerCertificateName = newName
		cert.Arn = "arn:aws:iam::" + b.accountID + ":server-certificate" + cert.Path + newName
	}

	if newPath != "" {
		normalizedPath := normPath(newPath)
		cert.Path = normalizedPath
		cert.Arn = "arn:aws:iam::" + b.accountID + ":server-certificate" + normalizedPath + cert.ServerCertificateName
	}

	b.serverCertificates[cert.ServerCertificateName] = cert

	return nil
}

// DeleteServerCertificate removes a server certificate.
func (b *InMemoryBackend) DeleteServerCertificate(name string) error {
	b.mu.Lock("DeleteServerCertificate")
	defer b.mu.Unlock()

	if _, exists := b.serverCertificates[name]; !exists {
		return fmt.Errorf("%w: server certificate %q not found", ErrUserNotFound, name)
	}

	delete(b.serverCertificates, name)

	return nil
}
