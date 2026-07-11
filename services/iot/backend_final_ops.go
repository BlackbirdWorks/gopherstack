package iot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Encryption configuration
// ---------------------------------------------------------------------------

// Real AWS IoT only defines two encryption types (see EncryptionType in the
// AWS SDK); there is no third "KMS_BASED" variant.
const (
	encryptionTypeAWSOwned        = "AWS_OWNED_KMS_KEY"
	encryptionTypeCustomerManaged = "CUSTOMER_MANAGED_KMS_KEY"

	configurationStatusHealthy = "HEALTHY"
)

// Generic AWS-style lifecycle status values shared across several final
// stub batch resources (authorizers, topic rule destinations).
const (
	statusActive     = "ACTIVE"
	statusEnabled    = "ENABLED"
	statusInProgress = "IN_PROGRESS"
)

// AccountEncryptionConfiguration represents the account-wide encryption
// configuration for data at rest.
type AccountEncryptionConfiguration struct {
	ConfigurationDetails *EncryptionConfigurationDetails `json:"configurationDetails,omitempty"`
	EncryptionType       string                          `json:"encryptionType"`
	KMSKeyARN            string                          `json:"kmsKeyArn,omitempty"`
	KMSAccessRoleARN     string                          `json:"kmsAccessRoleArn,omitempty"`
	LastModifiedDate     float64                         `json:"lastModifiedDate,omitempty"`
}

// EncryptionConfigurationDetails describes the health of the KMS key/role
// backing a customer-managed encryption configuration.
type EncryptionConfigurationDetails struct {
	ConfigurationStatus string `json:"configurationStatus"`
	ErrorCode           string `json:"errorCode,omitempty"`
	ErrorMessage        string `json:"errorMessage,omitempty"`
}

// UpdateEncryptionConfigurationInput is the input for UpdateEncryptionConfiguration.
type UpdateEncryptionConfigurationInput struct {
	EncryptionType   string `json:"encryptionType"`
	KMSKeyARN        string `json:"kmsKeyArn,omitempty"`
	KMSAccessRoleARN string `json:"kmsAccessRoleArn,omitempty"`
}

// DescribeEncryptionConfiguration returns the account's encryption
// configuration, defaulting to the AWS-owned key when never configured.
func (b *InMemoryBackend) DescribeEncryptionConfiguration() *AccountEncryptionConfiguration {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.accountEncryptionConfig == nil {
		return &AccountEncryptionConfiguration{
			EncryptionType:       encryptionTypeAWSOwned,
			ConfigurationDetails: &EncryptionConfigurationDetails{ConfigurationStatus: configurationStatusHealthy},
		}
	}

	cp := *b.accountEncryptionConfig
	if b.accountEncryptionConfig.ConfigurationDetails != nil {
		details := *b.accountEncryptionConfig.ConfigurationDetails
		cp.ConfigurationDetails = &details
	}

	return &cp
}

// UpdateEncryptionConfiguration validates and mutates the account's
// encryption configuration.
func (b *InMemoryBackend) UpdateEncryptionConfiguration(input *UpdateEncryptionConfigurationInput) error {
	switch input.EncryptionType {
	case encryptionTypeAWSOwned:
	case encryptionTypeCustomerManaged:
		if input.KMSKeyARN == "" {
			return fmt.Errorf("%w: kmsKeyArn is required for encryptionType %q", ErrValidation, input.EncryptionType)
		}

		if input.KMSAccessRoleARN == "" {
			return fmt.Errorf(
				"%w: kmsAccessRoleArn is required for encryptionType %q",
				ErrValidation,
				input.EncryptionType,
			)
		}
	default:
		return fmt.Errorf("%w: invalid encryptionType %q", ErrValidation, input.EncryptionType)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.accountEncryptionConfig = &AccountEncryptionConfiguration{
		EncryptionType:       input.EncryptionType,
		KMSKeyARN:            input.KMSKeyARN,
		KMSAccessRoleARN:     input.KMSAccessRoleARN,
		ConfigurationDetails: &EncryptionConfigurationDetails{ConfigurationStatus: configurationStatusHealthy},
		LastModifiedDate:     float64(time.Now().Unix()),
	}

	return nil
}

// ---------------------------------------------------------------------------
// TestAuthorization / TestInvokeAuthorizer
// ---------------------------------------------------------------------------

// AuthInfo describes one action/resources pair to evaluate authorization for.
type AuthInfo struct {
	ActionType string   `json:"actionType,omitempty"`
	Resources  []string `json:"resources"`
}

// PolicyIdentifier identifies a policy referenced by an authorization result.
type PolicyIdentifier struct {
	PolicyName string `json:"policyName,omitempty"`
	PolicyARN  string `json:"policyArn,omitempty"`
}

// AuthDecisionPolicies is the set of policies backing an allow/deny decision.
type AuthDecisionPolicies struct {
	Policies []PolicyIdentifier `json:"policies,omitempty"`
}

// AuthResult is the per-authInfo evaluation result of TestAuthorization.
type AuthResult struct {
	AuthInfo     *AuthInfo             `json:"authInfo,omitempty"`
	Allowed      *AuthDecisionPolicies `json:"allowed,omitempty"`
	Denied       *AuthDecisionPolicies `json:"denied,omitempty"`
	AuthDecision string                `json:"authDecision"`
}

// TestAuthorizationInput is the input for TestAuthorization.
type TestAuthorizationInput struct {
	Principal             string
	CognitoIdentityPoolID string
	ClientID              string
	PolicyNamesToAdd      []string
	PolicyNamesToSkip     []string
	AuthInfos             []AuthInfo
}

// actionTypeToIoTAction maps an AWS IoT ActionType to the policy action name
// it corresponds to in an IoT policy document (e.g. "iot:Publish").
func actionTypeToIoTAction(actionType string) string {
	switch strings.ToUpper(actionType) {
	case "PUBLISH":
		return "iot:Publish"
	case "SUBSCRIBE":
		return "iot:Subscribe"
	case "RECEIVE":
		return "iot:Receive"
	case "CONNECT":
		return "iot:Connect"
	default:
		return ""
	}
}

// TestAuthorization evaluates the effective policy set for a principal (plus
// PolicyNamesToAdd, minus PolicyNamesToSkip) against each requested
// authInfo/resource pair using real, stored IoT policy documents.
func (b *InMemoryBackend) TestAuthorization(input *TestAuthorizationInput) ([]*AuthResult, error) {
	if input.Principal == "" {
		return nil, fmt.Errorf("%w: principal is required", ErrValidation)
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	policies, err := b.effectiveTestPolicies(input)
	if err != nil {
		return nil, err
	}

	statements := make([]iamStatementWithPolicy, 0, len(policies))

	for _, p := range policies {
		stmts, parseErr := parseIoTPolicyDocument(p.PolicyDocument)
		if parseErr != nil {
			continue
		}

		for _, s := range stmts {
			statements = append(statements, iamStatementWithPolicy{
				statement: s,
				policy:    PolicyIdentifier{PolicyName: p.PolicyName, PolicyARN: p.ARN},
			})
		}
	}

	out := make([]*AuthResult, 0, len(input.AuthInfos))

	for i := range input.AuthInfos {
		out = append(out, evaluateAuthInfo(&input.AuthInfos[i], statements))
	}

	return out, nil
}

// effectiveTestPolicies resolves the policy set to evaluate for
// TestAuthorization: those attached to the principal, plus PolicyNamesToAdd,
// minus PolicyNamesToSkip. Must be called with b.mu held (read or write).
func (b *InMemoryBackend) effectiveTestPolicies(input *TestAuthorizationInput) ([]*Policy, error) {
	seen := make(map[string]bool)

	var policies []*Policy

	for policyName, targets := range b.policyTargets {
		if !slices.Contains(targets, input.Principal) || seen[policyName] {
			continue
		}

		if p, ok := b.policies.Get(policyName); ok {
			cp := *p
			policies = append(policies, &cp)
			seen[policyName] = true
		}
	}

	for _, name := range input.PolicyNamesToAdd {
		if seen[name] {
			continue
		}

		p, ok := b.policies.Get(name)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrPolicyNotFound, name)
		}

		cp := *p
		policies = append(policies, &cp)
		seen[name] = true
	}

	skip := make(map[string]bool, len(input.PolicyNamesToSkip))
	for _, name := range input.PolicyNamesToSkip {
		skip[name] = true
	}

	filtered := make([]*Policy, 0, len(policies))

	for _, p := range policies {
		if !skip[p.PolicyName] {
			filtered = append(filtered, p)
		}
	}

	return filtered, nil
}

// iamStatementWithPolicy pairs a parsed IAM-style statement with the policy
// identifier it came from, for reporting in AuthResult.
type iamStatementWithPolicy struct {
	policy    PolicyIdentifier
	statement iamStatement
}

// evaluateAuthInfo determines the AuthResult for one authInfo entry by
// evaluating every requested resource against the statement set. If any
// resource is denied/implicitly-denied, that is reflected in the aggregate
// per AWS's "explicit deny wins, else any implicit deny wins" semantics.
func evaluateAuthInfo(info *AuthInfo, statements []iamStatementWithPolicy) *AuthResult {
	action := actionTypeToIoTAction(info.ActionType)

	result := &AuthResult{AuthInfo: info}

	var (
		allowPolicies []PolicyIdentifier
		denyPolicies  []PolicyIdentifier
		anyAllowed    bool
	)

	seenAllow := map[string]bool{}
	seenDeny := map[string]bool{}

	resources := info.Resources
	if len(resources) == 0 {
		resources = []string{"*"}
	}

	for _, resource := range resources {
		matched := false

		for _, sw := range statements {
			if !statementMatches(sw.statement, action, resource) {
				continue
			}

			matched = true

			if strings.EqualFold(sw.statement.Effect, "Deny") {
				if !seenDeny[sw.policy.PolicyName] {
					denyPolicies = append(denyPolicies, sw.policy)
					seenDeny[sw.policy.PolicyName] = true
				}
			} else if !seenAllow[sw.policy.PolicyName] {
				allowPolicies = append(allowPolicies, sw.policy)
				seenAllow[sw.policy.PolicyName] = true
			}
		}

		if matched {
			anyAllowed = true
		}
	}

	switch {
	case len(denyPolicies) > 0:
		result.AuthDecision = "EXPLICIT_DENY"
		result.Denied = &AuthDecisionPolicies{Policies: denyPolicies}
	case anyAllowed && len(allowPolicies) > 0:
		result.AuthDecision = "ALLOWED"
		result.Allowed = &AuthDecisionPolicies{Policies: allowPolicies}
	default:
		result.AuthDecision = "IMPLICIT_DENY"
		result.Denied = &AuthDecisionPolicies{}
	}

	return result
}

// iamStatement is a minimal IAM-style policy statement as used in IoT policy
// documents: {"Effect":"Allow","Action":"iot:*","Resource":"*"}.
type iamStatement struct {
	Effect   string
	Action   []string
	Resource []string
}

// flexStringList unmarshals a JSON value that may be either a single string
// or an array of strings, as IAM-style Action/Resource fields allow.
type flexStringList []string

func (f *flexStringList) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*f = []string{single}

		return nil
	}

	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}

	*f = list

	return nil
}

// parseIoTPolicyDocument parses an IAM-style IoT policy document JSON string
// into a slice of statements.
func parseIoTPolicyDocument(doc string) ([]iamStatement, error) {
	var raw struct {
		Statement []struct {
			Effect   string         `json:"Effect"`
			Action   flexStringList `json:"Action"`
			Resource flexStringList `json:"Resource"`
		} `json:"Statement"`
	}

	if err := json.Unmarshal([]byte(doc), &raw); err != nil {
		return nil, err
	}

	out := make([]iamStatement, 0, len(raw.Statement))

	for _, s := range raw.Statement {
		out = append(out, iamStatement{
			Effect:   s.Effect,
			Action:   s.Action,
			Resource: s.Resource,
		})
	}

	return out, nil
}

// statementMatches reports whether stmt authorizes the given action on the
// given resource, supporting "*" wildcards as IAM/IoT policies do.
func statementMatches(stmt iamStatement, action, resource string) bool {
	actionOK := action == ""

	for _, a := range stmt.Action {
		if iamGlobMatch(a, action) {
			actionOK = true

			break
		}
	}

	if !actionOK {
		return false
	}

	for _, r := range stmt.Resource {
		if iamGlobMatch(r, resource) {
			return true
		}
	}

	return false
}

// iamGlobMatch reports whether value matches an IAM-style pattern containing
// "*" wildcards (e.g. "iot:*", "arn:aws:iot:*:*:topic/foo/*").
func iamGlobMatch(pattern, value string) bool {
	if pattern == "*" {
		return true
	}

	quoted := regexp.QuoteMeta(pattern)
	reStr := "^" + strings.ReplaceAll(quoted, `\*`, ".*") + "$"

	re, err := regexp.Compile(reStr)
	if err != nil {
		return pattern == value
	}

	return re.MatchString(value)
}

// TestInvokeAuthorizerInput is the input for TestInvokeAuthorizer.
type TestInvokeAuthorizerInput struct {
	Token          string
	TokenSignature string
	MQTTClientID   string
}

// TestInvokeAuthorizerOutput is the output for TestInvokeAuthorizer.
type TestInvokeAuthorizerOutput struct {
	PrincipalID              string
	PolicyDocuments          []string
	IsAuthenticated          bool
	DisconnectAfterInSeconds int32
	RefreshAfterInSeconds    int32
}

const (
	authorizerDisconnectAfterSeconds = 3600
	authorizerRefreshAfterSeconds    = 300
)

// TestInvokeAuthorizer evaluates a stored custom authorizer's configuration
// (status, signing requirement) against the supplied token/signature and
// returns a deterministic authentication result derived from that stored
// state.
func (b *InMemoryBackend) TestInvokeAuthorizer(
	authorizerName string,
	input *TestInvokeAuthorizerInput,
) (*TestInvokeAuthorizerOutput, error) {
	b.mu.RLock()
	a, ok := b.authorizers.Get(authorizerName)
	b.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("authorizer %q not found: %w", authorizerName, ErrResourceNotFound)
	}

	out := &TestInvokeAuthorizerOutput{}

	switch {
	case a.Status != "" && a.Status != statusActive:
		out.IsAuthenticated = false
	case !a.SigningDisabled && input.TokenSignature == "":
		out.IsAuthenticated = false
	default:
		out.IsAuthenticated = true

		principal := input.Token
		if principal == "" {
			principal = input.MQTTClientID
		}

		out.PrincipalID = authorizerPrincipalID(authorizerName, principal)
		out.PolicyDocuments = []string{defaultAuthorizerPolicyDocument()}
		out.DisconnectAfterInSeconds = authorizerDisconnectAfterSeconds
		out.RefreshAfterInSeconds = authorizerRefreshAfterSeconds
	}

	return out, nil
}

// authorizerPrincipalID deterministically derives a principal ID from the
// authorizer name and the caller-supplied token/clientId.
func authorizerPrincipalID(authorizerName, seed string) string {
	sum := sha256.Sum256([]byte(authorizerName + "/" + seed))

	return "authz-" + hex.EncodeToString(sum[:8])
}

// defaultAuthorizerPolicyDocument returns the (deterministic) IAM-style
// policy document a passing custom-authorizer invocation grants, absent a
// real Lambda to invoke.
func defaultAuthorizerPolicyDocument() string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`
}

// ---------------------------------------------------------------------------
// DetachPrincipalPolicy
// ---------------------------------------------------------------------------

// DetachPrincipalPolicy removes a principal from a policy's target list,
// mirroring AttachPrincipalPolicy/DetachPolicy's use of policyTargets.
func (b *InMemoryBackend) DetachPrincipalPolicy(policyName, principal string) error {
	if policyName == "" || principal == "" {
		return fmt.Errorf("%w: policyName and principal are required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.policies.Has(policyName) {
		return fmt.Errorf("%w: %s", ErrPolicyNotFound, policyName)
	}

	targets := b.policyTargets[policyName]
	filtered := make([]string, 0, len(targets))

	for _, t := range targets {
		if t != principal {
			filtered = append(filtered, t)
		}
	}

	b.policyTargets[policyName] = filtered

	return nil
}

// ---------------------------------------------------------------------------
// ConfirmTopicRuleDestination
// ---------------------------------------------------------------------------

// ConfirmTopicRuleDestination transitions a topic rule destination created
// with an HTTP URL configuration from IN_PROGRESS to ENABLED, given the
// confirmation token that was generated at creation time.
func (b *InMemoryBackend) ConfirmTopicRuleDestination(token string) error {
	if token == "" {
		return fmt.Errorf("%w: confirmationToken is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, dest := range b.topicRuleDestinations.All() {
		if dest.ConfirmationToken != "" && dest.ConfirmationToken == token {
			dest.Status = statusEnabled
			dest.ConfirmationToken = ""

			return nil
		}
	}

	return fmt.Errorf("%w: invalid or expired confirmation token", ErrValidation)
}

// ---------------------------------------------------------------------------
// DeleteCommandExecution
// ---------------------------------------------------------------------------

// DeleteCommandExecution removes a stored command execution identified by
// its executionId and (optionally) the ARN of its target device, matching
// AWS's real request shape where executions are addressed by
// executionId+targetArn rather than commandId.
func (b *InMemoryBackend) DeleteCommandExecution(executionID, targetARN string) error {
	if executionID == "" {
		return fmt.Errorf("%w: executionId is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for key, ex := range b.commandExecutions {
		if ex.ExecutionID != executionID {
			continue
		}

		if targetARN != "" && ex.ThingARN != targetARN {
			continue
		}

		delete(b.commandExecutions, key)

		return nil
	}

	return fmt.Errorf("command execution %q not found: %w", executionID, ErrResourceNotFound)
}

// AddCommandExecutionInternal seeds a command execution directly into the
// backend for testing (there is no public CreateCommandExecution control-
// plane operation; executions are normally created by device SDKs).
func (b *InMemoryBackend) AddCommandExecutionInternal(commandID, executionID string, ex IoTCommandExecution) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := ex
	cp.ExecutionID = executionID
	b.commandExecutions[b.commandExecutionKey(commandID, executionID)] = &cp
}

// ---------------------------------------------------------------------------
// GetBehaviorModelTrainingSummaries
// ---------------------------------------------------------------------------

// BehaviorModelTrainingSummary reports ML Detect model-training progress for
// one behavior of a security profile.
type BehaviorModelTrainingSummary struct {
	SecurityProfileName             string  `json:"securityProfileName"`
	BehaviorName                    string  `json:"behaviorName"`
	ModelStatus                     string  `json:"modelStatus,omitempty"`
	DatapointsCollectionPercentage  float64 `json:"datapointsCollectionPercentage,omitempty"`
	LastModelRefreshDate            float64 `json:"lastModelRefreshDate,omitempty"`
	TrainingDataCollectionStartDate float64 `json:"trainingDataCollectionStartDate,omitempty"`
}

// GetBehaviorModelTrainingSummaries returns stored per-behavior training
// summaries, optionally scoped to one security profile, paginated.
func (b *InMemoryBackend) GetBehaviorModelTrainingSummaries(
	securityProfileName string, maxResults int32, nextToken string,
) ([]*BehaviorModelTrainingSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var all []*BehaviorModelTrainingSummary

	if securityProfileName != "" {
		if !b.securityProfiles.Has(securityProfileName) {
			return nil, "", fmt.Errorf(
				"security profile %q not found: %w", securityProfileName, ErrResourceNotFound,
			)
		}

		for _, s := range b.behaviorTrainingSummaries[securityProfileName] {
			cp := *s
			all = append(all, &cp)
		}
	} else {
		for _, name := range sortedKeys(b.behaviorTrainingSummaries) {
			for _, s := range b.behaviorTrainingSummaries[name] {
				cp := *s
				all = append(all, &cp)
			}
		}
	}

	page, next := paginateMaps(all, searchPageSize(maxResults), searchStartOffset(nextToken))

	return page, next, nil
}

// AddBehaviorModelTrainingSummaryInternal seeds a training summary for a
// security profile (there is no public API to trigger ML Detect training;
// AWS populates this asynchronously once enough data is collected).
func (b *InMemoryBackend) AddBehaviorModelTrainingSummaryInternal(
	securityProfileName string, s BehaviorModelTrainingSummary,
) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := s
	cp.SecurityProfileName = securityProfileName
	b.behaviorTrainingSummaries[securityProfileName] = append(b.behaviorTrainingSummaries[securityProfileName], &cp)
}

// ---------------------------------------------------------------------------
// ListOutgoingCertificates
// ---------------------------------------------------------------------------

// OutgoingCertificate describes a certificate pending transfer to another
// AWS account.
type OutgoingCertificate struct {
	CertificateARN  string  `json:"certificateArn"`
	CertificateID   string  `json:"certificateId"`
	TransferredTo   string  `json:"transferredTo,omitempty"`
	TransferMessage string  `json:"transferMessage,omitempty"`
	CreationDate    float64 `json:"creationDate,omitempty"`
	TransferDate    float64 `json:"transferDate,omitempty"`
}

// ListOutgoingCertificates lists certificates whose status is
// PENDING_TRANSFER, derived from real, stored certificate state.
func (b *InMemoryBackend) ListOutgoingCertificates() []*OutgoingCertificate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*OutgoingCertificate, 0)

	for _, v := range b.certificates.Snapshot() {
		cert := v
		if cert.Status != certStatusPendingTransfer {
			continue
		}

		out = append(out, &OutgoingCertificate{
			CertificateARN: cert.ARN,
			CertificateID:  cert.CertificateID,
			CreationDate:   float64(cert.CreatedAt.Unix()),
			TransferDate:   float64(cert.LastModifiedAt.Unix()),
		})
	}

	return out
}

// ---------------------------------------------------------------------------
// SBOM validation / disassociation
// ---------------------------------------------------------------------------

// SbomValidationResult reports the outcome of validating one SBOM file
// associated with a package version.
type SbomValidationResult struct {
	FileName         string `json:"fileName,omitempty"`
	ValidationResult string `json:"validationResult"`
	ErrorCode        string `json:"errorCode,omitempty"`
	ErrorMessage     string `json:"errorMessage,omitempty"`
}

// computeSbomValidationResult deterministically evaluates an associated SBOM
// document: a well-formed S3 location succeeds, a missing/incomplete one
// fails with the AWS INCOMPATIBLE_FORMAT error code.
func computeSbomValidationResult(sbom *SbomDocument) *SbomValidationResult {
	if sbom == nil || sbom.S3Location == nil || sbom.S3Location.Bucket == "" || sbom.S3Location.Key == "" {
		return &SbomValidationResult{
			ValidationResult: "FAILED",
			ErrorCode:        "INCOMPATIBLE_FORMAT",
			ErrorMessage:     "SBOM file location is missing or incomplete",
		}
	}

	return &SbomValidationResult{
		FileName:         sbom.S3Location.Key,
		ValidationResult: "SUCCEEDED",
	}
}

// DisassociateSbomFromPackageVersion clears the SBOM (and its validation
// results) associated with a package version.
func (b *InMemoryBackend) DisassociateSbomFromPackageVersion(packageName, versionName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.requirePackageVersionLocked(packageName, versionName); err != nil {
		return err
	}

	key := packageVersionKey(packageName, versionName)
	delete(b.packageVersionSboms, key)
	delete(b.sbomValidationResults, key)

	return nil
}

// ListSbomValidationResults returns the stored validation results for a
// package version's associated SBOM, paginated.
func (b *InMemoryBackend) ListSbomValidationResults(
	packageName, versionName string, maxResults int32, nextToken string,
) ([]*SbomValidationResult, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.requirePackageVersionLocked(packageName, versionName); err != nil {
		return nil, "", err
	}

	src := b.sbomValidationResults[packageVersionKey(packageName, versionName)]
	out := make([]*SbomValidationResult, len(src))

	for i, r := range src {
		cp := *r
		out[i] = &cp
	}

	page, next := paginateMaps(out, searchPageSize(maxResults), searchStartOffset(nextToken))

	return page, next, nil
}

// requirePackageVersionLocked returns ErrResourceNotFound if the given
// package version does not exist (i.e. CreateIoTPackageVersion was never
// called for it). Must be called with b.mu held.
func (b *InMemoryBackend) requirePackageVersionLocked(packageName, versionName string) error {
	if b.packageVersions2[packageName] == nil {
		return fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}

	if _, ok := b.packageVersions2[packageName][versionName]; !ok {
		return fmt.Errorf("package version %q/%q not found: %w", packageName, versionName, ErrResourceNotFound)
	}

	return nil
}

// ---------------------------------------------------------------------------
// ListMetricValues
// ---------------------------------------------------------------------------

// MetricDatapoint is one reported value of a security-profile metric for a
// thing at a point in time.
type MetricDatapoint struct {
	Value     *MetricValueData `json:"value,omitempty"`
	Timestamp float64          `json:"timestamp"`
}

// MetricValueData is the polymorphic value of a metric datapoint.
type MetricValueData struct {
	Count  *int64   `json:"count,omitempty"`
	Number *float64 `json:"number,omitempty"`
	Cidrs  []string `json:"cidrs,omitempty"`
	Ports  []int32  `json:"ports,omitempty"`
}

// metricValueKey builds the composite key for a thing/metric pair.
func metricValueKey(thingName, metricName string) string {
	return thingName + "/" + metricName
}

// ListMetricValues returns stored metric datapoints for a thing/metric over
// [startTime, endTime], paginated.
func (b *InMemoryBackend) ListMetricValues(
	thingName, metricName string, startTime, endTime float64, maxResults int32, nextToken string,
) ([]*MetricDatapoint, string, error) {
	if thingName == "" || metricName == "" {
		return nil, "", fmt.Errorf("%w: thingName and metricName are required", ErrValidation)
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.things.Has(thingName) {
		return nil, "", fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	all := b.metricValues[metricValueKey(thingName, metricName)]
	filtered := make([]*MetricDatapoint, 0, len(all))

	for _, dp := range all {
		if startTime > 0 && dp.Timestamp < startTime {
			continue
		}

		if endTime > 0 && dp.Timestamp > endTime {
			continue
		}

		cp := *dp
		filtered = append(filtered, &cp)
	}

	page, next := paginateMaps(filtered, searchPageSize(maxResults), searchStartOffset(nextToken))

	return page, next, nil
}

// AddMetricValueInternal seeds a metric datapoint for a thing/metric pair
// (there is no public PutMetricValue control-plane operation; values are
// normally reported by the device SDK's Device Defender metrics agent).
func (b *InMemoryBackend) AddMetricValueInternal(thingName, metricName string, dp MetricDatapoint) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := metricValueKey(thingName, metricName)
	cp := dp
	b.metricValues[key] = append(b.metricValues[key], &cp)
}

// ---------------------------------------------------------------------------
// GetThingConnectivityData
// ---------------------------------------------------------------------------

// ThingConnectivityData is a thing's most recently reported connectivity
// status.
type ThingConnectivityData struct {
	DisconnectReason string  `json:"disconnectReason,omitempty"`
	Timestamp        float64 `json:"timestamp,omitempty"`
	Connected        bool    `json:"connected"`
}

// GetThingConnectivityData returns a thing's stored connectivity data,
// defaulting to "not connected" when nothing has been recorded yet.
func (b *InMemoryBackend) GetThingConnectivityData(thingName string) (*ThingConnectivityData, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.things.Has(thingName) {
		return nil, fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	if cd, ok := b.thingConnectivity[thingName]; ok {
		cp := *cd

		return &cp, nil
	}

	return &ThingConnectivityData{Connected: false}, nil
}

// SetThingConnectivityInternal seeds/updates a thing's connectivity data for
// testing (real connectivity is derived from live MQTT session events, which
// this in-memory backend does not yet wire into fleet indexing).
func (b *InMemoryBackend) SetThingConnectivityInternal(thingName string, data ThingConnectivityData) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := data
	b.thingConnectivity[thingName] = &cp
}

// ---------------------------------------------------------------------------
// DescribeProvisioningTemplateVersion
// ---------------------------------------------------------------------------

// DescribeProvisioningTemplateVersion returns a specific stored version of a
// provisioning template.
func (b *InMemoryBackend) DescribeProvisioningTemplateVersion(
	name string, versionID int32,
) (*ProvisioningTemplateVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.provTemplates.Has(name) {
		return nil, fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}

	for _, v := range b.provTemplateVersions[name] {
		if v.VersionID == versionID {
			cp := *v

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("version %d not found: %w", versionID, ErrResourceNotFound)
}

// ---------------------------------------------------------------------------
// ValidateSecurityProfileBehaviors
// ---------------------------------------------------------------------------

// SecurityProfileBehavior describes one behavior to validate.
type SecurityProfileBehavior struct {
	Criteria *SecurityProfileBehaviorCriteria `json:"criteria,omitempty"`
	Name     string                           `json:"name"`
	Metric   string                           `json:"metric,omitempty"`
}

// SecurityProfileBehaviorCriteria is the criteria portion of a behavior.
type SecurityProfileBehaviorCriteria struct {
	ComparisonOperator           string `json:"comparisonOperator,omitempty"`
	DurationSeconds              int32  `json:"durationSeconds,omitempty"`
	ConsecutiveDatapointsToAlarm int32  `json:"consecutiveDatapointsToAlarm,omitempty"`
	ConsecutiveDatapointsToClear int32  `json:"consecutiveDatapointsToClear,omitempty"`
}

// isValidComparisonOperator reports whether op is one of the AWS IoT Device
// Defender comparison operators (types.ComparisonOperator).
func isValidComparisonOperator(op string) bool {
	switch op {
	case "less-than", "less-than-equals", "greater-than", "greater-than-equals",
		"in-cidr-set", "not-in-cidr-set", "in-port-set", "not-in-port-set",
		"in-set", "not-in-set":
		return true
	default:
		return false
	}
}

// ValidateSecurityProfileBehaviors validates the supplied behaviors against
// AWS's structural rules (name required, criteria required, comparison
// operator must be a known value, non-negative durations/thresholds).
func (b *InMemoryBackend) ValidateSecurityProfileBehaviors(
	behaviors []SecurityProfileBehavior,
) (bool, []string) {
	errs := make([]string, 0, len(behaviors))

	for _, beh := range behaviors {
		errs = append(errs, validateOneBehavior(beh)...)
	}

	return len(errs) == 0, errs
}

func validateOneBehavior(beh SecurityProfileBehavior) []string {
	var errs []string

	if beh.Name == "" {
		errs = append(errs, "behavior name is required")
	}

	if beh.Criteria == nil {
		errs = append(errs, fmt.Sprintf("behavior %q: criteria is required", beh.Name))

		return errs
	}

	if beh.Criteria.ComparisonOperator != "" && !isValidComparisonOperator(beh.Criteria.ComparisonOperator) {
		errs = append(errs, fmt.Sprintf(
			"behavior %q: invalid comparisonOperator %q", beh.Name, beh.Criteria.ComparisonOperator,
		))
	}

	if beh.Criteria.DurationSeconds < 0 {
		errs = append(errs, fmt.Sprintf("behavior %q: durationSeconds must not be negative", beh.Name))
	}

	if beh.Criteria.ConsecutiveDatapointsToAlarm < 0 {
		errs = append(errs, fmt.Sprintf("behavior %q: consecutiveDatapointsToAlarm must not be negative", beh.Name))
	}

	if beh.Criteria.ConsecutiveDatapointsToClear < 0 {
		errs = append(errs, fmt.Sprintf("behavior %q: consecutiveDatapointsToClear must not be negative", beh.Name))
	}

	return errs
}

// ---------------------------------------------------------------------------
// Persistence helpers
// ---------------------------------------------------------------------------

// finalOpsSnapshot holds the deep-copied final-stub-batch state for
// persistence, mirroring the deviceDefenderSnapshot pattern.
type finalOpsSnapshot struct {
	AccountEncryptionConfig   *AccountEncryptionConfiguration
	SbomValidationResults     map[string][]*SbomValidationResult
	MetricValues              map[string][]*MetricDatapoint
	ThingConnectivity         map[string]*ThingConnectivityData
	BehaviorTrainingSummaries map[string][]*BehaviorModelTrainingSummary
}

// snapshotFinalOps deep-copies all final-stub-batch state. Must be called
// with b.mu held (read or write).
func (b *InMemoryBackend) snapshotFinalOps() finalOpsSnapshot {
	var accountEncryptionConfig *AccountEncryptionConfiguration
	if b.accountEncryptionConfig != nil {
		cp := *b.accountEncryptionConfig
		accountEncryptionConfig = &cp
	}

	sbomValidationResults := make(map[string][]*SbomValidationResult, len(b.sbomValidationResults))
	for k, v := range b.sbomValidationResults {
		sbomValidationResults[k] = cloneSbomValidationResults(v)
	}

	metricValues := make(map[string][]*MetricDatapoint, len(b.metricValues))
	for k, v := range b.metricValues {
		metricValues[k] = cloneMetricDatapoints(v)
	}

	thingConnectivity := make(map[string]*ThingConnectivityData, len(b.thingConnectivity))
	for k, v := range b.thingConnectivity {
		cp := *v
		thingConnectivity[k] = &cp
	}

	behaviorTrainingSummaries := make(map[string][]*BehaviorModelTrainingSummary, len(b.behaviorTrainingSummaries))
	for k, v := range b.behaviorTrainingSummaries {
		behaviorTrainingSummaries[k] = cloneBehaviorTrainingSummaries(v)
	}

	return finalOpsSnapshot{
		AccountEncryptionConfig:   accountEncryptionConfig,
		SbomValidationResults:     sbomValidationResults,
		MetricValues:              metricValues,
		ThingConnectivity:         thingConnectivity,
		BehaviorTrainingSummaries: behaviorTrainingSummaries,
	}
}

// restoreFinalOps restores final-stub-batch state from a snapshot. Must be
// called with b.mu held (write).
func (b *InMemoryBackend) restoreFinalOps(snap finalOpsSnapshot) {
	if snap.AccountEncryptionConfig != nil {
		cp := *snap.AccountEncryptionConfig
		b.accountEncryptionConfig = &cp
	} else {
		b.accountEncryptionConfig = nil
	}

	b.sbomValidationResults = make(map[string][]*SbomValidationResult, len(snap.SbomValidationResults))
	for k, v := range snap.SbomValidationResults {
		b.sbomValidationResults[k] = cloneSbomValidationResults(v)
	}

	b.metricValues = make(map[string][]*MetricDatapoint, len(snap.MetricValues))
	for k, v := range snap.MetricValues {
		b.metricValues[k] = cloneMetricDatapoints(v)
	}

	b.thingConnectivity = make(map[string]*ThingConnectivityData, len(snap.ThingConnectivity))
	for k, v := range snap.ThingConnectivity {
		cp := *v
		b.thingConnectivity[k] = &cp
	}

	b.behaviorTrainingSummaries = make(map[string][]*BehaviorModelTrainingSummary, len(snap.BehaviorTrainingSummaries))
	for k, v := range snap.BehaviorTrainingSummaries {
		b.behaviorTrainingSummaries[k] = cloneBehaviorTrainingSummaries(v)
	}
}

func cloneSbomValidationResults(src []*SbomValidationResult) []*SbomValidationResult {
	out := make([]*SbomValidationResult, len(src))
	for i, r := range src {
		cp := *r
		out[i] = &cp
	}

	return out
}

func cloneMetricDatapoints(src []*MetricDatapoint) []*MetricDatapoint {
	out := make([]*MetricDatapoint, len(src))
	for i, dp := range src {
		cp := *dp
		out[i] = &cp
	}

	return out
}

func cloneBehaviorTrainingSummaries(src []*BehaviorModelTrainingSummary) []*BehaviorModelTrainingSummary {
	out := make([]*BehaviorModelTrainingSummary, len(src))
	for i, s := range src {
		cp := *s
		out[i] = &cp
	}

	return out
}

// ensureNonNilFinalOpsSnap defaults nil maps in a restored snapshot's
// final-stub-batch fields to empty maps.
func ensureNonNilFinalOpsSnap(snap *backendSnapshot) {
	if snap.SbomValidationResults == nil {
		snap.SbomValidationResults = make(map[string][]*SbomValidationResult)
	}

	if snap.MetricValues == nil {
		snap.MetricValues = make(map[string][]*MetricDatapoint)
	}

	if snap.ThingConnectivity == nil {
		snap.ThingConnectivity = make(map[string]*ThingConnectivityData)
	}

	if snap.BehaviorTrainingSummaries == nil {
		snap.BehaviorTrainingSummaries = make(map[string][]*BehaviorModelTrainingSummary)
	}
}
