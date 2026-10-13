package workmail

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPageSize = 100
	minPageSize     = 1
	maxAliasLen     = 62
	minDomainLen    = 3
	maxDomainLen    = 209

	orgStateRequested = "REQUESTED"
	orgStateCreating  = "CREATING"
	jobStateRunning   = "RUNNING"
	jobStateCompleted = "COMPLETED"
	jobProgressDone   = 100
	pageTokenPrefix   = "wm1:"

	maxUserNameLen     = 64
	maxGroupNameLen    = 256
	maxResourceNameLen = 20
)

var (
	aliasPattern  = regexp.MustCompile(`^[0-9a-zA-Z]+(-[0-9a-zA-Z]+)*$`)
	namePattern   = regexp.MustCompile(`^[\w\-.]+(@[a-zA-Z0-9.\-]+\.[a-zA-Z0-9]{2,})?$`)
	domainPattern = regexp.MustCompile(`^[a-zA-Z0-9.-]+\.[a-zA-Z-]{2,}$`)
)

// SetLifecycleDelay sets how long organizations stay REQUESTED/CREATING and export jobs RUNNING; zero disables.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()
	b.lifecycleDelay = d
}

func validateAlias(alias string) error {
	switch {
	case alias == "":
		return fmt.Errorf("%w: Alias is required", ErrValidation)
	case len(alias) > maxAliasLen:
		return fmt.Errorf("%w: Alias must be at most %d characters", ErrValidation, maxAliasLen)
	case strings.HasPrefix(alias, "d-"), !aliasPattern.MatchString(alias):
		return fmt.Errorf(
			"%w: Alias %q must contain only letters, digits and single hyphens and must not start with d-",
			ErrValidation, alias,
		)
	}

	return nil
}

func validateDomainName(name string) error {
	if len(name) < minDomainLen || len(name) > maxDomainLen || !domainPattern.MatchString(name) {
		return fmt.Errorf("%w: DomainName %q is not a valid domain name", ErrValidation, name)
	}

	return nil
}

func encodePageToken(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(pageTokenPrefix + strconv.Itoa(offset)))
}

func decodePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}

	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || !strings.HasPrefix(string(raw), pageTokenPrefix) {
		return 0, fmt.Errorf("%w: invalid NextToken", ErrValidation)
	}

	offset, err := strconv.Atoi(strings.TrimPrefix(string(raw), pageTokenPrefix))
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("%w: invalid NextToken", ErrValidation)
	}

	return offset, nil
}

// validatePageParams rejects out-of-range MaxResults and malformed NextToken before dispatch.
func validatePageParams(body []byte) error {
	var req struct {
		MaxResults *int64  `json:"MaxResults"`
		NextToken  *string `json:"NextToken"`
	}

	_ = json.Unmarshal(body, &req)

	if req.MaxResults != nil && (*req.MaxResults < minPageSize || *req.MaxResults > defaultPageSize) {
		return fmt.Errorf("%w: MaxResults must be between %d and %d", ErrValidation, minPageSize, defaultPageSize)
	}

	if req.NextToken != nil {
		if _, err := decodePageToken(*req.NextToken); err != nil {
			return err
		}
	}

	return nil
}

// orgView returns a copy of org with the state implied by the lifecycle delay. Caller holds b.mu.
func (b *InMemoryBackend) orgView(org *Organization) *Organization {
	if b.lifecycleDelay <= 0 || org.State != stateActive {
		return org
	}

	elapsed := time.Since(org.CreatedAt)
	if elapsed >= b.lifecycleDelay {
		return org
	}

	cp := *org
	cp.CompletedDate = time.Time{}
	cp.State = orgStateCreating

	if elapsed < b.lifecycleDelay/2 {
		cp.State = orgStateRequested
	}

	return &cp
}

// jobView returns a copy of job with progress and state implied by the lifecycle delay. Caller holds b.mu.
func (b *InMemoryBackend) jobView(job *MailboxExportJob) *MailboxExportJob {
	if b.lifecycleDelay <= 0 || job.State != jobStateRunning {
		return job
	}

	cp := *job
	elapsed := time.Since(job.StartTime)

	if elapsed >= b.lifecycleDelay {
		cp.State = jobStateCompleted
		cp.EndTime = job.StartTime.Add(b.lifecycleDelay)
		cp.EstimatedProgress = jobProgressDone

		return &cp
	}

	cp.EstimatedProgress = int32(elapsed * jobProgressDone / b.lifecycleDelay) //nolint:gosec // bounded to 0-99

	return &cp
}

func (b *InMemoryBackend) requireActiveOrg(orgID string) error {
	b.mu.RLock("requireActiveOrg")
	defer b.mu.RUnlock()

	org, ok := b.organizations.Get(orgID)
	if !ok {
		return nil
	}

	if state := b.orgView(org).State; state != stateActive {
		return fmt.Errorf("%w: organization %q is in state %s", ErrOrganizationState, orgID, state)
	}

	return nil
}

func validateEntityName(kind, name string, maxLen int) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: Name is required", ErrValidation)
	case len(name) > maxLen:
		return fmt.Errorf("%w: %s name must be at most %d characters", ErrValidation, kind, maxLen)
	case !namePattern.MatchString(name):
		return fmt.Errorf("%w: %s name %q contains invalid characters", ErrValidation, kind, name)
	}

	return nil
}

func organizationIDOf(body []byte) string {
	var req struct {
		OrganizationID string `json:"OrganizationId"`
	}

	_ = json.Unmarshal(body, &req)

	return req.OrganizationID
}
