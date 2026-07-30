package inspector2

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// cisScanTargetIDHexLen is the hex-character length of the stub EC2 instance
// ID synthesized as a CIS scan target.
const cisScanTargetIDHexLen = 17

// CIS scan and check status values (AWS Inspector2 CIS API).
const (
	cisScanStatusCompleted = "COMPLETED"

	cisCheckStatusPassed = "PASSED"
	cisCheckStatusFailed = "FAILED"

	cisLevel1        = "LEVEL_1"
	cisPlatform      = "AMAZON_LINUX_2"
	cisReportSuccess = "SUCCEEDED"

	keyScanArn  = "scanArn"
	keyPlatform = "platform"
)

// cisScanNameMinLen/cisScanNameMaxLen enforce the real, documented length
// constraint shared by CreateCisScanConfigurationInput.scanName and
// UpdateCisScanConfigurationInput.scanName (confirmed via the AWS API
// Reference -- the Go SDK module's doc comments carry no length prose for
// this field, unlike CreateFilterInput.name): "Minimum length of 1. Maximum
// length of 128." No charset pattern is documented for this field (unlike
// the CodeSecurity name fields), so only length is enforced.
const (
	cisScanNameMinLen = 1
	cisScanNameMaxLen = 128
)

// validateCisScanName enforces the real scanName length constraint shared by
// CreateCisScanConfiguration and UpdateCisScanConfiguration: 1-128
// characters. Real AWS returns ValidationException for violations; this
// backend previously accepted any non-empty string on create and any string
// at all (including one exceeding 128 chars) on update.
func validateCisScanName(name string) error {
	if len(name) < cisScanNameMinLen || len(name) > cisScanNameMaxLen {
		return fmt.Errorf(
			"%w: scanName must be between %d and %d characters, got %d",
			ErrValidation, cisScanNameMinLen, cisScanNameMaxLen, len(name),
		)
	}

	return nil
}

func (b *InMemoryBackend) buildCisScanConfigARN() string {
	return arn.Build(inspector2Service, b.region, b.accountID, "cis-scan-configuration/"+uuid.New().String())
}

func (b *InMemoryBackend) buildCisScanARN() string {
	return arn.Build(inspector2Service, b.region, b.accountID, "cis-scan/"+uuid.New().String())
}

// CreateCisScanConfiguration creates a new CIS scan configuration.
func (b *InMemoryBackend) CreateCisScanConfiguration(
	name string,
	schedule map[string]any,
	targets map[string]any,
	tags map[string]string,
) (*CisScanConfiguration, error) {
	b.mu.Lock("CreateCisScanConfiguration")
	defer b.mu.Unlock()

	if err := validateCisScanName(name); err != nil {
		return nil, err
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	cfgARN := b.buildCisScanConfigARN()
	cfg := &CisScanConfiguration{
		Arn:        cfgARN,
		Name:       name,
		OwnedBy:    b.accountID,
		Tags:       tags,
		ScheduleV2: schedule,
		Targets:    targets,
	}
	b.cisScanConfigs.Put(cfg)

	// Materialize a completed scan run for the config so the result/report/
	// aggregation operations reflect real configuration state instead of canned
	// data. Real AWS runs scans asynchronously on the configured schedule; we
	// model the steady-state outcome at creation time.
	scan := b.buildCisScanForConfig(cfg)
	b.cisScans.Put(scan)

	return cfg, nil
}

// cisCheckCatalog is a small representative slice of the CIS benchmark checks
// that a scan evaluates per target. Results are derived deterministically from
// these so report/detail/aggregation operations stay internally consistent.
func cisCheckCatalog() []CisCheckResult {
	return []CisCheckResult{
		{
			CheckID:    "1.1.1",
			CheckDescr: "Ensure mounting of cramfs filesystems is disabled",
			Level:      cisLevel1,
			Platform:   cisPlatform,
			Status:     cisCheckStatusPassed,
		},
		{
			CheckID:    "1.3.1",
			CheckDescr: "Ensure AIDE is installed",
			Level:      cisLevel1,
			Platform:   cisPlatform,
			Status:     cisCheckStatusFailed,
		},
		{
			CheckID:    "5.2.1",
			CheckDescr: "Ensure permissions on /etc/ssh/sshd_config are configured",
			Level:      cisLevel1,
			Platform:   cisPlatform,
			Status:     cisCheckStatusPassed,
		},
	}
}

// cisTargetAccounts extracts the account IDs a scan targets, defaulting to the
// backend account when the configuration does not name any.
func (b *InMemoryBackend) cisTargetAccounts(targets map[string]any) []string {
	var accounts []string

	if raw, ok := targets["accountIds"].([]any); ok {
		for _, v := range raw {
			if id, isStr := v.(string); isStr && id != "" {
				accounts = append(accounts, id)
			}
		}
	}

	if len(accounts) == 0 {
		accounts = []string{b.accountID}
	}

	return accounts
}

// buildCisScanForConfig constructs a completed scan, including per-target check
// results, from a scan configuration. Caller must hold the write lock.
func (b *InMemoryBackend) buildCisScanForConfig(cfg *CisScanConfiguration) *CisScan {
	now := time.Now().UTC()
	accounts := b.cisTargetAccounts(cfg.Targets)
	catalog := cisCheckCatalog()

	results := make([]*CisCheckResult, 0, len(accounts)*len(catalog))
	failed := 0

	for _, acct := range accounts {
		targetID := "i-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:cisScanTargetIDHexLen]

		for i := range catalog {
			res := catalog[i]
			res.AccountID = acct
			res.TargetID = targetID

			if res.Status == cisCheckStatusFailed {
				failed++
				res.StatusReason = "remediation required"
			}

			rc := res
			results = append(results, &rc)
		}
	}

	return &CisScan{
		ScanArn:              b.buildCisScanARN(),
		ScanConfigurationArn: cfg.Arn,
		ScanName:             cfg.Name,
		Status:               cisScanStatusCompleted,
		SecurityLevel:        cisLevel1,
		ScheduledAt:          now,
		FinishedAt:           now,
		TargetAccountID:      accounts[0],
		TotalChecks:          len(results),
		FailedChecks:         failed,
		Results:              results,
	}
}

// DeleteCisScanConfiguration deletes a CIS scan configuration.
func (b *InMemoryBackend) DeleteCisScanConfiguration(configARN string) error {
	b.mu.Lock("DeleteCisScanConfiguration")
	defer b.mu.Unlock()

	if !b.cisScanConfigs.Delete(configARN) {
		return ErrCisScanConfigNotFound
	}

	// Drop any scans materialized from this configuration so list/result
	// operations stop reporting them. slices.Clone is required here: the
	// index's returned slice mutates in place as each Delete below removes an
	// entry from it.
	for _, s := range slices.Clone(b.cisScansByConfig.Get(configARN)) {
		b.cisScans.Delete(s.ScanArn)
	}

	return nil
}

// UpdateCisScanConfiguration updates a CIS scan configuration.
func (b *InMemoryBackend) UpdateCisScanConfiguration(
	configARN string,
	name string,
	schedule map[string]any,
	targets map[string]any,
) (*CisScanConfiguration, error) {
	b.mu.Lock("UpdateCisScanConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.cisScanConfigs.Get(configARN)
	if !ok {
		return nil, ErrCisScanConfigNotFound
	}

	if name != "" {
		if err := validateCisScanName(name); err != nil {
			return nil, err
		}

		cfg.Name = name
	}

	if schedule != nil {
		cfg.ScheduleV2 = schedule
	}

	if targets != nil {
		cfg.Targets = targets
	}

	return cfg, nil
}

// ListCisScanConfigurations returns CIS scan configurations.
func (b *InMemoryBackend) ListCisScanConfigurations() ([]*CisScanConfiguration, error) {
	b.mu.RLock("ListCisScanConfigurations")
	defer b.mu.RUnlock()

	result := make([]*CisScanConfiguration, 0, b.cisScanConfigs.Len())

	for _, cfg := range b.cisScanConfigs.Snapshot() {
		cp := *cfg
		result = append(result, &cp)
	}

	return result, nil
}

// StartCisSession starts a new CIS scan session.
func (b *InMemoryBackend) StartCisSession(scanJobID, sessionToken string) (*CisSession, error) {
	b.mu.Lock("StartCisSession")
	defer b.mu.Unlock()

	if scanJobID == "" {
		return nil, fmt.Errorf("%w: scanJobId is required", ErrValidation)
	}

	sess := &CisSession{
		ScanJobID:    scanJobID,
		SessionToken: sessionToken,
		Status:       statusActive,
		StartedAt:    time.Now().UTC(),
	}
	b.cisSessions.Put(sess)

	return sess, nil
}

// StopCisSession stops a CIS scan session.
func (b *InMemoryBackend) StopCisSession(scanJobID string) error {
	b.mu.Lock("StopCisSession")
	defer b.mu.Unlock()

	sess, ok := b.cisSessions.Get(scanJobID)
	if !ok {
		return ErrCisSessionNotFound
	}

	sess.Status = "STOPPING"

	return nil
}

// SendCisSessionHealth acknowledges CIS session health.
func (b *InMemoryBackend) SendCisSessionHealth(_ string) error {
	return nil
}

// SendCisSessionTelemetry records CIS session telemetry (no-op in memory).
func (b *InMemoryBackend) SendCisSessionTelemetry(_ string, _ map[string]any) error {
	return nil
}

// findCisScan returns the stored scan for either its scan ARN or the ARN of the
// configuration that produced it. Caller must hold at least an RLock.
func (b *InMemoryBackend) findCisScan(scanOrConfigARN string) *CisScan {
	if s, ok := b.cisScans.Get(scanOrConfigARN); ok {
		return s
	}

	if matches := b.cisScansByConfig.Get(scanOrConfigARN); len(matches) > 0 {
		return matches[0]
	}

	return nil
}

// GetCisScanReport returns the report for a completed CIS scan. The status and
// counts are drawn from the stored scan produced by its configuration. For an
// unrecognized scan ARN it returns a benign SUCCEEDED report with no findings,
// matching AWS, which does not error on missing report targets.
func (b *InMemoryBackend) GetCisScanReport(scanArn string) (map[string]any, error) {
	b.mu.RLock("GetCisScanReport")
	defer b.mu.RUnlock()

	scan := b.findCisScan(scanArn)
	if scan == nil {
		return map[string]any{
			keyStatus: cisReportSuccess,
			"url":     "",
		}, nil
	}

	return map[string]any{
		keyStatus:      cisReportSuccess,
		"url":          "",
		keyScanArn:     scan.ScanArn,
		"totalChecks":  scan.TotalChecks,
		"failedChecks": scan.FailedChecks,
	}, nil
}

// GetCisScanResultDetails returns the per-check results for a CIS scan. Results
// reflect the scan generated for the configuration; an unknown scan ARN yields
// an empty result set rather than an error (AWS behavior for absent scans).
func (b *InMemoryBackend) GetCisScanResultDetails(scanArn string) (map[string]any, error) {
	b.mu.RLock("GetCisScanResultDetails")
	defer b.mu.RUnlock()

	scan := b.findCisScan(scanArn)
	if scan == nil {
		return map[string]any{"checkResults": []any{}}, nil
	}

	checkResults := make([]map[string]any, 0, len(scan.Results))
	for _, r := range scan.Results {
		entry := map[string]any{
			keyScanArn:         scan.ScanArn,
			"checkId":          r.CheckID,
			"checkDescription": r.CheckDescr,
			keyLevel:           r.Level,
			keyPlatform:        r.Platform,
			keyStatus:          r.Status,
			keyAccountID:       r.AccountID,
			"targetResourceId": r.TargetID,
		}
		if r.StatusReason != "" {
			entry["statusReason"] = r.StatusReason
		}

		checkResults = append(checkResults, entry)
	}

	return map[string]any{"checkResults": checkResults}, nil
}

// ListCisScans returns all completed CIS scans, sorted by scan ARN for stable
// pagination-free ordering. Each entry summarizes the scan produced from a
// configuration.
func (b *InMemoryBackend) ListCisScans() ([]map[string]any, error) {
	b.mu.RLock("ListCisScans")
	defer b.mu.RUnlock()

	result := make([]map[string]any, 0, b.cisScans.Len())

	for _, s := range b.cisScans.Snapshot() {
		result = append(result, map[string]any{
			keyScanArn:              s.ScanArn,
			keyScanConfigurationArn: s.ScanConfigurationArn,
			"scanName":              s.ScanName,
			keyStatus:               s.Status,
			"securityLevel":         s.SecurityLevel,
			// scanDate is the real CisScan wire field (a DateTimeTimestamp,
			// epoch-seconds encoded); "scheduledBy" is a distinct *string*
			// field (the account/org that scheduled the scan) this backend
			// does not track, so it is not emitted rather than filled with
			// a fabricated value.
			"scanDate":        awstime.Epoch(s.ScheduledAt),
			"failedChecks":    s.FailedChecks,
			"totalChecks":     s.TotalChecks,
			"targetAccountId": s.TargetAccountID,
		})
	}

	return result, nil
}

// ListCisScanResultsAggregatedByChecks groups a scan's results by check ID,
// reporting passed/failed/skipped counts per check. An unknown scan ARN yields
// an empty aggregation list.
func (b *InMemoryBackend) ListCisScanResultsAggregatedByChecks(scanArn string) ([]map[string]any, error) {
	b.mu.RLock("ListCisScanResultsAggregatedByChecks")
	defer b.mu.RUnlock()

	scan := b.findCisScan(scanArn)
	if scan == nil {
		return []map[string]any{}, nil
	}

	type checkAgg struct {
		descr        string
		level        string
		platform     string
		passed       int64
		failed       int64
		skipped      int64
		firstSeenIdx int
	}

	aggs := make(map[string]*checkAgg)
	order := make([]string, 0)

	for i, r := range scan.Results {
		a, ok := aggs[r.CheckID]
		if !ok {
			a = &checkAgg{descr: r.CheckDescr, level: r.Level, platform: r.Platform, firstSeenIdx: i}
			aggs[r.CheckID] = a
			order = append(order, r.CheckID)
		}

		switch r.Status {
		case cisCheckStatusPassed:
			a.passed++
		case cisCheckStatusFailed:
			a.failed++
		default:
			a.skipped++
		}
	}

	sort.Strings(order)

	result := make([]map[string]any, 0, len(order))
	for _, id := range order {
		a := aggs[id]
		result = append(result, map[string]any{
			keyScanArn:         scan.ScanArn,
			"checkId":          id,
			"checkDescription": a.descr,
			keyLevel:           a.level,
			keyPlatform:        a.platform,
			"statusCounts": map[string]any{
				"passed":  a.passed,
				"failed":  a.failed,
				"skipped": a.skipped,
			},
		})
	}

	return result, nil
}

// ListCisScanResultsAggregatedByTargetResource groups a scan's results by target
// resource, reporting passed/failed/skipped counts per resource. An unknown scan
// ARN yields an empty aggregation list.
func (b *InMemoryBackend) ListCisScanResultsAggregatedByTargetResource(
	scanArn string,
) ([]map[string]any, error) {
	b.mu.RLock("ListCisScanResultsAggregatedByTargetResource")
	defer b.mu.RUnlock()

	scan := b.findCisScan(scanArn)
	if scan == nil {
		return []map[string]any{}, nil
	}

	type targetAgg struct {
		accountID string
		platform  string
		passed    int64
		failed    int64
		skipped   int64
	}

	aggs := make(map[string]*targetAgg)
	order := make([]string, 0)

	for _, r := range scan.Results {
		a, ok := aggs[r.TargetID]
		if !ok {
			a = &targetAgg{accountID: r.AccountID, platform: r.Platform}
			aggs[r.TargetID] = a
			order = append(order, r.TargetID)
		}

		switch r.Status {
		case cisCheckStatusPassed:
			a.passed++
		case cisCheckStatusFailed:
			a.failed++
		default:
			a.skipped++
		}
	}

	sort.Strings(order)

	result := make([]map[string]any, 0, len(order))
	for _, tid := range order {
		a := aggs[tid]
		result = append(result, map[string]any{
			keyScanArn:         scan.ScanArn,
			"targetResourceId": tid,
			keyAccountID:       a.accountID,
			keyPlatform:        a.platform,
			"statusCounts": map[string]any{
				"passed":  a.passed,
				"failed":  a.failed,
				"skipped": a.skipped,
			},
		})
	}

	return result, nil
}
