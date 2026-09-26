package secretsmanager

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// rotationSchedulerInterval controls the background schedule evaluation cadence.
	rotationSchedulerInterval = time.Second
	// hoursPerDay is the number of hours in a day, used for day-granularity truncation.
	hoursPerDay = 24
	// rateUnitHour/rateUnitDay (+ plurals) are rate() expression unit words, deduplicated
	// across rotationInterval/scheduleWindowLimit/rotationRateUnit.
	rateUnitHour  = "hour"
	rateUnitHours = "hours"
	rateUnitDay   = "day"
	rateUnitDays  = "days"
)

// pendingRotation describes a Lambda-backed rotation awaiting its step invocations.
type pendingRotation struct {
	region    string
	secretID  string
	versionID string
	lambdaARN string
}

// computeNextRotationDate returns the secret's next rotation timestamp, or nil if rotation
// isn't configured. Reports the window start, since gopherstack fires there (rotate-secrets_schedule.html).
func computeNextRotationDate(secret *Secret) *float64 {
	if !secret.RotationEnabled || secret.RotationRules == nil {
		return nil
	}

	base := secret.LastRotatedDate
	if base == nil {
		base = secret.LastChangedDate
	}

	if base == nil {
		return nil
	}

	baseTime := time.Unix(0, int64(*base*float64(time.Second)))

	next, ok := nextRotationOccurrence(secret.RotationRules, baseTime)
	if !ok {
		return nil
	}

	nextFloat := UnixTimeFloat(next)

	return &nextFloat
}

// rotationRateUnit reports a rate() ScheduleExpression's unit ("hour" or "day"), or "" for
// AutomaticallyAfterDays, cron(), or this repo's test-only second/minute rate units.
func rotationRateUnit(rules *RotationRulesType) string {
	const rateExpressionParts = 2

	if rules == nil || rules.AutomaticallyAfterDays != nil {
		return ""
	}

	expr := strings.TrimSpace(rules.ScheduleExpression)
	if !strings.HasPrefix(expr, "rate(") || !strings.HasSuffix(expr, ")") {
		return ""
	}

	parts := strings.Fields(strings.TrimSuffix(strings.TrimPrefix(expr, "rate("), ")"))
	if len(parts) != rateExpressionParts {
		return ""
	}

	switch parts[1] {
	case rateUnitHour, rateUnitHours:
		return rateUnitHour
	case rateUnitDay, rateUnitDays:
		return rateUnitDay
	default:
		return ""
	}
}

// nextRotationOccurrence returns the next rotation window start after base, per
// rotate-secrets_schedule.html (rate(days) aligns to midnight UTC; cron() is pre-aligned).
func nextRotationOccurrence(rules *RotationRulesType, base time.Time) (time.Time, bool) {
	if isCronExpression(rules.ScheduleExpression) {
		return nextCronTime(rules.ScheduleExpression, base)
	}

	interval, ok := rotationInterval(rules)
	if !ok {
		return time.Time{}, false
	}

	candidate := base.Add(interval)

	// AWS schedules evaluate in UTC, but base may carry a local Location (e.g. time.Unix).
	// Date/hour components are read from candidate.UTC() to keep the alignment correct.
	switch u := candidate.UTC(); rotationRateUnit(rules) {
	case rateUnitDay:
		return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC), true
	case rateUnitHour:
		return time.Date(u.Year(), u.Month(), u.Day(), u.Hour(), 0, 0, 0, time.UTC), true
	default:
		return candidate, true
	}
}

// RotateSecret creates a new version of the secret (rotation stub).
func (b *InMemoryBackend) RotateSecret(
	ctx context.Context,
	input *RotateSecretInput,
) (*RotateSecretOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RotateSecret")
	defer b.mu.Unlock()

	id := resolveSecretID(input.SecretID)

	secret, lookupErr := b.resolvePrimaryOnlySecretLocked(region, input.SecretID, id)
	if lookupErr != nil {
		return nil, lookupErr
	}

	// Real AWS requires a rotation strategy -- a Lambda ARN, either already
	// stored on the secret or supplied on this request -- before it will
	// enable/perform rotation; see ErrRotationStrategyRequired's doc comment.
	// Checked before any mutation below so a rejected call leaves the secret
	// untouched.
	effectiveLambdaARN := secret.RotationLambdaARN
	if input.RotationLambdaARN != "" {
		effectiveLambdaARN = input.RotationLambdaARN
	}
	if effectiveLambdaARN == "" {
		return nil, ErrRotationStrategyRequired
	}

	if input.RotationLambdaARN != "" {
		secret.RotationLambdaARN = input.RotationLambdaARN
	}

	if input.ExternalSecretRotationRoleArn != "" {
		secret.ExternalSecretRotationRoleArn = input.ExternalSecretRotationRoleArn
	}

	if input.ExternalSecretRotationMetadata != nil {
		secret.ExternalSecretRotationMetadata = cloneExternalSecretRotationMetadata(
			input.ExternalSecretRotationMetadata,
		)
	}

	if input.RotationRules != nil {
		if err := validateRotationRules(input.RotationRules); err != nil {
			return nil, err
		}

		secret.RotationRules = cloneRotationRules(input.RotationRules)
		secret.RotationEnabled = true
		b.ensureRotationScheduler()
	}

	rotateImmediately := true
	if input.RotateImmediately != nil {
		rotateImmediately = *input.RotateImmediately
	}

	if !rotateImmediately {
		return &RotateSecretOutput{
			ARN:  secret.ARN,
			Name: secret.Name,
		}, nil
	}

	versionID, err := b.rotateSecretLocked(ctx, secret, input.ClientRequestToken)
	if err != nil {
		return nil, err
	}

	// Promote immediately when no Lambda invoker is wired (stub/direct-backend usage).
	// When a Lambda ARN is set AND a Lambda invoker is configured, the handler or
	// scheduler will call FinishRotation after invoking the four rotation steps.
	// Checked against secret.RotationLambdaARN (set above from input, or already
	// stored from a prior EnableRotation/RotateSecret call), not input.RotationLambdaARN
	// directly -- callers normally configure the ARN once and omit it on every
	// subsequent RotateSecret call.
	if secret.RotationLambdaARN == "" || b.lambdaInvoker == nil {
		b.finishRotationLocked(region, secret, versionID)
	}

	return &RotateSecretOutput{
		ARN:       secret.ARN,
		Name:      secret.Name,
		VersionID: versionID,
	}, nil
}

// rotateSecretLocked creates a new secret version with the AWSPENDING staging label.
// Callers MUST follow up with finishRotationLocked (to promote to AWSCURRENT) or
// abortRotationLocked (to discard the pending version). Must be called with b.mu held.
func (b *InMemoryBackend) rotateSecretLocked(
	ctx context.Context,
	secret *Secret,
	token string,
) (string, error) {
	currentVer := b.findVersion(secret, "", StagingLabelCurrent)
	if currentVer == nil {
		return "", ErrVersionNotFound
	}

	versionID := token
	if versionID == "" {
		versionID = generateVersionID()
	}

	// When a rotation Lambda ARN is configured, generate a fresh secret value
	// (sealed via KMS when an encryptor is wired). The Lambda lifecycle
	// (createSecret/setSecret/testSecret/finishSecret) will validate and
	// promote the value.
	if secret.RotationLambdaARN != "" {
		newVer, err := b.sealVersion(
			ctx,
			secret,
			versionID,
			uuid.New().String(),
			nil,
			[]string{"AWSPENDING"},
			UnixTimeFloat(b.now()),
		)
		if err != nil {
			return "", err
		}

		secret.Versions[versionID] = newVer

		return versionID, nil
	}

	// Without a Lambda ARN, preserve the existing value: carry the current
	// version's (possibly ciphertext) fields forward unchanged rather than
	// decrypting and re-encrypting it.
	newVer := &SecretVersion{
		VersionID:     versionID,
		SecretString:  currentVer.SecretString,
		SecretBinary:  currentVer.SecretBinary,
		Ciphertext:    currentVer.Ciphertext,
		WasString:     currentVer.WasString,
		StagingLabels: []string{"AWSPENDING"},
		CreatedDate:   UnixTimeFloat(b.now()),
	}
	secret.Versions[versionID] = newVer

	return versionID, nil
}

// finishRotationLocked promotes the AWSPENDING version identified by versionID to
// AWSCURRENT, moving the old AWSCURRENT to AWSPREVIOUS. Must be called with b.mu held.
func (b *InMemoryBackend) finishRotationLocked(region string, secret *Secret, versionID string) {
	newVer, ok := secret.Versions[versionID]
	if !ok {
		return
	}

	b.rotateStagingLabels(secret) // AWSCURRENT → AWSPREVIOUS, drops old AWSPREVIOUS
	newVer.StagingLabels = []string{StagingLabelCurrent}
	secret.CurrentVersionID = versionID
	secret.RotationEnabled = true
	now := UnixTimeFloat(b.now())
	secret.LastChangedDate = &now
	secret.LastRotatedDate = &now
	pruneVersions(secret)
	b.syncReplicationStatusLocked(region, secret)
}

// abortRotationLocked removes the AWSPENDING version, cancelling an in-progress rotation.
// Must be called with b.mu held.
func (b *InMemoryBackend) abortRotationLocked(secret *Secret, versionID string) {
	delete(secret.Versions, versionID)
}

// BeginRotationTestProbe creates a transient AWSPENDING version for a rotation
// test probe. Real AWS Secrets Manager runs this when RotateSecret is called
// with RotateImmediately=false: it validates the rotation configuration by
// invoking only the Lambda's testSecret step against a temporary AWSPENDING
// version, then removes that version without promoting it to AWSCURRENT.
// Callers MUST follow up with AbortRotation (success or failure) to remove
// the transient version.
func (b *InMemoryBackend) BeginRotationTestProbe(
	ctx context.Context,
	secretID string,
) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("BeginRotationTestProbe")
	defer b.mu.Unlock()

	id := resolveSecretID(secretID)

	secret, ok := b.secretGet(region, id)
	if !ok {
		return "", ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return "", ErrSecretDeleted
	}

	return b.rotateSecretLocked(ctx, secret, "")
}

// FinishRotation promotes the AWSPENDING version to AWSCURRENT. Called by the
// handler after all Lambda rotation steps succeed.
func (b *InMemoryBackend) FinishRotation(ctx context.Context, secretID, versionID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("FinishRotation")
	defer b.mu.Unlock()

	id := resolveSecretID(secretID)
	secret, ok := b.secretGet(region, id)

	if !ok || secret.DeletedDate != nil {
		return ErrSecretNotFound
	}

	b.finishRotationLocked(region, secret, versionID)

	return nil
}

// AbortRotation removes the AWSPENDING version, aborting an in-progress rotation.
// Called by the handler when a Lambda rotation step fails.
func (b *InMemoryBackend) AbortRotation(ctx context.Context, secretID, versionID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("AbortRotation")
	defer b.mu.Unlock()

	id := resolveSecretID(secretID)
	secret, ok := b.secretGet(region, id)

	if !ok || secret.DeletedDate != nil {
		return ErrSecretNotFound
	}

	b.abortRotationLocked(secret, versionID)

	return nil
}

// runLambdaRotationSteps invokes the four Lambda rotation steps (createSecret,
// setSecret, testSecret, finishSecret) for the given secret and version token.
func (b *InMemoryBackend) runLambdaRotationSteps(
	ctx context.Context,
	lambdaARN, secretID, token string,
) error {
	fnName := extractFunctionNameFromARN(lambdaARN)

	for _, step := range rotationSteps {
		event, marshalErr := buildRotationStepEvent(secretID, token, step)
		if marshalErr != nil {
			return fmt.Errorf("rotation event marshal: %w", marshalErr)
		}

		if _, _, err := b.lambdaInvoker.InvokeFunction(ctx, fnName, "RequestResponse", event); err != nil {
			return fmt.Errorf("rotation Lambda step %q failed: %w", step, err)
		}
	}

	return nil
}

func cloneExternalSecretRotationMetadata(
	items []ExternalSecretRotationMetadataItem,
) []ExternalSecretRotationMetadataItem {
	if items == nil {
		return nil
	}

	return append([]ExternalSecretRotationMetadataItem(nil), items...)
}

func cloneRotationRules(rules *RotationRulesType) *RotationRulesType {
	if rules == nil {
		return nil
	}

	cloned := *rules
	if rules.AutomaticallyAfterDays != nil {
		days := *rules.AutomaticallyAfterDays
		cloned.AutomaticallyAfterDays = &days
	}

	return &cloned
}

// validateRotationRules checks that rotation rule values are within AWS-allowed bounds.
func validateRotationRules(rules *RotationRulesType) error {
	const (
		minRotationDays = 1
		maxRotationDays = 365
	)

	if rules.AutomaticallyAfterDays != nil {
		days := *rules.AutomaticallyAfterDays
		if days < minRotationDays || days > maxRotationDays {
			return fmt.Errorf(
				"%w: AutomaticallyAfterDays must be between %d and %d",
				ErrInvalidParameter,
				minRotationDays,
				maxRotationDays,
			)
		}

		// AutomaticallyAfterDays and ScheduleExpression are mutually exclusive
		// (API_RotationRulesType.html).
		if rules.ScheduleExpression != "" {
			return fmt.Errorf(
				"%w: RotationRules must set AutomaticallyAfterDays or ScheduleExpression, not both",
				ErrInvalidParameter,
			)
		}
	}

	if rules.Duration == "" {
		return nil
	}

	durationHours, err := parseRotationDuration(rules.Duration)
	if err != nil {
		return err
	}

	// A Duration must not extend into the next rotation window or the next UTC day
	// (API_RotationRulesType.html).
	if limit, ok := scheduleWindowLimit(rules.ScheduleExpression); ok && durationHours > limit {
		return fmt.Errorf(
			"%w: Duration %s must not extend into the next rotation window or the next UTC day",
			ErrInvalidParameter, rules.Duration,
		)
	}

	return nil
}

// parseRotationDuration validates and parses a RotationRules.Duration string. Per
// API_RotationRulesType.html: pattern "[0-9]+h", length 2-3 (i.e. 1-2 digit hours).
func parseRotationDuration(s string) (int, error) {
	const minLen, maxLen = 2, 3

	invalid := func() error {
		return fmt.Errorf(
			"%w: Duration %q must match pattern [0-9]+h with length %d-%d (e.g. \"3h\")",
			ErrInvalidParameter, s, minLen, maxLen,
		)
	}

	if len(s) < minLen || len(s) > maxLen || !strings.HasSuffix(s, "h") {
		return 0, invalid()
	}

	digits := strings.TrimSuffix(s, "h")
	for _, r := range digits {
		if r < '0' || r > '9' {
			return 0, invalid()
		}
	}

	hours, err := strconv.Atoi(digits)
	if err != nil || hours <= 0 {
		return 0, invalid()
	}

	return hours, nil
}

// scheduleWindowLimit returns the max valid Duration, in hours, for a ScheduleExpression
// (rotate-secrets_schedule.html). ok is false for schedules with no real-AWS window.
func scheduleWindowLimit(expr string) (int, bool) {
	const rateExpressionParts = 2

	expr = strings.TrimSpace(expr)

	switch {
	case isCronExpression(expr):
		cf, err := parseCronExpr(expr)
		if err != nil {
			return 0, false
		}

		if len(cf.hours) > 1 {
			return cronHourlyWindowLimit(cf.hours), true
		}

		return hoursPerDay - cf.hours[0], true

	case strings.HasPrefix(expr, "rate(") && strings.HasSuffix(expr, ")"):
		parts := strings.Fields(strings.TrimSuffix(strings.TrimPrefix(expr, "rate("), ")"))
		if len(parts) != rateExpressionParts {
			return 0, false
		}

		n, err := strconv.Atoi(parts[0])
		if err != nil || n <= 0 {
			return 0, false
		}

		switch parts[1] {
		case rateUnitHour, rateUnitHours:
			return n, true
		case rateUnitDay, rateUnitDays:
			return hoursPerDay, true
		default:
			return 0, false
		}

	default:
		return 0, false
	}
}

// cronHourlyWindowLimit returns the smallest gap, in hours and wrapping past midnight, between
// an hours-based cron's Hours values -- the max Duration allowed (rotate-secrets_schedule.html).
func cronHourlyWindowLimit(hours []int) int {
	minGap := hoursPerDay
	for i := 1; i < len(hours); i++ {
		if gap := hours[i] - hours[i-1]; gap < minGap {
			minGap = gap
		}
	}

	if wrapGap := hoursPerDay - hours[len(hours)-1] + hours[0]; wrapGap < minGap {
		minGap = wrapGap
	}

	return minGap
}

// CancelRotateSecret cancels an in-progress rotation by removing the AWSPENDING staging label.
func (b *InMemoryBackend) CancelRotateSecret(
	ctx context.Context, input *CancelRotateSecretInput,
) (*CancelRotateSecretOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CancelRotateSecret")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, ok := b.secretGet(region, name)
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	const pendingLabel = "AWSPENDING"

	var canceledVersionID string

	for _, ver := range secret.Versions {
		newLabels := make([]string, 0, len(ver.StagingLabels))

		for _, lbl := range ver.StagingLabels {
			if lbl == pendingLabel {
				canceledVersionID = ver.VersionID

				continue
			}

			newLabels = append(newLabels, lbl)
		}

		ver.StagingLabels = newLabels
	}

	// "Turns off automatic rotation" (api_op_CancelRotateSecret.go doc
	// comment, secretsmanager@v1.48.0) -- DescribeSecret's RotationEnabled
	// must flip to false, not just the in-progress AWSPENDING version drop.
	secret.RotationEnabled = false

	return &CancelRotateSecretOutput{
		ARN:       secret.ARN,
		Name:      secret.Name,
		VersionID: canceledVersionID,
	}, nil
}

func (b *InMemoryBackend) ensureRotationScheduler() {
	b.schedulerOnce.Do(func() {
		b.schedulerWG.Go(b.rotationSchedulerLoop)
	})
}

func (b *InMemoryBackend) rotationSchedulerLoop() {
	ticker := time.NewTicker(rotationSchedulerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.schedulerStop:
			return
		case now := <-ticker.C:
			b.runScheduledRotations(now)
		}
	}
}

// StopRotationScheduler signals the rotation scheduler goroutine to exit. It is
// idempotent and safe to call even if the scheduler was never started: closing
// the stop channel simply has no observer in that case.
func (b *InMemoryBackend) StopRotationScheduler() {
	if b.schedulerStop == nil {
		return
	}

	b.schedulerStopOnce.Do(func() { close(b.schedulerStop) })

	// Join the scheduler goroutine so that, once this returns, the caller
	// knows it has exited. Safe from deadlock: no caller holds b.mu here, and
	// the loop only acquires it inside runScheduledRotations.
	b.schedulerWG.Wait()
}

func (b *InMemoryBackend) runScheduledRotations(now time.Time) {
	// Phase 1: create AWSPENDING versions while holding the lock.
	var pending []pendingRotation

	func() {
		b.mu.Lock("rotationScheduler")
		defer b.mu.Unlock()

		for _, secret := range b.secrets.All() {
			if p, ok := b.scheduleRotationLocked(secret.region, secret.Name, secret, now); ok {
				pending = append(pending, p)
			}
		}
	}()

	// Phase 2: invoke Lambda WITHOUT holding the lock, then promote or abort.
	for _, p := range pending {
		ctx := context.WithValue(b.svcCtx, regionContextKey{}, p.region)
		lambdaErr := b.runLambdaRotationSteps(ctx, p.lambdaARN, p.secretID, p.versionID)
		if lambdaErr != nil {
			_ = b.AbortRotation(ctx, p.secretID, p.versionID)
		} else {
			_ = b.FinishRotation(ctx, p.secretID, p.versionID)
		}
	}
}

// scheduleRotationLocked evaluates a single secret for a due rotation. When rotation is due
// it creates the AWSPENDING version; if no Lambda is configured it promotes immediately and
// returns ok=false, otherwise it returns the pendingRotation to invoke without the lock held.
// Callers must hold b.mu.
func (b *InMemoryBackend) scheduleRotationLocked(
	region, id string, secret *Secret, now time.Time,
) (pendingRotation, bool) {
	if secret.DeletedDate != nil || !secret.RotationEnabled || secret.RotationRules == nil {
		return pendingRotation{}, false
	}

	base := secret.LastRotatedDate
	if base == nil {
		base = secret.LastChangedDate
	}

	if !rotationDue(secret.RotationRules, now, base) {
		return pendingRotation{}, false
	}

	ctx := context.WithValue(b.svcCtx, regionContextKey{}, region)

	versionID, err := b.rotateSecretLocked(ctx, secret, "")
	if err != nil {
		return pendingRotation{}, false
	}

	lambdaARN := secret.RotationLambdaARN
	if b.lambdaInvoker == nil || lambdaARN == "" {
		// No Lambda configured — promote immediately while still locked.
		b.finishRotationLocked(region, secret, versionID)

		return pendingRotation{}, false
	}

	return pendingRotation{
		region:    region,
		secretID:  id,
		versionID: versionID,
		lambdaARN: lambdaARN,
	}, true
}

// rotationDue reports whether a rotation should fire at `now` given the rotation rules and
// the base time (last rotation or last change). Returns false when rules are nil or unparseable.
func rotationDue(rules *RotationRulesType, now time.Time, base *float64) bool {
	if rules == nil || base == nil {
		return false
	}

	baseTime := time.Unix(0, int64(*base*float64(time.Second)))

	if now.Before(baseTime) {
		return false
	}

	next, ok := nextRotationOccurrence(rules, baseTime)
	if !ok {
		return false
	}

	return !now.Before(next)
}

func rotationInterval(rules *RotationRulesType) (time.Duration, bool) {
	const rateExpressionParts = 2

	if rules == nil {
		return 0, false
	}

	if rules.AutomaticallyAfterDays != nil && *rules.AutomaticallyAfterDays > 0 {
		return time.Duration(*rules.AutomaticallyAfterDays) * 24 * time.Hour, true
	}

	expr := strings.TrimSpace(rules.ScheduleExpression)
	if expr == "" {
		return 0, false
	}
	if !strings.HasPrefix(expr, "rate(") || !strings.HasSuffix(expr, ")") {
		return 0, false
	}

	payload := strings.TrimSuffix(strings.TrimPrefix(expr, "rate("), ")")
	parts := strings.Fields(payload)
	if len(parts) != rateExpressionParts {
		return 0, false
	}

	n, err := strconv.Atoi(parts[0])
	if err != nil || n <= 0 {
		return 0, false
	}

	switch parts[1] {
	case "second", "seconds":
		return time.Duration(n) * time.Second, true
	case "minute", "minutes":
		return time.Duration(n) * time.Minute, true
	case rateUnitHour, rateUnitHours:
		return time.Duration(n) * time.Hour, true
	case rateUnitDay, rateUnitDays:
		return time.Duration(n) * 24 * time.Hour, true
	default:
		return 0, false
	}
}
