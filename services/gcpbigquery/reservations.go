package gcpbigquery

import (
	"maps"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// Reservation API (bigqueryreservation.googleapis.com/v1); field rules are those of
// google.golang.org/api/bigqueryreservation/v1 and
// https://docs.cloud.google.com/bigquery/docs/reference/reservations/rest.
const (
	reservationsPrefix = "v1"
	segLocations       = "locations"
	segReservations    = "reservations"
	segCommitments     = "capacityCommitments"
	segAssignments     = "assignments"
	segBIReservation   = "biReservation"
	failedPrecondition = "failedPrecondition"
	maxResourceIDLen   = 64
	slotMultiple       = 100
	flexPlan           = "FLEX"
	noPlan             = "NONE"
	litSlotCapacity    = "slotCapacity"
	scalingUnspecified = "SCALING_MODE_UNSPECIFIED"
	litSize            = "size"
	yearsThree         = 3
	litMaxSlots        = "maxSlots"
	litScalingMode     = "scalingMode"
	litIgnoreIdle      = "ignoreIdleSlots"
	litPlan            = "plan"
	litRenewalPlan     = "renewalPlan"
	litAssignee        = "assignee"
	litJobType         = "jobType"
	litSlotCount       = "slotCount"
	litUpdateTime      = "updateTime"
	litState           = "state"
	litAutoscale       = "autoscale"
	reservationAudit   = "google.cloud.bigquery.reservation.v1.ReservationService."
	wildcardParent     = "-"
)

// reservationState is the persisted state of the Reservation API: resources keyed by their full name.
type reservationState struct {
	Reservations map[string]map[string]any `json:"reservations,omitempty"`
	Commitments  map[string]map[string]any `json:"capacityCommitments,omitempty"`
	Assignments  map[string]map[string]any `json:"assignments,omitempty"`
	BI           map[string]map[string]any `json:"biReservations,omitempty"`
}

func newReservationState() *reservationState {
	return &reservationState{
		Reservations: map[string]map[string]any{}, Commitments: map[string]map[string]any{},
		Assignments: map[string]map[string]any{}, BI: map[string]map[string]any{},
	}
}

func (s *reservationState) normalize() {
	if s.Reservations == nil {
		s.Reservations = map[string]map[string]any{}
	}

	if s.Commitments == nil {
		s.Commitments = map[string]map[string]any{}
	}

	if s.Assignments == nil {
		s.Assignments = map[string]map[string]any{}
	}

	if s.BI == nil {
		s.BI = map[string]map[string]any{}
	}
}

//nolint:gochecknoglobals // compiled once, read-only
var (
	reservationIDPattern = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`^[a-z]([a-z0-9-]*[a-z0-9])?$`)
	})
	commitmentIDPattern = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	})
	assigneePattern = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`^(projects|folders|organizations)/[^/]+$`)
	})
)

func strField(m map[string]any, k string) string {
	v, _ := m[k].(string)

	return v
}

func nameOf(m map[string]any) string { return strField(m, "name") }

func rfc3339Now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func failedPre(format string, args ...any) *APIError {
	e := errInvalid(format, args...)
	e.Reason = failedPrecondition

	return e
}

func locationPath(project, location string) string {
	return "projects/" + project + "/locations/" + location
}

func int64Field(m map[string]any, key string) (int64, bool, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, false, nil
	}

	n, ok := intOf(v)
	if !ok {
		return 0, true, errInvalid("%s must be an int64", key)
	}

	return int64(n), true, nil
}

func setInt64(m map[string]any, key string, n int64) { m[key] = strconv.FormatInt(n, 10) }

func clientFields(body map[string]any, output ...string) map[string]any {
	out := cloneMap(body)
	for _, k := range append(output, "name") {
		delete(out, k)
	}

	return out
}

func validateReservation(r map[string]any) error {
	if err := validateEnum(r, "edition", editions()...); err != nil {
		return err
	}

	if err := validateEnum(r, litScalingMode, scalingUnspecified, "AUTOSCALE_ONLY", "IDLE_SLOTS_ONLY",
		"ALL_SLOTS"); err != nil {
		return err
	}

	slots, _, err := int64Field(r, litSlotCapacity)
	if err != nil {
		return err
	}

	maxSlots, hasMax, err := int64Field(r, litMaxSlots)
	if err != nil {
		return err
	}

	if slots < 0 {
		return errInvalid("slotCapacity must not be negative")
	}

	if conc, _, cerr := int64Field(r, "concurrency"); cerr != nil || conc < 0 {
		return errInvalid("concurrency must be a non-negative int64")
	}

	return validateScaling(r, slots, maxSlots, hasMax)
}

func validateScaling(r map[string]any, slots, maxSlots int64, hasMax bool) error {
	mode, _ := r[litScalingMode].(string)
	hasMode := mode != "" && mode != scalingUnspecified
	ignore, _ := r[litIgnoreIdle].(bool)
	capped := hasMax && maxSlots != 0

	if hasMax && maxSlots == 0 && !hasMode {
		delete(r, litMaxSlots)

		return nil
	}

	if err := validateModeCombo(mode, ignore); err != nil {
		return err
	}

	switch {
	case hasMode && !capped:
		return errInvalid("scalingMode requires max_slots to be set")
	case capped && !hasMode:
		return errInvalid("max_slots must be set together with scalingMode")
	case capped && r[litAutoscale] != nil:
		return errInvalid("autoscale must be unset when max_slots and scalingMode are set")
	case capped && slots >= maxSlots:
		return errInvalid("slotCapacity must be smaller than max_slots")
	}

	return nil
}

func validateModeCombo(mode string, ignore bool) error {
	switch {
	case mode == "AUTOSCALE_ONLY" && !ignore:
		return errInvalid("ignoreIdleSlots must be true when scalingMode is AUTOSCALE_ONLY")
	case (mode == "IDLE_SLOTS_ONLY" || mode == "ALL_SLOTS") && ignore:
		return errInvalid("ignoreIdleSlots must be false when scalingMode is %s", mode)
	}

	return nil
}

func editions() []string {
	return []string{"EDITION_UNSPECIFIED", "STANDARD", "ENTERPRISE", "ENTERPRISE_PLUS"}
}

func validateEnum(m map[string]any, key string, allowed ...string) error {
	v, ok := m[key].(string)
	if !ok || v == "" {
		return nil
	}

	if !strs.ContainsFold(allowed, v) {
		return errInvalid("Invalid value %q for %s", v, key)
	}

	return nil
}

func validResourceID(id string, re *regexp.Regexp) bool {
	return id != "" && len(id) <= maxResourceIDLen && re.MatchString(id)
}

func (b *InMemoryBackend) resName(kind, project, location, id string) string {
	return locationPath(project, location) + "/" + kind + "/" + id
}

// CreateReservation implements reservations.create.
func (b *InMemoryBackend) CreateReservation(
	project, location, id string, body map[string]any,
) (map[string]any, error) {
	if !validResourceID(id, reservationIDPattern()) {
		return nil, errInvalid("Invalid reservation ID %q: use lower case letters, numbers and dashes, "+
			"start with a letter, do not end with a dash, at most 64 characters", id)
	}

	r := clientFields(body, "creationTime", litUpdateTime, "originalPrimaryLocation", "primaryLocation",
		"replicationStatus", "reservationGroupPath")
	if err := validateReservation(r); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateReservation")
	defer b.mu.Unlock()

	name := b.resName(segReservations, project, location, id)
	if _, ok := b.res.Reservations[name]; ok {
		return nil, errDuplicate("Already Exists: Reservation %s", name)
	}

	r["name"], r["creationTime"], r[litUpdateTime] = name, rfc3339Now(), rfc3339Now()
	normaliseInts(r)
	b.res.Reservations[name] = r

	b.auditCall("google.cloud.bigquery.reservation.v1.ReservationService.CreateReservation", name, project)

	return cloneMap(r), nil
}

func normaliseInts(m map[string]any) {
	for _, k := range []string{litSlotCapacity, litMaxSlots, "concurrency", litSlotCount, litSize, "precedence"} {
		if n, ok, err := int64Field(m, k); ok && err == nil {
			setInt64(m, k, n)
		}
	}
}

// GetReservation implements reservations.get.
func (b *InMemoryBackend) GetReservation(project, location, id string) (map[string]any, error) {
	b.mu.RLock("GetReservation")
	defer b.mu.RUnlock()

	r, ok := b.res.Reservations[b.resName(segReservations, project, location, id)]
	if !ok {
		return nil, errNotFound("Not found: Reservation %s", b.resName(segReservations, project, location, id))
	}

	return cloneMap(r), nil
}

func listByPrefix(store map[string]map[string]any, prefix string, o ListOptions) page.Page[map[string]any] {
	names := make([]string, 0, len(store))

	for n := range store {
		if strings.HasPrefix(n, prefix) && !strings.Contains(strings.TrimPrefix(n, prefix), "/") {
			names = append(names, n)
		}
	}

	sort.Strings(names)

	all := make([]map[string]any, len(names))
	for i, n := range names {
		all[i] = cloneMap(store[n])
	}

	return page.New(all, o.PageToken, o.Max, defaultListMax)
}

// ListReservations implements reservations.list.
func (b *InMemoryBackend) ListReservations(project, location string, o ListOptions) page.Page[map[string]any] {
	b.mu.RLock("ListReservations")
	defer b.mu.RUnlock()

	return listByPrefix(b.res.Reservations, locationPath(project, location)+"/"+segReservations+"/", o)
}

func maskedPatch(dst, body map[string]any, mask string, immutable ...string) error {
	patch := cloneMap(body)
	for _, k := range immutable {
		delete(patch, k)
	}

	if mask == "" {
		maps.Copy(dst, patch)

		return nil
	}

	for f := range strings.SplitSeq(mask, ",") {
		f = camelCase(strings.TrimSpace(f))
		if slices.Contains(immutable, f) {
			return errInvalid("Field %s cannot be updated", f)
		}

		if v, ok := patch[f]; ok {
			dst[f] = v
		} else {
			delete(dst, f)
		}
	}

	return nil
}

func camelCase(snake string) string {
	parts := strings.Split(snake, "_")
	for i := 1; i < len(parts); i++ {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}

	return strings.Join(parts, "")
}

// UpdateReservation implements reservations.patch.
func (b *InMemoryBackend) UpdateReservation(
	project, location, id, mask string, body map[string]any,
) (map[string]any, error) {
	b.mu.Lock("UpdateReservation")
	defer b.mu.Unlock()

	name := b.resName(segReservations, project, location, id)

	r, ok := b.res.Reservations[name]
	if !ok {
		return nil, errNotFound("Not found: Reservation %s", name)
	}

	merged := cloneMap(r)
	if err := maskedPatch(merged, body, mask, "name", "creationTime", litUpdateTime); err != nil {
		return nil, err
	}

	normaliseInts(merged)

	if err := validateReservation(merged); err != nil {
		return nil, err
	}

	merged[litUpdateTime] = rfc3339Now()
	b.res.Reservations[name] = merged

	return cloneMap(merged), nil
}

// DeleteReservation implements reservations.delete; a reservation with assignments cannot be deleted.
func (b *InMemoryBackend) DeleteReservation(project, location, id string) error {
	b.mu.Lock("DeleteReservation")
	defer b.mu.Unlock()

	name := b.resName(segReservations, project, location, id)
	if _, ok := b.res.Reservations[name]; !ok {
		return errNotFound("Not found: Reservation %s", name)
	}

	for an := range b.res.Assignments {
		if strings.HasPrefix(an, name+"/") {
			return failedPre("Reservation %s has assignments; delete them first", name)
		}
	}

	delete(b.res.Reservations, name)

	b.auditCall("google.cloud.bigquery.reservation.v1.ReservationService.DeleteReservation", name, project)

	return nil
}

func commitmentPlans() []string {
	return []string{flexPlan, "FLEX_ANNUAL", "TRIAL", "MONTHLY", "ANNUAL", "THREE_YEAR"}
}

func planEnd(plan string, start time.Time) time.Time {
	switch strings.ToUpper(plan) {
	case "MONTHLY", "TRIAL":
		return start.AddDate(0, 1, 0)
	case "ANNUAL", "FLEX_ANNUAL":
		return start.AddDate(1, 0, 0)
	case "THREE_YEAR":
		return start.AddDate(yearsThree, 0, 0)
	}

	return start.Add(time.Minute)
}

func planRank(plan string) int {
	for i, p := range commitmentPlans() {
		if strings.EqualFold(p, plan) {
			return i
		}
	}

	return -1
}

func validateCommitment(c map[string]any) error {
	if err := validateEnum(c, litPlan, append([]string{noPlan}, commitmentPlans()...)...); err != nil {
		return err
	}

	if err := validateEnum(c, litRenewalPlan, append([]string{noPlan}, commitmentPlans()...)...); err != nil {
		return err
	}

	if err := validateEnum(c, "edition", editions()...); err != nil {
		return err
	}

	n, ok, err := int64Field(c, litSlotCount)
	if err != nil || !ok || n <= 0 || n%slotMultiple != 0 {
		return errInvalid("slotCount must be a positive multiple of %d", slotMultiple)
	}

	if plan, _ := c[litPlan].(string); planRank(plan) < 0 {
		return errInvalid("plan is required and must be one of %s", strings.Join(commitmentPlans(), ", "))
	}

	return nil
}

func (b *InMemoryBackend) newCommitment(project, location, id string, c map[string]any) map[string]any {
	now := time.Now().UTC()
	plan, _ := c[litPlan].(string)
	c["name"] = b.resName(segCommitments, project, location, id)
	c[litState] = "ACTIVE"
	c["commitmentStartTime"] = now.Format(time.RFC3339Nano)
	c["commitmentEndTime"] = planEnd(plan, now).Format(time.RFC3339Nano)
	normaliseInts(c)

	b.res.Commitments[nameOf(c)] = c

	return c
}

// CreateCapacityCommitment implements capacityCommitments.create.
func (b *InMemoryBackend) CreateCapacityCommitment(
	project, location, id string, body map[string]any,
) (map[string]any, error) {
	c := clientFields(body, "commitmentStartTime", "commitmentEndTime", litState, "failureStatus", "isFlatRate")
	if err := validateCommitment(c); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateCapacityCommitment")
	defer b.mu.Unlock()

	if id == "" {
		id = "cc-" + strconv.FormatInt(b.nextSeq(), 10)
	}

	if !validResourceID(id, commitmentIDPattern()) {
		return nil, errInvalid("Invalid capacity commitment ID %q", id)
	}

	if _, ok := b.res.Commitments[b.resName(segCommitments, project, location, id)]; ok {
		return nil, errDuplicate("Already Exists: Capacity commitment %s", id)
	}

	out := b.newCommitment(project, location, id, c)
	b.auditCall(reservationAudit+"CreateCapacityCommitment", nameOf(out), project)

	return cloneMap(out), nil
}

// GetCapacityCommitment implements capacityCommitments.get.
func (b *InMemoryBackend) GetCapacityCommitment(project, location, id string) (map[string]any, error) {
	b.mu.RLock("GetCapacityCommitment")
	defer b.mu.RUnlock()

	name := b.resName(segCommitments, project, location, id)

	c, ok := b.res.Commitments[name]
	if !ok {
		return nil, errNotFound("Not found: Capacity commitment %s", name)
	}

	return cloneMap(c), nil
}

// ListCapacityCommitments implements capacityCommitments.list.
func (b *InMemoryBackend) ListCapacityCommitments(project, location string, o ListOptions) page.Page[map[string]any] {
	b.mu.RLock("ListCapacityCommitments")
	defer b.mu.RUnlock()

	return listByPrefix(b.res.Commitments, locationPath(project, location)+"/"+segCommitments+"/", o)
}

// UpdateCapacityCommitment implements capacityCommitments.patch: only plan and renewalPlan change, and the plan can
// only move to a longer commitment period.
func (b *InMemoryBackend) UpdateCapacityCommitment(
	project, location, id, mask string, body map[string]any,
) (map[string]any, error) {
	b.mu.Lock("UpdateCapacityCommitment")
	defer b.mu.Unlock()

	name := b.resName(segCommitments, project, location, id)

	c, ok := b.res.Commitments[name]
	if !ok {
		return nil, errNotFound("Not found: Capacity commitment %s", name)
	}

	merged := cloneMap(c)
	patch := map[string]any{}

	for _, k := range []string{litPlan, litRenewalPlan} {
		if v, set := body[k]; set {
			patch[k] = v
		}
	}

	immutable := []string{litName, litSlotCount, litState, "commitmentStartTime", "commitmentEndTime", "edition"}
	if err := maskedPatch(merged, patch, mask, immutable...); err != nil {
		return nil, err
	}

	if err := validateEnum(merged, litRenewalPlan, append([]string{noPlan}, commitmentPlans()...)...); err != nil {
		return nil, err
	}

	oldPlan, _ := c[litPlan].(string)
	newPlan, _ := merged[litPlan].(string)

	switch {
	case planRank(newPlan) < 0:
		return nil, errInvalid("plan must be one of %s", strings.Join(commitmentPlans(), ", "))
	case !strings.EqualFold(oldPlan, newPlan) && planEnd(newPlan, time.Time{}).Before(planEnd(oldPlan, time.Time{})):
		return nil, failedPre("The plan can only be changed to one with a longer commitment period")
	}

	if !strings.EqualFold(oldPlan, newPlan) {
		start, _ := time.Parse(time.RFC3339Nano, strField(c, "commitmentStartTime"))
		merged["commitmentEndTime"] = planEnd(newPlan, start).Format(time.RFC3339Nano)
	}

	b.res.Commitments[name] = merged

	return cloneMap(merged), nil
}

// DeleteCapacityCommitment implements capacityCommitments.delete; it fails before the commitment end time.
func (b *InMemoryBackend) DeleteCapacityCommitment(project, location, id string) error {
	b.mu.Lock("DeleteCapacityCommitment")
	defer b.mu.Unlock()

	name := b.resName(segCommitments, project, location, id)

	c, ok := b.res.Commitments[name]
	if !ok {
		return errNotFound("Not found: Capacity commitment %s", name)
	}

	end, _ := time.Parse(time.RFC3339Nano, strField(c, "commitmentEndTime"))
	if time.Now().Before(end) {
		return failedPre("Capacity commitment %s cannot be deleted before its commitmentEndTime %s",
			name, end.Format(time.RFC3339))
	}

	delete(b.res.Commitments, name)

	return nil
}

func shortID(name string) string { return name[strings.LastIndex(name, "/")+1:] }

// MergeCapacityCommitments implements capacityCommitments.merge: commitments of one plan become a single one.
func (b *InMemoryBackend) MergeCapacityCommitments(
	project, location string, body map[string]any,
) (map[string]any, error) {
	ids := anyStrings(body["capacityCommitmentIds"])
	if len(ids) < 2 { //nolint:mnd // merging needs two commitments
		return nil, errInvalid("capacityCommitmentIds needs at least two commitments")
	}

	b.mu.Lock("MergeCapacityCommitments")
	defer b.mu.Unlock()

	var (
		total int64
		plan  string
		end   string
		names []string
	)

	for _, raw := range ids {
		name := b.resName(segCommitments, project, location, shortID(raw))

		c, ok := b.res.Commitments[name]
		if !ok {
			return nil, errNotFound("Not found: Capacity commitment %s", name)
		}

		p, _ := c[litPlan].(string)
		if plan != "" && !strings.EqualFold(plan, p) {
			return nil, failedPre("Capacity commitments of different plans cannot be merged")
		}

		n, _, _ := int64Field(c, litSlotCount)
		total += n
		plan = p

		if e, _ := c["commitmentEndTime"].(string); e > end {
			end = e
		}

		names = append(names, name)
	}

	merged := map[string]any{litPlan: plan, litSlotCount: total}
	if rp, ok := b.res.Commitments[names[0]][litRenewalPlan]; ok {
		merged[litRenewalPlan] = rp
	}

	out := b.newCommitment(project, location, "cc-"+strconv.FormatInt(b.nextSeq(), 10), merged)
	out["commitmentEndTime"] = end

	for _, n := range names {
		delete(b.res.Commitments, n)
	}

	return cloneMap(out), nil
}

// SplitCapacityCommitment implements capacityCommitments.split; the pieces keep the plan and end time.
func (b *InMemoryBackend) SplitCapacityCommitment(
	project, location, id string, body map[string]any,
) (map[string]any, error) {
	n, ok, err := int64Field(body, litSlotCount)
	if err != nil || !ok || n <= 0 || n%slotMultiple != 0 {
		return nil, errInvalid("slotCount must be a positive multiple of %d", slotMultiple)
	}

	b.mu.Lock("SplitCapacityCommitment")
	defer b.mu.Unlock()

	name := b.resName(segCommitments, project, location, id)

	c, found := b.res.Commitments[name]
	if !found {
		return nil, errNotFound("Not found: Capacity commitment %s", name)
	}

	have, _, _ := int64Field(c, litSlotCount)
	if n >= have {
		return nil, errInvalid("slotCount must be smaller than the commitment's %d slots", have)
	}

	pieces := make([]map[string]any, 0, 2) //nolint:mnd // two halves

	for _, count := range []int64{n, have - n} {
		piece := cloneMap(c)
		piece[litSlotCount] = count
		out := b.newCommitment(project, location, "cc-"+strconv.FormatInt(b.nextSeq(), 10), piece)
		out["commitmentStartTime"], out["commitmentEndTime"] = c["commitmentStartTime"], c["commitmentEndTime"]
		pieces = append(pieces, cloneMap(out))
	}

	delete(b.res.Commitments, name)

	return map[string]any{"first": pieces[0], "second": pieces[1]}, nil
}

func jobTypes() []string {
	return []string{
		"PIPELINE", "QUERY", "ML_EXTERNAL", "BACKGROUND", "CONTINUOUS", "BACKGROUND_CHANGE_DATA_CAPTURE",
		"BACKGROUND_COLUMN_METADATA_INDEX", "BACKGROUND_SEARCH_INDEX_REFRESH",
	}
}

func validateAssignment(a map[string]any) error {
	assignee, _ := a[litAssignee].(string)
	if !assigneePattern().MatchString(assignee) {
		return errInvalid("assignee must be projects/{id}, folders/{id} or organizations/{id}")
	}

	jt, _ := a[litJobType].(string)
	if !strs.ContainsFold(jobTypes(), jt) {
		return errInvalid("jobType is required and must be one of %s", strings.Join(jobTypes(), ", "))
	}

	return nil
}

func (b *InMemoryBackend) assignmentConflict(project, location string, a map[string]any, except string) bool {
	prefix := locationPath(project, location) + "/" + segReservations + "/"

	for n, o := range b.res.Assignments {
		same := o[litAssignee] == a[litAssignee] && o[litJobType] == a[litJobType]
		if n != except && strings.HasPrefix(n, prefix) && same {
			return true
		}
	}

	return false
}

// CreateAssignment implements reservations.assignments.create.
func (b *InMemoryBackend) CreateAssignment(
	project, location, reservation, id string, body map[string]any,
) (map[string]any, error) {
	a := clientFields(body, litState, "principal")
	if err := validateAssignment(a); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateAssignment")
	defer b.mu.Unlock()

	parent := b.resName(segReservations, project, location, reservation)
	if _, ok := b.res.Reservations[parent]; !ok {
		return nil, errNotFound("Not found: Reservation %s", parent)
	}

	if id == "" {
		id = strconv.FormatInt(b.nextSeq(), 10)
	}

	if !validResourceID(id, commitmentIDPattern()) {
		return nil, errInvalid("Invalid assignment ID %q", id)
	}

	if b.assignmentConflict(project, location, a, "") {
		return nil, errDuplicate("Already Exists: an assignment for %v with job type %v already exists",
			a[litAssignee], a[litJobType])
	}

	a["name"] = parent + "/" + segAssignments + "/" + id
	a[litState] = "ACTIVE"
	normaliseInts(a)
	b.res.Assignments[nameOf(a)] = a

	return cloneMap(a), nil
}

// ListAssignments implements reservations.assignments.list; reservation "-" lists every reservation's assignments.
func (b *InMemoryBackend) ListAssignments(
	project, location, reservation string, o ListOptions,
) (page.Page[map[string]any], error) {
	b.mu.RLock("ListAssignments")
	defer b.mu.RUnlock()

	if reservation == wildcardParent {
		return b.searchLocked(project, location, "", o), nil
	}

	parent := b.resName(segReservations, project, location, reservation)
	if _, ok := b.res.Reservations[parent]; !ok {
		return page.Page[map[string]any]{}, errNotFound("Not found: Reservation %s", parent)
	}

	return listByPrefix(b.res.Assignments, parent+"/"+segAssignments+"/", o), nil
}

func (b *InMemoryBackend) searchLocked(project, location, assignee string, o ListOptions) page.Page[map[string]any] {
	prefix := locationPath(project, location) + "/" + segReservations + "/"
	names := make([]string, 0, len(b.res.Assignments))

	for n, a := range b.res.Assignments {
		if strings.HasPrefix(n, prefix) && (assignee == "" || a[litAssignee] == assignee) {
			names = append(names, n)
		}
	}

	sort.Strings(names)

	all := make([]map[string]any, len(names))
	for i, n := range names {
		all[i] = cloneMap(b.res.Assignments[n])
	}

	return page.New(all, o.PageToken, o.Max, defaultListMax)
}

// SearchAssignments implements searchAssignments and searchAllAssignments; the query is assignee="projects/x".
func (b *InMemoryBackend) SearchAssignments(
	project, location, query string, o ListOptions,
) (page.Page[map[string]any], error) {
	assignee := ""

	if q := strings.TrimSpace(query); q != "" {
		key, val, ok := strings.Cut(q, "=")
		if !ok || strings.TrimSpace(key) != litAssignee {
			return page.Page[map[string]any]{}, errInvalid("Invalid query %q; use assignee=\"projects/{id}\"", query)
		}

		assignee = strings.Trim(strings.TrimSpace(val), `"'`)
	}

	b.mu.RLock("SearchAssignments")
	defer b.mu.RUnlock()

	return b.searchLocked(project, location, assignee, o), nil
}

// DeleteAssignment implements reservations.assignments.delete.
func (b *InMemoryBackend) DeleteAssignment(project, location, reservation, id string) error {
	b.mu.Lock("DeleteAssignment")
	defer b.mu.Unlock()

	name := b.resName(segReservations, project, location, reservation) + "/" + segAssignments + "/" + id
	if _, ok := b.res.Assignments[name]; !ok {
		return errNotFound("Not found: Assignment %s", name)
	}

	delete(b.res.Assignments, name)

	return nil
}

// UpdateAssignment implements reservations.assignments.patch; the assignee and job type are fixed.
func (b *InMemoryBackend) UpdateAssignment(
	project, location, reservation, id, mask string, body map[string]any,
) (map[string]any, error) {
	b.mu.Lock("UpdateAssignment")
	defer b.mu.Unlock()

	name := b.resName(segReservations, project, location, reservation) + "/" + segAssignments + "/" + id

	a, ok := b.res.Assignments[name]
	if !ok {
		return nil, errNotFound("Not found: Assignment %s", name)
	}

	merged := cloneMap(a)
	if err := maskedPatch(merged, body, mask, "name", litAssignee, litJobType, litState, "principal"); err != nil {
		return nil, err
	}

	normaliseInts(merged)
	b.res.Assignments[name] = merged

	return cloneMap(merged), nil
}

// MoveAssignment implements reservations.assignments.move into another reservation of the same location.
func (b *InMemoryBackend) MoveAssignment(
	project, location, reservation, id string, body map[string]any,
) (map[string]any, error) {
	dest, _ := body["destinationId"].(string)
	if dest == "" {
		return nil, errInvalid("destinationId is required")
	}

	b.mu.Lock("MoveAssignment")
	defer b.mu.Unlock()

	name := b.resName(segReservations, project, location, reservation) + "/" + segAssignments + "/" + id

	a, ok := b.res.Assignments[name]
	if !ok {
		return nil, errNotFound("Not found: Assignment %s", name)
	}

	destRes := dest
	if !strings.HasPrefix(dest, "projects/") {
		destRes = b.resName(segReservations, project, location, dest)
	}

	if _, found := b.res.Reservations[destRes]; !found {
		return nil, errNotFound("Not found: Reservation %s", destRes)
	}

	newID := id
	if v, _ := body["assignmentId"].(string); v != "" {
		newID = v
	}

	moved := cloneMap(a)
	moved["name"] = destRes + "/" + segAssignments + "/" + newID

	if _, exists := b.res.Assignments[nameOf(moved)]; exists && moved["name"] != name {
		return nil, errDuplicate("Already Exists: Assignment %s", moved["name"])
	}

	delete(b.res.Assignments, name)
	b.res.Assignments[nameOf(moved)] = moved

	return cloneMap(moved), nil
}

// GetBiReservation implements getBiReservation.
func (b *InMemoryBackend) GetBiReservation(project, location string) map[string]any {
	b.mu.RLock("GetBiReservation")
	defer b.mu.RUnlock()

	name := locationPath(project, location) + "/" + segBIReservation
	if r, ok := b.res.BI[name]; ok {
		return cloneMap(r)
	}

	return map[string]any{"name": name, litSize: "0"}
}

// UpdateBiReservation implements updateBiReservation.
func (b *InMemoryBackend) UpdateBiReservation(
	project, location, mask string, body map[string]any,
) (map[string]any, error) {
	name := locationPath(project, location) + "/" + segBIReservation

	b.mu.Lock("UpdateBiReservation")
	defer b.mu.Unlock()

	cur, ok := b.res.BI[name]
	if !ok {
		cur = map[string]any{"name": name, litSize: "0"}
	}

	merged := cloneMap(cur)
	if err := maskedPatch(merged, body, mask, "name", litUpdateTime); err != nil {
		return nil, err
	}

	n, _, err := int64Field(merged, litSize)
	if err != nil || n < 0 {
		return nil, errInvalid("size must be a non-negative number of bytes")
	}

	normaliseInts(merged)
	merged[litUpdateTime] = rfc3339Now()
	b.res.BI[name] = merged

	return cloneMap(merged), nil
}
