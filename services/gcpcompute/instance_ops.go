package gcpcompute

import (
	"reflect"
	"regexp"
	"slices"
	"strconv"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	phaseSuspend = "suspend"
	stSuspending = "SUSPENDING"
	stSuspended  = "SUSPENDED"
	actNoEffect  = "NO_EFFECT"
	actRefresh   = "REFRESH"
	actRestart   = "RESTART"
	maxSerial    = 4
)

var (
	emailRE         = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)
	actionOrder     = []string{actNoEffect, actRefresh, actRestart} //nolint:gochecknoglobals // lookup
	minCPUPlatforms = []string{                                     //nolint:gochecknoglobals // read-only lookup table
		"Automatic", "Intel Sandy Bridge", "Intel Ivy Bridge", "Intel Haswell", "Intel Broadwell", "Intel Skylake",
		platCascade, "Intel Ice Lake", "Intel Sapphire Rapids", "AMD Rome", "AMD Milan", "AMD Genoa",
	}
	updateSkip = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
		jsonKind: true, "id": true, jsonSelfLink: true, "selfLinkWithId": true, jsonCreated: true, "status": true,
		"statusMessage": true, "cpuPlatform": true, "lastStartTimestamp": true, "lastStopTimestamp": true,
		"lastSuspendedTimestamp": true, "zone": true, "resourceStatus": true, "satisfiesPzi": true,
		"satisfiesPzs": true, "startRestricted": true, jsonFingerp: true, "labelFingerprint": true,
	}
	updateNoEffect = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
		jsonDesc: true, "deletionProtection": true, jsonLabels: true, "metadata": true, "tags": true,
	}
	updateRestart = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
		"canIpForward": true, "shieldedInstanceConfig": true, "displayDevice": true, "minCpuPlatform": true,
		"serviceAccounts": true,
	}
)

type updateOpts struct {
	minimal        string
	mostDisruptive string
}

type serviceAccountRequest struct {
	Email  string   `json:"email"`
	Scopes []string `json:"scopes"`
}

type minCPURequest struct {
	MinCPUPlatform string `json:"minCpuPlatform"`
}

func (b *InMemoryBackend) requireStoppedLocked(r *instRec, project, zone, what string) *APIError {
	if r.status(b.now().UTC(), b.timing) != stTerminated {
		return errNotReady(
			relPath(project, scopeZone, zone, "instances", r.Inst.Name),
			"the instance must be TERMINATED to change its "+what,
		)
	}

	return nil
}

func (b *InMemoryBackend) setInstanceScheduling(
	project, zone, name string, in *computeapi.Scheduling,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetScheduling")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	next, derr := defaultScheduling(in)
	if derr != nil {
		return nil, derr
	}

	if err = b.requireStoppedLocked(r, project, zone, "scheduling options"); err != nil {
		return nil, err
	}

	r.Inst.Scheduling = next
	b.touchLocked(r)

	return b.newOpLocked("setScheduling", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func schedulingNeedsStop(cur, next *computeapi.Scheduling) bool {
	if cur == nil {
		return false
	}

	return cur.Preemptible != next.Preemptible || cur.ProvisioningModel != next.ProvisioningModel ||
		cur.InstanceTerminationAction != next.InstanceTerminationAction || cur.MinNodeCpus != next.MinNodeCpus ||
		!reflect.DeepEqual(cur.NodeAffinities, next.NodeAffinities)
}

func validServiceAccounts(email string, scopes []string) ([]*computeapi.ServiceAccount, *APIError) {
	if email == "" {
		if len(scopes) > 0 {
			return nil, errRequired("email")
		}

		return nil, nil
	}

	if !emailRE.MatchString(email) {
		return nil, errField("email", email, "Must be a service account email address.")
	}

	for _, s := range scopes {
		if s == "" {
			return nil, errField("scopes", s, "Scopes cannot be empty.")
		}
	}

	return []*computeapi.ServiceAccount{{Email: email, Scopes: scopes}}, nil
}

func (b *InMemoryBackend) setInstanceServiceAccount(
	project, zone, name string, in *serviceAccountRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetServiceAccount")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if err = b.requireStoppedLocked(r, project, zone, "service account"); err != nil {
		return nil, err
	}

	accounts, aerr := validServiceAccounts(in.Email, in.Scopes)
	if aerr != nil {
		return nil, aerr
	}

	r.Inst.ServiceAccounts = accounts
	b.touchLocked(r)

	return b.newOpLocked("setServiceAccount", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func (b *InMemoryBackend) setInstanceMinCPUPlatform(
	project, zone, name, platform string,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetMinCpuPlatform")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if err = b.requireStoppedLocked(r, project, zone, "minimum CPU platform"); err != nil {
		return nil, err
	}

	if !slices.Contains(minCPUPlatforms, platform) {
		return nil, errField("minCpuPlatform", platform, "Not a supported CPU platform.")
	}

	r.Inst.MinCpuPlatform = platform
	b.touchLocked(r)

	return b.newOpLocked("setMinCpuPlatform", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func (b *InMemoryBackend) updateInstanceShielded(
	project, zone, name string, in *computeapi.ShieldedInstanceConfig, raw map[string]any,
) (*computeapi.Operation, error) {
	b.mu.Lock("UpdateShieldedInstanceConfig")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if err = b.requireStoppedLocked(r, project, zone, "shielded instance configuration"); err != nil {
		return nil, err
	}

	cfg := clone(r.Inst.ShieldedInstanceConfig)
	if cfg == nil {
		cfg = &computeapi.ShieldedInstanceConfig{}
	}

	if _, ok := raw["enableSecureBoot"]; ok {
		cfg.EnableSecureBoot = in.EnableSecureBoot
	}

	if _, ok := raw["enableVtpm"]; ok {
		cfg.EnableVtpm = in.EnableVtpm
	}

	if _, ok := raw["enableIntegrityMonitoring"]; ok {
		cfg.EnableIntegrityMonitoring = in.EnableIntegrityMonitoring
	}

	r.Inst.ShieldedInstanceConfig = cfg
	b.touchLocked(r)

	return b.newOpLocked("updateShieldedInstanceConfig", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func (b *InMemoryBackend) setInstanceIntegrityPolicy(
	project, zone, name string, in *computeapi.ShieldedInstanceIntegrityPolicy,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetShieldedInstanceIntegrityPolicy")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if r.status(b.now().UTC(), b.timing) != stRunning {
		return nil, errNotReady(relPath(project, scopeZone, zone, "instances", name),
			"the instance must be RUNNING to change its integrity policy")
	}

	r.Inst.ShieldedInstanceIntegrityPolicy = &computeapi.ShieldedInstanceIntegrityPolicy{
		UpdateAutoLearnPolicy: in.UpdateAutoLearnPolicy,
	}
	b.touchLocked(r)

	return b.newOpLocked("setShieldedInstanceIntegrityPolicy", instTarget(project, zone, r.Inst), b.timing.Done), nil
}

func (b *InMemoryBackend) suspendInstance(project, zone, name string) (*computeapi.Operation, error) {
	b.mu.Lock("SuspendInstance")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	path := relPath(project, scopeZone, zone, "instances", name)

	if r.status(b.now().UTC(), b.timing) != stRunning {
		return nil, errNotReady(path, "the instance must be RUNNING to be suspended")
	}

	if s := r.Inst.Scheduling; s != nil && (s.Preemptible || s.ProvisioningModel == provSpot) {
		return nil, errInvalid("Invalid value for field 'resource.scheduling': Spot and preemptible instances " +
			"cannot be suspended.")
	}

	if len(r.Inst.GuestAccelerators) > 0 {
		return nil, errInvalid("Invalid value for field 'resource.guestAccelerators': instances with GPUs " +
			"cannot be suspended.")
	}

	now := b.now().UTC()
	r.Phase, r.Since = phaseSuspend, now
	r.Inst.LastSuspendedTimestamp = now.Format(timeLayout)
	b.touchLocked(r)

	return b.newOpLocked("suspend", instTarget(project, zone, r.Inst), b.timing.Stop), nil
}

func (b *InMemoryBackend) resumeInstance(project, zone, name string) (*computeapi.Operation, error) {
	b.mu.Lock("ResumeInstance")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if r.status(b.now().UTC(), b.timing) != stSuspended {
		return nil, errNotReady(relPath(project, scopeZone, zone, "instances", name),
			"the instance must be SUSPENDED to be resumed")
	}

	now := b.now().UTC()
	r.Phase, r.Since = phaseStart, now
	r.Inst.LastStartTimestamp = now.Format(timeLayout)
	b.touchLocked(r)

	return b.newOpLocked("resume", instTarget(project, zone, r.Inst), b.timing.Stage), nil
}

func (b *InMemoryBackend) serialPortOutput(
	project, zone, name string, port, start int64,
) (*computeapi.SerialPortOutput, error) {
	b.mu.RLock("GetSerialPortOutput")
	defer b.mu.RUnlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if port < 1 || port > maxSerial {
		return nil, errField("port", strconv.FormatInt(port, 10), "Must be between 1 and 4.")
	}

	if start < 0 {
		return nil, errField("start", strconv.FormatInt(start, 10), "Must not be negative.")
	}

	return &computeapi.SerialPortOutput{
		Kind: "compute#serialPortOutput", SelfLink: r.Inst.SelfLink + "/serialPort",
		ForceSendFields: []string{"Start", "Next"},
	}, nil
}

func (b *InMemoryBackend) guestAttributes(project, zone, name string) (*computeapi.GuestAttributes, error) {
	b.mu.RLock("GetGuestAttributes")
	defer b.mu.RUnlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	return nil, errNotFound(trimURL(r.Inst.SelfLink) + "/guestAttributes")
}

func actionOf(raw, field string) (string, *APIError) {
	if raw == "" {
		return "", nil
	}

	if !slices.Contains(actionOrder, raw) {
		return "", errField(field, raw, "Must be NO_EFFECT, REFRESH or RESTART.")
	}

	return raw, nil
}

func (b *InMemoryBackend) updateInstance(
	project, zone, name string, in *computeapi.Instance, raw map[string]any, o updateOpts,
) (*computeapi.Operation, error) {
	b.mu.Lock("UpdateInstance")
	defer b.mu.Unlock()

	r, err := b.getInstLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	minimal, aerr := actionOf(o.minimal, "minimalAction")
	if aerr != nil {
		return nil, aerr
	}

	most, aerr := actionOf(o.mostDisruptive, "mostDisruptiveAllowedAction")
	if aerr != nil {
		return nil, aerr
	}

	next := clone(r.Inst)

	need, aerr := b.applyInstanceUpdate(r.Inst, next, in, raw)
	if aerr != nil {
		return nil, aerr
	}

	if actionRank(minimal) > actionRank(need) {
		need = minimal
	}

	if most != "" && actionRank(need) > actionRank(most) {
		return nil, errField("mostDisruptiveAllowedAction", most,
			"The update requires the "+need+" action, which is more disruptive than allowed.")
	}

	restart := need == actRestart && r.status(b.now().UTC(), b.timing) == stRunning

	r.Inst = next
	b.touchLocked(r)

	done := b.timing.Done

	if restart {
		now := b.now().UTC()
		r.Phase, r.Since = phaseStart, now
		r.Inst.LastStopTimestamp, r.Inst.LastStartTimestamp = now.Format(timeLayout), now.Format(timeLayout)
		done = b.timing.Stage
	}

	return b.newOpLocked("update", instTarget(project, zone, r.Inst), done), nil
}

// applyInstanceUpdate copies the changed, updatable fields of in onto next and returns the least disruptive action.
func (b *InMemoryBackend) applyInstanceUpdate(
	cur, next, in *computeapi.Instance, raw map[string]any,
) (string, *APIError) {
	if fp, ok := raw[jsonFingerp].(string); ok && fp != "" && fp != cur.Fingerprint {
		return "", errPrecondition("Supplied fingerprint does not match current fingerprint.")
	}

	curDoc := toDoc(cur)
	need := actNoEffect

	keys := make([]string, 0, len(raw))
	for k := range raw {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	for _, k := range keys {
		if updateSkip[k] || reflect.DeepEqual(raw[k], curDoc[k]) {
			continue
		}

		act, err := b.updateField(cur, next, in, raw, k)
		if err != nil {
			return "", err
		}

		if actionRank(act) > actionRank(need) {
			need = act
		}
	}

	if lf, ok := raw["labelFingerprint"].(string); ok && lf != "" && lf != cur.LabelFingerprint {
		if _, labelsGiven := raw[jsonLabels]; labelsGiven {
			return "", errPrecondition("%s", labelsStaleDetail)
		}
	}

	return need, nil
}

func (b *InMemoryBackend) updateField(
	cur, next, in *computeapi.Instance, raw map[string]any, k string,
) (string, *APIError) {
	switch {
	case k == "machineType" && lastSegment(in.MachineType) == lastSegment(cur.MachineType):
		return actNoEffect, nil
	case !updateNoEffect[k] && !updateRestart[k] && k != "scheduling":
		return "", errField("resource."+k, "", "This field cannot be changed by instances.update.")
	}

	act := actNoEffect
	if updateRestart[k] {
		act = actRestart
	}

	switch k {
	case jsonDesc:
		next.Description = in.Description
	case "deletionProtection":
		next.DeletionProtection = in.DeletionProtection
	case "canIpForward":
		next.CanIpForward = in.CanIpForward
	case "displayDevice":
		next.DisplayDevice = in.DisplayDevice
	default:
		return updateComplexField(k, act, cur, next, in, raw)
	}

	return act, nil
}

func updateComplexField(
	k, act string, cur, next, in *computeapi.Instance, raw map[string]any,
) (string, *APIError) {
	switch k {
	case jsonLabels:
		return act, applyUpdateLabels(next, in, raw)
	case "metadata":
		return act, applyUpdateMetadata(cur, next, in)
	case "tags":
		return act, applyUpdateTags(cur, next, in)
	case "scheduling":
		return applyUpdateScheduling(cur, next, in)
	}

	return act, applyUpdateRestartField(k, next, in)
}

func applyUpdateLabels(next, in *computeapi.Instance, raw map[string]any) *APIError {
	req := labelsRequest{Labels: in.Labels, LabelFingerprint: next.LabelFingerprint}
	if lf, ok := raw["labelFingerprint"].(string); ok && lf != "" {
		req.LabelFingerprint = lf
	}

	labels, fp, err := applyLabels(next.Labels, next.LabelFingerprint, req)
	if err != nil {
		return err
	}

	next.Labels, next.LabelFingerprint = labels, fp

	return nil
}

func applyUpdateMetadata(cur, next, in *computeapi.Instance) *APIError {
	if in.Metadata == nil {
		return nil
	}

	if in.Metadata.Fingerprint != "" && in.Metadata.Fingerprint != cur.Metadata.Fingerprint {
		return errPrecondition("Supplied fingerprint does not match current metadata fingerprint.")
	}

	if err := validMetadata(in.Metadata); err != nil {
		return err
	}

	next.Metadata = normalizeMetadata(in.Metadata)

	return nil
}

func applyUpdateTags(cur, next, in *computeapi.Instance) *APIError {
	if in.Tags == nil {
		return nil
	}

	if in.Tags.Fingerprint != "" && in.Tags.Fingerprint != cur.Tags.Fingerprint {
		return errPrecondition("Supplied fingerprint does not match current tags fingerprint.")
	}

	if err := validTags("tags.items", in.Tags.Items); err != nil {
		return err
	}

	next.Tags = &computeapi.Tags{Items: in.Tags.Items, Fingerprint: tagsFP(in.Tags.Items)}

	return nil
}

func applyUpdateScheduling(cur, next, in *computeapi.Instance) (string, *APIError) {
	s, err := defaultScheduling(in.Scheduling)
	if err != nil {
		return "", err
	}

	next.Scheduling = s

	if schedulingNeedsStop(cur.Scheduling, s) {
		return actRestart, nil
	}

	return actNoEffect, nil
}

func applyUpdateRestartField(k string, next, in *computeapi.Instance) *APIError {
	switch k {
	case "shieldedInstanceConfig":
		if in.ShieldedInstanceConfig == nil {
			return errRequired("shieldedInstanceConfig")
		}

		next.ShieldedInstanceConfig = in.ShieldedInstanceConfig
	case "minCpuPlatform":
		if !slices.Contains(minCPUPlatforms, in.MinCpuPlatform) {
			return errField("minCpuPlatform", in.MinCpuPlatform, "Not a supported CPU platform.")
		}

		next.MinCpuPlatform = in.MinCpuPlatform
	case "serviceAccounts":
		if len(in.ServiceAccounts) > 1 {
			return errField("serviceAccounts", "", "Only one service account per instance is supported.")
		}

		var email string

		var scopes []string

		if len(in.ServiceAccounts) == 1 {
			email, scopes = in.ServiceAccounts[0].Email, in.ServiceAccounts[0].Scopes
		}

		accounts, err := validServiceAccounts(email, scopes)
		if err != nil {
			return err
		}

		next.ServiceAccounts = accounts
	}

	return nil
}

func actionRank(a string) int { return slices.Index(actionOrder, a) }
