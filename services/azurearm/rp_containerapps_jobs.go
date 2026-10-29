package azurearm

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	acaJobTriggerManual   = "Manual"
	acaJobTriggerSchedule = "Schedule"
	acaJobTriggerEvent    = "Event"
	acaCronFields         = 5
	acaExecStopped        = "Stopped"
	acaExecUnknown        = "Unknown"
)

type storedACAExecution struct {
	Start    time.Time      `json:"start"`
	End      *time.Time     `json:"end,omitempty"`
	Template map[string]any `json:"template"`
	Name     string         `json:"name"`
	Status   string         `json:"status,omitempty"`
	Replicas int            `json:"replicas"`
	Stopped  bool           `json:"stopped,omitempty"`
}

type storedACAJob struct {
	Created         time.Time             `json:"created"`
	Modified        time.Time             `json:"modified"`
	Tags            map[string]string     `json:"tags,omitempty"`
	Config          map[string]any        `json:"config"`
	Template        map[string]any        `json:"template"`
	ResourceGroup   string                `json:"resourceGroup"`
	Sub             string                `json:"sub"`
	Name            string                `json:"name"`
	Location        string                `json:"location"`
	EnvironmentID   string                `json:"environmentId"`
	WorkloadProfile string                `json:"workloadProfile,omitempty"`
	Identity        resourceIdentity      `json:"identity"`
	Executions      []*storedACAExecution `json:"executions,omitempty"`
}

func (j *storedACAJob) execution(name string) *storedACAExecution {
	for _, e := range j.Executions {
		if strings.EqualFold(e.Name, name) {
			return e
		}
	}

	return nil
}

func acaExecKey(j *storedACAJob, exec string, replica int) string {
	return fmt.Sprintf("%sjob/%s/%s/%s/%d", acaKeyPrefix, strings.ToLower(j.ResourceGroup), strings.ToLower(j.Name),
		strings.ToLower(exec), replica)
}

func (p *ContainerAppsProvider) executionKeys(j *storedACAJob) []string {
	var keys []string

	for _, e := range j.Executions {
		for i := range max(e.Replicas, 1) {
			keys = append(keys, acaExecKey(j, e.Name, i))
		}
	}

	return keys
}

func acaCheckJobConfig(cfg map[string]any, ident resourceIdentity) (map[string]bool, error) {
	trigger := strOf(cfg, "triggerType")
	if trigger != acaJobTriggerManual && trigger != acaJobTriggerSchedule && trigger != acaJobTriggerEvent {
		return nil, acaBad("configuration.triggerType '%s' must be Manual, Schedule or Event.", trigger)
	}

	if n, has := acaNum(cfg, "replicaTimeout"); !has || n < 1 {
		return nil, acaBad("configuration.replicaTimeout is required and must be at least 1 second.")
	}

	if n, has := acaNum(cfg, "replicaRetryLimit"); has && n < 0 {
		return nil, acaBad("configuration.replicaRetryLimit cannot be negative.")
	}

	if err := acaCheckJobTrigger(cfg, trigger); err != nil {
		return nil, err
	}

	secrets, err := acaCheckSecrets(acaList(cfg[fieldSecrets]), ident)
	if err != nil {
		return nil, err
	}

	return secrets, acaCheckRegistries(acaList(cfg[fieldRegistries]), secrets, ident)
}

func acaCheckJobTrigger(cfg map[string]any, trigger string) error {
	var tc map[string]any

	switch trigger {
	case acaJobTriggerManual:
		tc = mapOf(cfg["manualTriggerConfig"])
	case acaJobTriggerSchedule:
		tc = mapOf(cfg["scheduleTriggerConfig"])
		if len(strings.Fields(strOf(tc, "cronExpression"))) != acaCronFields {
			return acaBad("scheduleTriggerConfig.cronExpression must be a five-field cron expression.")
		}
	case acaJobTriggerEvent:
		tc = mapOf(cfg["eventTriggerConfig"])
		if tc == nil {
			return acaBad("configuration.eventTriggerConfig is required for Event jobs.")
		}
	}

	par, hasPar := acaNum(tc, "parallelism")
	done, hasDone := acaNum(tc, "replicaCompletionCount")

	switch {
	case hasPar && par < 1, hasDone && done < 1:
		return acaBad("parallelism and replicaCompletionCount must be at least 1.")
	case hasPar && hasDone && done > par:
		return acaBad("replicaCompletionCount cannot exceed parallelism.")
	}

	return nil
}

func (p *ContainerAppsProvider) putJob(id ResourceID, body map[string]any) (map[string]any, error) {
	p.opMu.Lock("putJob")
	defer p.opMu.Unlock()

	name := id.LeafName()
	if !acaNameValid(name, acaMinNameLen, acaMaxAppNameLen) {
		return nil, acaBad("Job name '%s' must be %d-%d lowercase alphanumerics or '-', start with a letter, end "+
			"alphanumeric and contain no '--'.", name, acaMinNameLen, acaMaxAppNameLen)
	}

	props := mapOf(body[fieldProperties])
	if props == nil {
		return nil, acaBad("properties are required.")
	}

	p.mu.Lock("putJob")
	defer p.mu.Unlock()

	key := acaKey(id.ResourceGroup, name)
	prev := p.jobs[key]

	env, err := p.appEnv(id, props, nil)
	if err != nil {
		return nil, err
	}

	if prev != nil && !strings.EqualFold(prev.EnvironmentID, env.armID()) {
		return nil, acaBad("The managed environment of an existing job cannot be changed.")
	}

	var prevIdent resourceIdentity
	if prev != nil {
		prevIdent = prev.Identity
	}

	_, hasIdent := body[fieldIdentity]

	ident, err := p.ids.apply(body[fieldIdentity], hasIdent, prevIdent)
	if err != nil {
		return nil, err
	}

	rec, err := p.buildJob(id, prev, body, props, env, ident)
	if err != nil {
		return nil, err
	}

	p.jobs[key] = rec

	return p.jobBody(id, rec), nil
}

func (p *ContainerAppsProvider) buildJob(
	id ResourceID, prev *storedACAJob, body, props map[string]any, env *storedACAEnv, ident resourceIdentity,
) (*storedACAJob, error) {
	cfg, tmpl := acaClone(mapOf(props[fieldConfiguration])), acaClone(mapOf(props[fieldTemplate]))
	profile := strOf(props, fieldWorkloadProf)

	if cfg == nil {
		return nil, acaBad("properties.configuration is required.")
	}

	if err := p.checkProfile(acaAppReq{env: env, profile: profile}); err != nil {
		return nil, err
	}

	secrets, err := acaCheckJobConfig(cfg, ident)
	if err != nil {
		return nil, err
	}

	if err = acaCheckTemplate(tmpl, secrets, env, profile); err != nil {
		return nil, err
	}

	now := p.now().UTC()
	rec := &storedACAJob{
		Created: now, Modified: now, Sub: id.SubscriptionID, Name: id.LeafName(), ResourceGroup: id.ResourceGroup,
		Location: DefaultLocation, Tags: stringTags(body[fieldTags]), Config: cfg, Template: tmpl,
		Identity: ident, EnvironmentID: env.armID(), WorkloadProfile: profile,
	}

	if loc := strOf(body, fieldLocation); loc != "" {
		rec.Location = loc
	}

	if prev != nil {
		rec.Created, rec.Executions, rec.Location = prev.Created, prev.Executions, prev.Location
	}

	delete(tmpl, fieldScale)
	acaNormalizeJobTemplate(tmpl)

	return rec, nil
}

func acaNormalizeJobTemplate(t map[string]any) {
	for _, key := range []string{fieldContainers, fieldInitContainer} {
		for _, e := range acaList(t[key]) {
			if c := mapOf(e); mapOf(c[fieldResources]) == nil {
				c[fieldResources] = map[string]any{"cpu": acaDefaultCPU, "memory": "1Gi"}
			}
		}
	}
}

func (p *ContainerAppsProvider) jobBody(id ResourceID, j *storedACAJob) map[string]any {
	cfg := acaClone(j.Config)

	secrets := acaPublicSecrets(acaList(cfg[fieldSecrets]))

	if len(secrets) > 0 {
		cfg[fieldSecrets] = secrets
	} else {
		delete(cfg, fieldSecrets)
	}

	props := map[string]any{
		fieldProvisioningState: provisioningStateSucceeded, "environmentId": j.EnvironmentID,
		fieldConfiguration: cfg, fieldTemplate: acaClone(j.Template), acaFieldOutboundIPs: []string{acaStaticIP},
	}

	if j.WorkloadProfile != "" {
		props[fieldWorkloadProf] = j.WorkloadProfile
	}

	jid := acaResourceID(id.SubscriptionID, j.ResourceGroup, []string{acaJobType}, []string{j.Name})
	out := map[string]any{
		"id": jid.ARMID(), fieldName: j.Name, fieldType: namespaceMicrosoftApp + "/" + acaJobType,
		fieldLocation: j.Location, fieldTags: tagsOrEmpty(j.Tags), fieldProperties: props,
		acaFieldSystemData: acaSystemData(j.Created, j.Modified),
	}

	if ident := p.ids.body(j.Identity); ident != nil {
		out[fieldIdentity] = ident
	}

	return out
}

func (p *ContainerAppsProvider) listJobs(id ResourceID) []map[string]any {
	var out []map[string]any

	for _, k := range sortedKeys(p.jobs) {
		if j := p.jobs[k]; id.ResourceGroup == "" || resourceGroupsEqual(j.ResourceGroup, id.ResourceGroup) {
			out = append(out, p.jobBody(id, j))
		}
	}

	return out
}

func (p *ContainerAppsProvider) deleteJob(id ResourceID, name string) error {
	p.opMu.Lock("deleteJob")
	defer p.opMu.Unlock()

	p.mu.Lock("deleteJob")

	key := acaKey(id.ResourceGroup, name)

	j, ok := p.jobs[key]
	if !ok {
		p.mu.Unlock()

		return ErrResourceNotFound
	}

	keys := p.executionKeys(j)

	delete(p.jobs, key)
	p.mu.Unlock()

	p.rt.removeKeys(keys)

	return nil
}

func (p *ContainerAppsProvider) patchJob(id ResourceID, patch map[string]any) (map[string]any, error) {
	p.mu.RLock("patchJob")
	j, ok := p.jobs[acaKey(id.ResourceGroup, id.LeafName())]

	var cur map[string]any

	if ok {
		props := map[string]any{
			fieldEnvironmentID: j.EnvironmentID,
			fieldConfiguration: acaClone(j.Config),
			fieldTemplate:      acaClone(j.Template),
		}
		if j.WorkloadProfile != "" {
			props[fieldWorkloadProf] = j.WorkloadProfile
		}

		cur = map[string]any{fieldLocation: j.Location, fieldTags: acaTagsAny(j.Tags), fieldProperties: props}

		if ident := p.ids.body(j.Identity); ident != nil {
			cur[fieldIdentity] = ident
		}
	}
	p.mu.RUnlock()

	if !ok {
		return nil, ErrResourceNotFound
	}

	acaMergePatch(cur, patch)

	return p.putJob(id, cur)
}

func (p *ContainerAppsProvider) listJobSecrets(id ResourceID) (map[string]any, error) {
	p.mu.RLock("listJobSecrets")
	defer p.mu.RUnlock()

	j, ok := p.jobs[acaKey(id.ResourceGroup, id.LeafName())]
	if !ok {
		return nil, ErrResourceNotFound
	}

	return map[string]any{fieldValue: acaSecretList(j.Config)}, nil
}

// acaApplyOverrides returns the job template with the per-execution container overrides applied.
func acaApplyOverrides(base, override map[string]any) (map[string]any, error) {
	out := acaClone(base)

	for _, e := range acaList(override[fieldContainers]) {
		o := mapOf(e)
		found := false

		for _, ce := range acaList(out[fieldContainers]) {
			c := mapOf(ce)
			if c[fieldName] != o[fieldName] {
				continue
			}

			found = true

			for _, k := range []string{acaFieldImage, "command", "args", "env", fieldResources} {
				if v, has := o[k]; has {
					c[k] = v
				}
			}
		}

		if !found {
			return nil, acaBad(
				"Execution override container '%s' does not exist in the job template.",
				strOf(o, fieldName),
			)
		}
	}

	return out, nil
}

// startJob creates an execution and runs its replicas; it answers once they have started or failed.
func (p *ContainerAppsProvider) startJob(id ResourceID, body map[string]any) (map[string]any, error) {
	p.opMu.Lock("startJob")
	defer p.opMu.Unlock()

	p.mu.Lock("startJob")

	j, ok := p.jobs[acaKey(id.ResourceGroup, id.LeafName())]
	if !ok {
		p.mu.Unlock()

		return nil, ErrResourceNotFound
	}

	tmpl, err := acaApplyOverrides(j.Template, body)
	if err != nil {
		p.mu.Unlock()

		return nil, err
	}

	exec := &storedACAExecution{
		Name: j.Name + "-" + awsid.NewN("", acaSuffixLen), Start: p.now().UTC(), Template: tmpl,
		Replicas: max(acaIntOf(mapOf(j.Config[acaTriggerConfigKey(j.Config)]), "parallelism", 1), 1),
	}

	props, perr := p.jobProps(j, tmpl)
	domain := ""

	if env := p.envByID(j.EnvironmentID); env != nil {
		domain = env.DefaultDomain
	}

	j.Executions = append(j.Executions, exec)
	jobCopy := *j
	p.mu.Unlock()

	for i := range exec.Replicas {
		key := acaExecKey(&jobCopy, exec.Name, i)

		if perr != nil {
			p.noteExecFailure(exec)

			break
		}

		p.rt.engine.Deploy(
			aciConfig{Key: key, Name: exec.Name, Props: jobReplicaProps(props, jobCopy.Name, exec.Name, domain)},
		)
	}

	jid := acaResourceID(id.SubscriptionID, id.ResourceGroup, []string{acaJobType, acaExecutionType},
		[]string{j.Name, exec.Name})

	return map[string]any{"id": jid.ARMID(), fieldName: exec.Name}, nil
}

func acaTriggerConfigKey(cfg map[string]any) string {
	switch strOf(cfg, "triggerType") {
	case acaJobTriggerSchedule:
		return "scheduleTriggerConfig"
	case acaJobTriggerEvent:
		return "eventTriggerConfig"
	}

	return "manualTriggerConfig"
}

func (p *ContainerAppsProvider) noteExecFailure(exec *storedACAExecution) {
	p.mu.Lock("noteExecFailure")
	defer p.mu.Unlock()

	now := p.now().UTC()
	exec.Status, exec.End = aciStateFailed, &now
}

func (p *ContainerAppsProvider) jobProps(j *storedACAJob, tmpl map[string]any) (aciPropsWire, error) {
	a := &storedACAApp{Config: j.Config}

	secrets, err := p.resolveSecrets(a)
	if err != nil {
		return aciPropsWire{}, err
	}

	env := p.envByID(j.EnvironmentID)
	if env == nil {
		return aciPropsWire{}, fmt.Errorf("managed environment %s: %w", j.EnvironmentID, ErrResourceNotFound)
	}

	vols, err := acaVolumeWires(tmpl, env, secrets)
	if err != nil {
		return aciPropsWire{}, err
	}

	props := aciPropsWire{OSType: aciOSLinux, RestartPolicy: aciRestartNever, Volumes: vols}

	for _, e := range acaList(tmpl[fieldContainers]) {
		props.Containers = append(props.Containers, acaContainerWire(mapOf(e), secrets, 0))
	}

	for _, e := range acaList(j.Config[fieldRegistries]) {
		r := mapOf(e)
		if ref := strOf(r, "passwordSecretRef"); ref != "" {
			props.ImageRegistryCredentials = append(props.ImageRegistryCredentials,
				aciCredWire{Server: strOf(r, "server"), Username: strOf(r, "username"), Password: secrets[ref]})
		}
	}

	return props, nil
}

func jobReplicaProps(props aciPropsWire, job, exec, domain string) aciPropsWire {
	out := props
	out.Containers = make([]aciContainerWire, 0, len(props.Containers))

	for _, c := range props.Containers {
		env := append([]aciEnvWire(nil), c.Properties.EnvironmentVariables...)
		env = append(env,
			aciEnvWire{Name: "CONTAINER_APP_JOB_NAME", Value: &job},
			aciEnvWire{Name: "CONTAINER_APP_JOB_EXECUTION_NAME", Value: &exec},
			aciEnvWire{Name: "CONTAINER_APP_ENV_DNS_SUFFIX", Value: &domain})
		c.Properties.EnvironmentVariables = env
		out.Containers = append(out.Containers, c)
	}

	return out
}

// execStatus derives an execution's state from its replicas.
func (p *ContainerAppsProvider) execStatus(j *storedACAJob, e *storedACAExecution) (string, *time.Time) {
	if e.Stopped {
		return acaExecStopped, e.End
	}

	r := p.replicaStates(j, e)

	switch {
	case r.seen == 0 && e.Status != "":
		return e.Status, e.End
	case r.seen == 0:
		return acaExecUnknown, nil
	case r.failed:
		return aciStateFailed, r.end
	case r.running:
		return aciStateRunning, nil
	}

	return aciStateSucceeded, r.end
}

type acaReplicaStates struct {
	end     *time.Time
	seen    int
	running bool
	failed  bool
}

func (p *ContainerAppsProvider) replicaStates(j *storedACAJob, e *storedACAExecution) acaReplicaStates {
	var r acaReplicaStates

	for i := range max(e.Replicas, 1) {
		view, live := p.rt.engine.View(acaExecKey(j, e.Name, i))
		if !live {
			continue
		}

		r.seen++

		switch view.State {
		case aciStateFailed:
			r.failed = true
		case aciStateSucceeded:
		default:
			r.running = true
		}

		for _, c := range view.Containers {
			if c.Cur.FinishTime != nil && (r.end == nil || c.Cur.FinishTime.After(*r.end)) {
				r.end = c.Cur.FinishTime
			}
		}
	}

	return r
}

func (p *ContainerAppsProvider) executionBody(id ResourceID, j *storedACAJob, e *storedACAExecution) map[string]any {
	status, end := p.execStatus(j, e)

	props := map[string]any{acaFieldStatus: status, "startTime": e.Start.Format(time.RFC3339Nano)}
	if end != nil {
		props["endTime"] = end.Format(time.RFC3339Nano)
	}

	props[fieldTemplate] = map[string]any{fieldContainers: acaExecContainers(e.Template)}

	eid := acaResourceID(id.SubscriptionID, j.ResourceGroup, []string{acaJobType, acaExecutionType},
		[]string{j.Name, e.Name})

	return map[string]any{
		"id":            eid.ARMID(),
		fieldName:       e.Name,
		fieldType:       namespaceMicrosoftApp + "/" + acaJobType + "/" + acaExecutionType,
		fieldProperties: props,
	}
}

func acaExecContainers(t map[string]any) []any {
	list := acaList(t[fieldContainers])
	out := make([]any, 0, len(list))

	for _, e := range list {
		c := mapOf(e)
		item := map[string]any{fieldName: c[fieldName], acaFieldImage: c[acaFieldImage]}

		for _, k := range []string{"command", "args", "env", fieldResources} {
			if v, has := c[k]; has {
				item[k] = v
			}
		}

		out = append(out, item)
	}

	return out
}

func (p *ContainerAppsProvider) executionGet(id ResourceID, t acaTarget) (map[string]any, error) {
	j, ok := p.jobs[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	e := j.execution(t.names[1])
	if e == nil {
		return nil, ErrResourceNotFound
	}

	return p.executionBody(id, j, e), nil
}

func (p *ContainerAppsProvider) listExecutions(id ResourceID, t acaTarget) ([]map[string]any, error) {
	j, ok := p.jobs[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	out := make([]map[string]any, 0, len(j.Executions))
	for _, v := range slices.Backward(j.Executions) {
		out = append(out, p.executionBody(id, j, v))
	}

	return out, nil
}

// stopExecutions stops one execution, or every execution when name is empty, and returns the stopped ones.
func (p *ContainerAppsProvider) stopExecutions(
	_ context.Context, id ResourceID, name string,
) ([]map[string]any, error) {
	p.opMu.Lock("stopExecutions")
	defer p.opMu.Unlock()

	p.mu.Lock("stopExecutions")

	j, ok := p.jobs[acaKey(id.ResourceGroup, id.LeafName())]
	if !ok {
		p.mu.Unlock()

		return nil, ErrResourceNotFound
	}

	var stopped []map[string]any

	var keys []string

	for _, e := range j.Executions {
		if name != "" && !strings.EqualFold(e.Name, name) {
			continue
		}

		if status, _ := p.execStatus(j, e); status != aciStateRunning {
			continue
		}

		now := p.now().UTC()
		e.Stopped, e.End = true, &now

		for i := range max(e.Replicas, 1) {
			keys = append(keys, acaExecKey(j, e.Name, i))
		}

		eid := acaResourceID(id.SubscriptionID, j.ResourceGroup, []string{acaJobType, acaExecutionType},
			[]string{j.Name, e.Name})
		stopped = append(stopped, map[string]any{"id": eid.ARMID(), fieldName: e.Name})
	}

	if name != "" && j.execution(name) == nil {
		p.mu.Unlock()

		return nil, ErrResourceNotFound
	}
	p.mu.Unlock()

	for _, k := range keys {
		p.rt.engine.Stop(k)
	}

	return stopped, nil
}
