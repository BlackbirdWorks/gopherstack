package azurearm

import (
	"context"
	"fmt"
	"reflect"
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

	acaDefaultMaxExecutions = 100
	acaJobHistoryLimit      = 100
	acaDefaultJobPolling    = 30
)

type storedACAExecution struct {
	Start      time.Time      `json:"start"`
	End        *time.Time     `json:"end,omitempty"`
	Template   map[string]any `json:"template"`
	Name       string         `json:"name"`
	Status     string         `json:"status,omitempty"`
	Source     string         `json:"source,omitempty"`
	Attempts   []int          `json:"attempts,omitempty"`
	Replicas   int            `json:"replicas"`
	Need       int            `json:"need,omitempty"`
	RetryLimit int            `json:"retryLimit,omitempty"`
	TimeoutSec int            `json:"timeoutSec,omitempty"`
	Stopped    bool           `json:"stopped,omitempty"`
	Done       bool           `json:"done,omitempty"`
}

type storedACAJob struct {
	Created         time.Time             `json:"created"`
	Modified        time.Time             `json:"modified"`
	LastFire        time.Time             `json:"lastFire,omitempty"`
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
		if _, err := parseACACron(strOf(tc, "cronExpression")); err != nil {
			return acaBad("scheduleTriggerConfig.cronExpression '%s' is not a valid five-field cron expression: %v.",
				strOf(tc, "cronExpression"), err)
		}
	case acaJobTriggerEvent:
		tc = mapOf(cfg["eventTriggerConfig"])
		if tc == nil {
			return acaBad("configuration.eventTriggerConfig is required for Event jobs.")
		}

		if err := acaCheckJobScale(mapOf(tc[fieldScale])); err != nil {
			return err
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

func acaCheckJobScale(sc map[string]any) error {
	minE, hasMin := acaNum(sc, "minExecutions")
	maxE, hasMax := acaNum(sc, "maxExecutions")
	poll, hasPoll := acaNum(sc, "pollingInterval")

	switch {
	case hasMin && minE < 0:
		return acaBad("eventTriggerConfig.scale.minExecutions cannot be negative.")
	case hasMax && maxE < 1:
		return acaBad("eventTriggerConfig.scale.maxExecutions must be at least 1.")
	case hasMin && minE > acaJobMaxExecutions(sc):
		return acaBad("eventTriggerConfig.scale.minExecutions cannot exceed maxExecutions.")
	case hasPoll && poll < 1:
		return acaBad("eventTriggerConfig.scale.pollingInterval must be at least 1 second.")
	}

	names := map[string]bool{}

	for _, e := range acaList(sc["rules"]) {
		r := mapOf(e)
		name := strOf(r, fieldName)

		if name == "" || names[name] || strOf(r, fieldType) == "" {
			return acaBad("Event job scale rules need a unique name and a type.")
		}

		names[name] = true
	}

	return nil
}

func acaJobMaxExecutions(sc map[string]any) float64 {
	if n, ok := acaNum(sc, "maxExecutions"); ok {
		return n
	}

	return acaDefaultMaxExecutions
}

func (p *ContainerAppsProvider) putJob(id ResourceID, body map[string]any) (map[string]any, error) {
	p.opMu.Lock("putJob")
	defer p.opMu.Unlock()
	defer p.armScheduler()

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

	rec.LastFire = now

	if prev != nil {
		rec.Created, rec.Executions, rec.Location = prev.Created, prev.Executions, prev.Location

		if reflect.DeepEqual(prev.Config[acaTriggerConfigKey(prev.Config)], cfg[acaTriggerConfigKey(cfg)]) {
			rec.LastFire = prev.LastFire
		}
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
	defer p.armScheduler()

	p.mu.Lock("deleteJob")

	key := acaKey(id.ResourceGroup, name)

	j, ok := p.jobs[key]
	if !ok {
		p.mu.Unlock()

		return ErrResourceNotFound
	}

	keys := p.executionKeys(j)

	delete(p.jobs, key)
	delete(p.lastPoll, key)
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

// startJob creates a manual execution and answers once its replicas have started or failed.
func (p *ContainerAppsProvider) startJob(id ResourceID, body map[string]any) (map[string]any, error) {
	name, err := p.startExecution(id.ResourceGroup, id.LeafName(), body, acaJobTriggerManual)
	if err != nil {
		return nil, err
	}

	jid := acaResourceID(id.SubscriptionID, id.ResourceGroup, []string{acaJobType, acaExecutionType},
		[]string{id.LeafName(), name})

	return map[string]any{"id": jid.ARMID(), fieldName: name}, nil
}

// startExecution creates an execution of the job with the given override and deploys its replicas.
func (p *ContainerAppsProvider) startExecution(rg, jobName string, override map[string]any, source string) (string, error) {
	p.opMu.Lock("startExecution")
	defer p.opMu.Unlock()

	p.mu.Lock("startExecution")

	j, ok := p.jobs[acaKey(rg, jobName)]
	if !ok {
		p.mu.Unlock()

		return "", ErrResourceNotFound
	}

	tmpl, err := acaApplyOverrides(j.Template, override)
	if err != nil {
		p.mu.Unlock()

		return "", err
	}

	tc := mapOf(j.Config[acaTriggerConfigKey(j.Config)])
	replicas := max(acaIntOf(tc, "parallelism", 1), 1)

	exec := &storedACAExecution{
		Name: j.Name + "-" + awsid.NewN("", acaSuffixLen), Start: p.now().UTC(), Template: tmpl, Source: source,
		Replicas: replicas, Need: min(max(acaIntOf(tc, "replicaCompletionCount", 1), 1), replicas),
		Attempts: make([]int, replicas), RetryLimit: acaIntOf(j.Config, "replicaRetryLimit", 0),
		TimeoutSec: acaIntOf(j.Config, "replicaTimeout", 0),
	}

	props, perr := p.jobProps(j, tmpl)
	domain := ""

	if env := p.envByID(j.EnvironmentID); env != nil {
		domain = env.DefaultDomain
	}

	j.Executions = append(j.Executions, exec)
	pruned := p.pruneExecutions(j)
	jobCopy := *j
	p.mu.Unlock()

	p.rt.removeKeys(pruned)

	for i := range exec.Replicas {
		if perr != nil {
			p.noteExecFailure(exec)

			break
		}

		p.rt.engine.Deploy(aciConfig{
			Key: acaExecKey(&jobCopy, exec.Name, i), Name: exec.Name,
			Props: jobReplicaProps(props, jobCopy.Name, exec.Name, domain),
		})
	}

	p.armScheduler()

	return exec.Name, nil
}

// pruneExecutions drops the oldest finished executions of a scheduled or event job beyond the 100 the service keeps,
// returning the engine keys of their replicas. The caller holds mu.
func (p *ContainerAppsProvider) pruneExecutions(j *storedACAJob) []string {
	if strOf(j.Config, "triggerType") == acaJobTriggerManual {
		return nil
	}

	done := 0

	for _, e := range j.Executions {
		if e.Done || e.Stopped {
			done++
		}
	}

	var keys []string

	for done > acaJobHistoryLimit {
		i := slices.IndexFunc(j.Executions, func(e *storedACAExecution) bool { return e.Done || e.Stopped })
		for r := range max(j.Executions[i].Replicas, 1) {
			keys = append(keys, acaExecKey(j, j.Executions[i].Name, r))
		}

		j.Executions = slices.Delete(j.Executions, i, i+1)
		done--
	}

	return keys
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
	exec.Status, exec.End, exec.Done = aciStateFailed, &now, true
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

type acaVerdict struct {
	end    *time.Time
	status string
	retry  []int
	final  bool
}

// evaluate derives an execution's state from its replicas: failed replicas are retried up to replicaRetryLimit,
// the execution succeeds once replicaCompletionCount replicas succeed, fails when a replica runs out of retries or
// replicaTimeout passes, and is Running otherwise.
func (p *ContainerAppsProvider) evaluate(j *storedACAJob, e *storedACAExecution, now time.Time) acaVerdict {
	if e.Stopped {
		return acaVerdict{status: acaExecStopped, end: e.End, final: true}
	}

	if e.Done {
		return acaVerdict{status: e.Status, end: e.End, final: true}
	}

	var (
		v                          acaVerdict
		seen, running, failed, win int
		last                       *time.Time
	)

	for i := range max(e.Replicas, 1) {
		view, live := p.rt.engine.View(acaExecKey(j, e.Name, i))
		if !live {
			continue
		}

		seen++

		switch view.State {
		case aciStateFailed:
			if i < len(e.Attempts) && e.Attempts[i] < e.RetryLimit {
				v.retry = append(v.retry, i)
				running++
			} else {
				failed++
			}
		case aciStateSucceeded:
			win++
		default:
			running++
		}

		for _, c := range view.Containers {
			if c.Cur.FinishTime != nil && (last == nil || c.Cur.FinishTime.After(*last)) {
				last = c.Cur.FinishTime
			}
		}
	}

	finish := func(status string) acaVerdict {
		end := last
		if end == nil || status == aciStateFailed && running > 0 {
			t := now
			end = &t
		}

		return acaVerdict{status: status, end: end, final: true}
	}

	switch {
	case seen == 0 && e.Status != "":
		return acaVerdict{status: e.Status, end: e.End}
	case seen == 0:
		return acaVerdict{status: acaExecUnknown}
	case running > 0 && e.TimeoutSec > 0 && now.Sub(e.Start) >= time.Duration(e.TimeoutSec)*time.Second:
		return finish(aciStateFailed)
	case failed > 0:
		return finish(aciStateFailed)
	case win >= max(e.Need, 1) || running == 0:
		return finish(aciStateSucceeded)
	}

	v.status = aciStateRunning

	return v
}

// execStatus is the state and end time evaluate reports.
func (p *ContainerAppsProvider) execStatus(j *storedACAJob, e *storedACAExecution) (string, *time.Time) {
	v := p.evaluate(j, e, p.now().UTC())

	if v.status == aciStateRunning {
		return v.status, nil
	}

	return v.status, v.end
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
