package gcpscheduler

import (
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultTimeZone     = "Etc/UTC"
	defaultMinBackoff   = 5 * time.Second
	defaultMaxBackoff   = time.Hour
	defaultMaxDoublings = 5
	maxRetryCount       = 5
	defaultDeadline     = 3 * time.Minute
	minDeadline         = 15 * time.Second
	maxHTTPDeadline     = 30 * time.Minute
	maxAEDeadline       = 24 * time.Hour
	maxBodyBytes        = 1 << 20
	maxDescription      = 500
	autoJobBase         = 1_000_000_000_000_000_000
)

func mutableJobFields() []string {
	return []string{
		"description", "schedule", "time_zone", "retry_config", "attempt_deadline",
		"http_target", "app_engine_http_target", "pubsub_target",
	}
}

func cloneJob(j *schedulerpb.Job) *schedulerpb.Job {
	out, _ := proto.Clone(j).(*schedulerpb.Job)

	return out
}

func tsOf(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t.UTC()) }

func errJobMissing() error { return errNotFound("Job not found.") }

func bodyAllowed(m schedulerpb.HttpMethod) bool {
	return m != schedulerpb.HttpMethod_GET && m != schedulerpb.HttpMethod_HEAD && m != schedulerpb.HttpMethod_OPTIONS
}

func validateHTTPTarget(t *schedulerpb.HttpTarget) error {
	u, err := url.Parse(t.GetUri())
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errInvalid("HttpTarget uri %q must be an absolute http or https URL.", t.GetUri())
	}

	if len(t.GetBody()) > maxBodyBytes {
		return errInvalid("HttpTarget body exceeds the 1 MiB limit.")
	}

	if len(t.GetBody()) > 0 && !bodyAllowed(t.GetHttpMethod()) {
		return errInvalid("HTTP method %s cannot have a body.", t.GetHttpMethod())
	}

	if t.GetOauthToken() != nil && t.GetOauthToken().GetServiceAccountEmail() == "" {
		return errInvalid("oauth_token requires service_account_email.")
	}

	if t.GetOidcToken() != nil && t.GetOidcToken().GetServiceAccountEmail() == "" {
		return errInvalid("oidc_token requires service_account_email.")
	}

	return nil
}

func validateAppEngineTarget(t *schedulerpb.AppEngineHttpTarget) error {
	if u := t.GetRelativeUri(); u != "" && !strings.HasPrefix(u, "/") {
		return errInvalid("relative_uri %q must begin with '/'.", u)
	}

	if len(t.GetBody()) > maxBodyBytes {
		return errInvalid("AppEngineHttpTarget body exceeds the 1 MiB limit.")
	}

	if len(t.GetBody()) > 0 && !bodyAllowed(t.GetHttpMethod()) {
		return errInvalid("HTTP method %s cannot have a body.", t.GetHttpMethod())
	}

	return nil
}

func validatePubsubTarget(t *schedulerpb.PubsubTarget) error {
	p := strings.Split(t.GetTopicName(), "/")
	if len(p) != 4 || p[0] != projectsPrefix || p[2] != "topics" || p[1] == "" ||
		p[3] == "" {
		return errInvalid("topic_name %q must be of the form projects/{project}/topics/{topic}.", t.GetTopicName())
	}

	if len(t.GetData()) == 0 && len(t.GetAttributes()) == 0 {
		return errInvalid("Pub/Sub message must contain either non-empty data or at least one attribute.")
	}

	return nil
}

func validateTarget(j *schedulerpb.Job) (time.Duration, error) {
	switch t := j.GetTarget().(type) {
	case *schedulerpb.Job_HttpTarget:
		return maxHTTPDeadline, validateHTTPTarget(t.HttpTarget)
	case *schedulerpb.Job_AppEngineHttpTarget:
		return maxAEDeadline, validateAppEngineTarget(t.AppEngineHttpTarget)
	case *schedulerpb.Job_PubsubTarget:
		return maxHTTPDeadline, validatePubsubTarget(t.PubsubTarget)
	}

	return 0, errInvalid("Job must have exactly one of http_target, app_engine_http_target or pubsub_target.")
}

// defaultRetry fills unset retry settings; zero means unset for every field.
func defaultRetry(r *schedulerpb.RetryConfig) (*schedulerpb.RetryConfig, error) {
	if r == nil {
		r = &schedulerpb.RetryConfig{}
	}

	if r.GetRetryCount() < 0 || r.GetRetryCount() > maxRetryCount {
		return nil, errInvalid("retry_count must be between 0 and %d.", maxRetryCount)
	}

	if r.GetMaxDoublings() < 0 {
		return nil, errInvalid("max_doublings must not be negative.")
	}

	durations := []*durationpb.Duration{r.GetMaxRetryDuration(), r.GetMinBackoffDuration(), r.GetMaxBackoffDuration()}
	for _, d := range durations {
		if d != nil && d.AsDuration() < 0 {
			return nil, errInvalid("retry durations must not be negative.")
		}
	}

	out := &schedulerpb.RetryConfig{
		RetryCount:         r.GetRetryCount(),
		MaxRetryDuration:   r.GetMaxRetryDuration(),
		MinBackoffDuration: r.GetMinBackoffDuration(),
		MaxBackoffDuration: r.GetMaxBackoffDuration(),
		MaxDoublings:       r.GetMaxDoublings(),
	}

	if out.GetMaxRetryDuration() == nil {
		out.MaxRetryDuration = durationpb.New(0)
	}

	if out.GetMinBackoffDuration().AsDuration() == 0 {
		out.MinBackoffDuration = durationpb.New(defaultMinBackoff)
	}

	if out.GetMaxBackoffDuration().AsDuration() == 0 {
		out.MaxBackoffDuration = durationpb.New(defaultMaxBackoff)
	}

	if out.GetMaxDoublings() == 0 {
		out.MaxDoublings = defaultMaxDoublings
	}

	if out.GetMinBackoffDuration().AsDuration() > out.GetMaxBackoffDuration().AsDuration() {
		return nil, errInvalid("min_backoff_duration must not exceed max_backoff_duration.")
	}

	return out, nil
}

// prepare validates j and fills its mutable-field defaults, returning the parsed schedule and location.
func prepare(j *schedulerpb.Job) (schedule, *time.Location, error) {
	if len(j.GetDescription()) > maxDescription {
		return nil, nil, errInvalid("description must be at most %d characters.", maxDescription)
	}

	maxDeadline, err := validateTarget(j)
	if err != nil {
		return nil, nil, err
	}

	if j.GetTimeZone() == "" {
		j.TimeZone = defaultTimeZone
	}

	loc, err := time.LoadLocation(j.GetTimeZone())
	if err != nil {
		return nil, nil, errInvalid("Invalid time_zone %q: not an IANA time zone.", j.GetTimeZone())
	}

	if j.GetSchedule() == "" {
		return nil, nil, errInvalid("schedule must be specified.")
	}

	sched, err := parseCron(j.GetSchedule())
	if err != nil {
		return nil, nil, errInvalid("Invalid schedule: %v. Use unix-cron or the App Engine cron format.", err)
	}

	if d := j.GetAttemptDeadline(); d != nil && (d.AsDuration() < minDeadline || d.AsDuration() > maxDeadline) {
		return nil, nil, errInvalid("attempt_deadline must be between %s and %s.", minDeadline, maxDeadline)
	}

	if j.RetryConfig, err = defaultRetry(j.GetRetryConfig()); err != nil {
		return nil, nil, err
	}

	return sched, loc, nil
}

func (b *InMemoryBackend) nextFireLocked(r *jobRec, now time.Time) *timestamppb.Timestamp {
	if n := r.sched.next(now, r.loc); !n.IsZero() {
		return tsOf(n)
	}

	return nil
}

// CreateJob creates a job under a location.
func (b *InMemoryBackend) CreateJob(req *schedulerpb.CreateJobRequest) (*schedulerpb.Job, error) {
	if err := parseLocation(req.GetParent()); err != nil {
		return nil, err
	}

	if req.GetJob() == nil {
		return nil, errInvalid("job must be specified.")
	}

	in := cloneJob(req.GetJob())
	in.State, in.Status, in.ScheduleTime, in.LastAttemptTime, in.UserUpdateTime = 0, nil, nil, nil, nil

	if in.GetName() != "" {
		loc, err := parseJobName(in.GetName())
		if err != nil {
			return nil, err
		}

		if loc != req.GetParent() {
			return nil, errInvalid("Job name %q must be under parent %q.", in.GetName(), req.GetParent())
		}
	}

	sched, loc, err := prepare(in)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()

	if in.GetName() == "" {
		b.jobSeq++
		in.Name = req.GetParent() + "/jobs/" + strconv.FormatUint(autoJobBase+b.jobSeq, 10)
	}

	if _, ok := b.jobs[in.GetName()]; ok {
		return nil, errExists("Job %s already exists.", in.GetName())
	}

	now := b.now()
	in.State = schedulerpb.Job_ENABLED
	in.UserUpdateTime = tsOf(now)

	rec := &jobRec{pb: in, loc: loc, sched: sched}
	in.ScheduleTime = b.nextFireLocked(rec, now)
	b.jobs[in.GetName()] = rec

	return cloneJob(in), nil
}

// GetJob returns a job.
func (b *InMemoryBackend) GetJob(name string) (*schedulerpb.Job, error) {
	if _, err := parseJobName(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetJob")
	defer b.mu.RUnlock()

	r, ok := b.jobs[name]
	if !ok {
		return nil, errJobMissing()
	}

	return cloneJob(r.pb), nil
}

// ListJobs lists the jobs of a location ordered by name.
func (b *InMemoryBackend) ListJobs(req *schedulerpb.ListJobsRequest) ([]*schedulerpb.Job, string, error) {
	if err := parseLocation(req.GetParent()); err != nil {
		return nil, "", err
	}

	if req.GetPageSize() < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	prefix := req.GetParent() + "/jobs/"
	names := make([]string, 0, len(b.jobs))

	for n := range b.jobs {
		if strings.HasPrefix(n, prefix) {
			names = append(names, n)
		}
	}

	slices.Sort(names)

	all := make([]*schedulerpb.Job, 0, len(names))
	for _, n := range names {
		all = append(all, cloneJob(b.jobs[n].pb))
	}

	pg := page.New(all, req.GetPageToken(), min(int(req.GetPageSize()), maxPageSize), defaultPageSize)

	return pg.Data, pg.Next, nil
}

// copyPath copies the field at path from src into dst (clearing it when src leaves it unset).
func copyPath(dst, src protoreflect.Message, parts []string) error {
	fd := dst.Descriptor().Fields().ByName(protoreflect.Name(parts[0]))
	if fd == nil {
		return errInvalid("Unknown field %q in update_mask.", parts[0])
	}

	if len(parts) == 1 {
		if src.Has(fd) {
			dst.Set(fd, src.Get(fd))
		} else {
			dst.Clear(fd)
		}

		return nil
	}

	if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
		return errInvalid("Field %q in update_mask has no sub-fields.", parts[0])
	}

	return copyPath(dst.Mutable(fd).Message(), src.Get(fd).Message(), parts[1:])
}

func applyJobPath(dst, src *schedulerpb.Job, path string) error {
	parts := strings.Split(path, ".")
	if !slices.Contains(mutableJobFields(), parts[0]) {
		return errInvalid("Field %q cannot be updated.", path)
	}

	return copyPath(dst.ProtoReflect(), src.ProtoReflect(), parts)
}

// UpdateJob updates the masked fields of a job (all mutable fields when the mask is empty).
func (b *InMemoryBackend) UpdateJob(req *schedulerpb.UpdateJobRequest) (*schedulerpb.Job, error) {
	if req.GetJob() == nil {
		return nil, errInvalid("job must be specified.")
	}

	if _, err := parseJobName(req.GetJob().GetName()); err != nil {
		return nil, err
	}

	paths := req.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		paths = mutableJobFields()
	}

	b.mu.Lock("UpdateJob")
	defer b.mu.Unlock()

	r, ok := b.jobs[req.GetJob().GetName()]
	if !ok {
		return nil, errJobMissing()
	}

	next := cloneJob(r.pb)
	for _, p := range paths {
		if err := applyJobPath(next, req.GetJob(), p); err != nil {
			return nil, err
		}
	}

	sched, loc, err := prepare(next)
	if err != nil {
		return nil, err
	}

	now := b.now()
	next.UserUpdateTime = tsOf(now)
	r.pb, r.sched, r.loc = next, sched, loc

	if next.GetState() == schedulerpb.Job_ENABLED {
		next.ScheduleTime = b.nextFireLocked(r, now)
	}

	return cloneJob(next), nil
}

// DeleteJob removes a job; in-flight attempts finish but their outcome is discarded.
func (b *InMemoryBackend) DeleteJob(name string) error {
	if _, err := parseJobName(name); err != nil {
		return err
	}

	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()

	if _, ok := b.jobs[name]; !ok {
		return errJobMissing()
	}

	delete(b.jobs, name)

	return nil
}

// PauseJob stops an ENABLED job from firing.
func (b *InMemoryBackend) PauseJob(name string) (*schedulerpb.Job, error) {
	if _, err := parseJobName(name); err != nil {
		return nil, err
	}

	b.mu.Lock("PauseJob")
	defer b.mu.Unlock()

	r, ok := b.jobs[name]
	if !ok {
		return nil, errJobMissing()
	}

	if r.pb.GetState() != schedulerpb.Job_ENABLED {
		return nil, errPrecondition("Job must be ENABLED to be paused; it is %s.", r.pb.GetState())
	}

	r.pb.State = schedulerpb.Job_PAUSED
	r.pb.ScheduleTime = nil
	r.retryAt, r.failures = time.Time{}, 0

	return cloneJob(r.pb), nil
}

// ResumeJob re-enables a PAUSED job, scheduling its next fire time from now.
func (b *InMemoryBackend) ResumeJob(name string) (*schedulerpb.Job, error) {
	if _, err := parseJobName(name); err != nil {
		return nil, err
	}

	b.mu.Lock("ResumeJob")
	defer b.mu.Unlock()

	r, ok := b.jobs[name]
	if !ok {
		return nil, errJobMissing()
	}

	if r.pb.GetState() == schedulerpb.Job_ENABLED {
		return cloneJob(r.pb), nil
	}

	if r.pb.GetState() != schedulerpb.Job_PAUSED {
		return nil, errPrecondition("Job must be PAUSED to be resumed; it is %s.", r.pb.GetState())
	}

	r.pb.State = schedulerpb.Job_ENABLED
	r.pb.ScheduleTime = b.nextFireLocked(r, b.now())

	return cloneJob(r.pb), nil
}
