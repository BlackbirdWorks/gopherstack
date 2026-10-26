package gcpcloudbuild

import (
	"cmp"
	"crypto/subtle"
	"maps"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var triggerNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

//nolint:gochecknoglobals // immutable catalog
var triggerImmutable = []string{
	"id",
	"resource_name",
	fieldCreateTime,
}

const shortSHALen = 7

//nolint:gochecknoglobals // immutable catalog
var revisionBuiltins = []string{
	"REPO_NAME", "BRANCH_NAME", "TAG_NAME", "REF_NAME", "COMMIT_SHA", "REVISION_ID", "SHORT_SHA",
}

func validateTrigger(t *cloudbuildpb.BuildTrigger) error {
	if t == nil {
		return errInvalid("trigger is required.")
	}

	if t.GetName() != "" && !triggerNamePattern.MatchString(t.GetName()) {
		return errInvalid(
			"Invalid trigger name %q: use 1-64 letters, digits, '-' and '_'.",
			t.GetName(),
		)
	}

	if len(t.GetTags()) > maxListItems || len(t.GetSubstitutions()) > maxSubstitution {
		return errInvalid("Invalid trigger: too many tags or substitutions.")
	}

	if err := validateSubstitutionKeys(t.GetSubstitutions()); err != nil {
		return err
	}

	if err := validateEvents(t); err != nil {
		return err
	}

	return validateTemplate(t)
}

func validateEvents(t *cloudbuildpb.BuildTrigger) error {
	n := 0

	for _, set := range []bool{
		t.GetTriggerTemplate() != nil, t.GetGithub() != nil, t.GetPubsubConfig() != nil,
		t.GetWebhookConfig() != nil, t.GetRepositoryEventConfig() != nil,
	} {
		if set {
			n++
		}
	}

	if n > 1 {
		return errInvalid(
			"Invalid trigger: at most one of trigger_template, github, pubsub_config, " +
				"webhook_config and repository_event_config may be set.",
		)
	}

	if t.GetWebhookConfig() != nil && t.GetWebhookConfig().GetSecret() == "" {
		return errInvalid("Invalid trigger: webhook_config.secret is required.")
	}

	if t.GetPubsubConfig() != nil && t.GetPubsubConfig().GetTopic() == "" {
		return errInvalid("Invalid trigger: pubsub_config.topic is required.")
	}

	if t.GetFilter() != "" && (t.GetWebhookConfig() != nil || t.GetPubsubConfig() != nil) {
		if _, err := compileFilter(t.GetFilter()); err != nil {
			return errInvalid("Invalid trigger filter %q: %v.", t.GetFilter(), err)
		}
	}

	return nil
}

func validateTemplate(t *cloudbuildpb.BuildTrigger) error {
	switch tpl := t.GetBuildTemplate().(type) {
	case *cloudbuildpb.BuildTrigger_Build:
		return validateBuild(tpl.Build)
	case *cloudbuildpb.BuildTrigger_Filename:
		if tpl.Filename == "" {
			return errInvalid("Invalid trigger: filename must not be empty.")
		}
	case *cloudbuildpb.BuildTrigger_Autodetect, *cloudbuildpb.BuildTrigger_GitFileSource:
	default:
		return errInvalid("Invalid trigger: one of build, filename, autodetect or git_file_source is required.")
	}

	return nil
}

func normalizeTrigger(t *cloudbuildpb.BuildTrigger) {
	if t.GetWebhookConfig() != nil {
		t.WebhookConfig.State = cloudbuildpb.WebhookConfig_OK
	}

	if t.GetPubsubConfig() != nil {
		t.PubsubConfig.State = cloudbuildpb.PubsubConfig_OK
	}
}

func (b *InMemoryBackend) nameTakenLocked(project, location, name, exceptID string) bool {
	if name == "" {
		return false
	}

	for _, t := range b.triggers {
		r, err := parseName(t.GetResourceName(), "triggers")
		if err == nil && r.project == project && r.location == location && t.GetName() == name &&
			t.GetId() != exceptID {
			return true
		}
	}

	return false
}

// CreateBuildTrigger stores a trigger.
func (b *InMemoryBackend) CreateBuildTrigger(
	in *cloudbuildpb.CreateBuildTriggerRequest,
) (*cloudbuildpb.BuildTrigger, error) {
	project, location, err := parseParent(in.GetParent(), in.GetProjectId())
	if err != nil {
		return nil, err
	}

	if err = validateTrigger(in.GetTrigger()); err != nil {
		return nil, err
	}

	t := cloneTrigger(in.GetTrigger())
	r := ref{project: project, location: location, id: uuid.NewString()}
	t.Id, t.ResourceName, t.CreateTime = r.id, r.name("triggers"), b.ts()

	normalizeTrigger(t)

	b.mu.Lock("CreateBuildTrigger")
	defer b.mu.Unlock()

	if len(b.triggers) >= maxTriggers {
		return nil, errPrecondition("Too many build triggers.")
	}

	if b.nameTakenLocked(project, location, t.GetName(), "") {
		return nil, errExists("Trigger with name %q already exists.", t.GetName())
	}

	b.triggers[r.key()] = t

	return cloneTrigger(t), nil
}

func (b *InMemoryBackend) triggerLocked(r ref) (*cloudbuildpb.BuildTrigger, error) {
	t, ok := b.triggers[r.key()]
	if !ok {
		return nil, errNotFound("Trigger %q not found.", r.name("triggers"))
	}

	if tr, err := parseName(t.GetResourceName(), "triggers"); err != nil ||
		tr.location != r.location {
		return nil, errNotFound("Trigger %q not found.", r.name("triggers"))
	}

	return t, nil
}

// GetBuildTrigger returns a trigger by name or by project_id and trigger_id.
func (b *InMemoryBackend) GetBuildTrigger(
	in *cloudbuildpb.GetBuildTriggerRequest,
) (*cloudbuildpb.BuildTrigger, error) {
	r, err := resolveRef(in.GetName(), in.GetProjectId(), in.GetTriggerId(), "triggers")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetBuildTrigger")
	defer b.mu.RUnlock()

	t, err := b.triggerLocked(r)
	if err != nil {
		return nil, err
	}

	return cloneTrigger(t), nil
}

// ListBuildTriggers lists the triggers of a project and location ordered by creation time.
func (b *InMemoryBackend) ListBuildTriggers(
	in *cloudbuildpb.ListBuildTriggersRequest,
) ([]*cloudbuildpb.BuildTrigger, string, error) {
	project, location, err := parseParent(in.GetParent(), in.GetProjectId())
	if err != nil {
		return nil, "", err
	}

	if in.GetPageSize() < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListBuildTriggers")

	var all []*cloudbuildpb.BuildTrigger

	for _, t := range b.triggers {
		if r, pErr := parseName(
			t.GetResourceName(),
			"triggers",
		); pErr == nil && r.project == project &&
			r.location == location {
			all = append(all, cloneTrigger(t))
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(x, y *cloudbuildpb.BuildTrigger) int {
		return cmp.Or(
			x.GetCreateTime().AsTime().Compare(y.GetCreateTime().AsTime()),
			cmp.Compare(x.GetId(), y.GetId()),
		)
	})

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

// DeleteBuildTrigger removes a trigger.
func (b *InMemoryBackend) DeleteBuildTrigger(in *cloudbuildpb.DeleteBuildTriggerRequest) error {
	r, err := resolveRef(in.GetName(), in.GetProjectId(), in.GetTriggerId(), "triggers")
	if err != nil {
		return err
	}

	b.mu.Lock("DeleteBuildTrigger")
	defer b.mu.Unlock()

	if _, err = b.triggerLocked(r); err != nil {
		return err
	}

	delete(b.triggers, r.key())

	return nil
}

// UpdateBuildTrigger replaces a trigger, or only the masked fields when update_mask is set.
func (b *InMemoryBackend) UpdateBuildTrigger(
	in *cloudbuildpb.UpdateBuildTriggerRequest,
) (*cloudbuildpb.BuildTrigger, error) {
	name := in.GetTrigger().GetResourceName()
	if in.GetTriggerId() != "" {
		name = ""
	}

	r, err := resolveRef(name, in.GetProjectId(), in.GetTriggerId(), "triggers")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateBuildTrigger")
	defer b.mu.Unlock()

	cur, err := b.triggerLocked(r)
	if err != nil {
		return nil, err
	}

	next, err := mergeTrigger(cur, in)
	if err != nil {
		return nil, err
	}

	if b.nameTakenLocked(r.project, r.location, next.GetName(), cur.GetId()) {
		return nil, errExists("Trigger with name %q already exists.", next.GetName())
	}

	b.triggers[r.key()] = next

	return cloneTrigger(next), nil
}

func mergeTrigger(
	cur *cloudbuildpb.BuildTrigger,
	in *cloudbuildpb.UpdateBuildTriggerRequest,
) (*cloudbuildpb.BuildTrigger, error) {
	if in.GetTrigger() == nil {
		return nil, errInvalid("trigger is required.")
	}

	next := cloneTrigger(cur)

	if paths := in.GetUpdateMask().GetPaths(); len(paths) > 0 {
		if err := applyMask(next, in.GetTrigger(), paths, triggerImmutable); err != nil {
			return nil, err
		}
	} else {
		next, _ = proto.Clone(in.GetTrigger()).(*cloudbuildpb.BuildTrigger)
		next.Id, next.ResourceName, next.CreateTime = cur.GetId(), cur.GetResourceName(), cur.GetCreateTime()
	}

	normalizeTrigger(next)

	if err := validateTrigger(next); err != nil {
		return nil, err
	}

	return next, nil
}

// RunBuildTrigger queues a build from a trigger's inline build template.
func (b *InMemoryBackend) RunBuildTrigger(
	in *cloudbuildpb.RunBuildTriggerRequest,
) (*longrunningpb.Operation, error) {
	r, err := resolveRef(in.GetName(), in.GetProjectId(), in.GetTriggerId(), "triggers")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("RunBuildTrigger")

	t, err := b.triggerLocked(r)
	if err != nil {
		b.mu.RUnlock()

		return nil, err
	}

	t = cloneTrigger(t)

	b.mu.RUnlock()

	return b.runTrigger(r, t, in.GetSource())
}

func (b *InMemoryBackend) runTrigger(
	r ref,
	t *cloudbuildpb.BuildTrigger,
	src *cloudbuildpb.RepoSource,
) (*longrunningpb.Operation, error) {
	tpl, ok := t.GetBuildTemplate().(*cloudbuildpb.BuildTrigger_Build)
	if !ok {
		return nil, errPrecondition("Trigger %q reads its build config from a repository file; "+
			"only triggers with an inline build template can run.", t.GetName())
	}

	build := cloneBuild(tpl.Build)
	build.Substitutions = mergeSubs(
		build.GetSubstitutions(),
		t.GetSubstitutions(),
		src.GetSubstitutions(),
	)
	if build.GetSource() == nil {
		build.Source = triggerSource(t, src)
	}

	build.Tags = append(build.GetTags(), t.GetTags()...)
	build.Tags = append(build.Tags, "trigger-"+t.GetId())

	return b.newBuild(buildInput{
		build: build, project: r.project, location: r.location, trigger: t.GetId(),
		builtin: triggerBuiltins(t, src),
	})
}

// triggerBuiltins are the substitutions Cloud Build derives from a trigger and the revision it ran at.
func triggerBuiltins(t *cloudbuildpb.BuildTrigger, src *cloudbuildpb.RepoSource) map[string]string {
	m := map[string]string{"TRIGGER_NAME": t.GetName()}

	for _, k := range revisionBuiltins {
		m[k] = ""
	}

	if src == nil {
		src = t.GetTriggerTemplate()
	}

	if src == nil {
		return m
	}

	m["REPO_NAME"] = src.GetRepoName()

	switch rev := src.GetRevision().(type) {
	case *cloudbuildpb.RepoSource_BranchName:
		m["BRANCH_NAME"], m["REF_NAME"] = rev.BranchName, rev.BranchName
	case *cloudbuildpb.RepoSource_TagName:
		m["TAG_NAME"], m["REF_NAME"] = rev.TagName, rev.TagName
	case *cloudbuildpb.RepoSource_CommitSha:
		m["COMMIT_SHA"], m["REVISION_ID"] = rev.CommitSha, rev.CommitSha
		m["SHORT_SHA"] = rev.CommitSha[:min(shortSHALen, len(rev.CommitSha))]
	}

	return m
}

// ReceiveTriggerWebhook runs a webhook trigger after checking the URL secret.
func (b *InMemoryBackend) ReceiveTriggerWebhook(
	in *cloudbuildpb.ReceiveTriggerWebhookRequest,
) error {
	r, err := resolveRef(in.GetName(), in.GetProjectId(), in.GetTrigger(), "triggers")
	if err != nil {
		return err
	}

	b.mu.RLock("ReceiveTriggerWebhook")

	t, err := b.triggerLocked(r)
	if err != nil {
		b.mu.RUnlock()

		return err
	}

	t = cloneTrigger(t)
	_, svc, _ := b.servicesLocked()

	b.mu.RUnlock()

	wh := t.GetWebhookConfig()
	if wh == nil {
		return errPrecondition("Trigger %q is not a webhook trigger.", t.GetName())
	}

	if err = checkWebhookSecret(svc.Secrets, wh.GetSecret(), in.GetSecret()); err != nil {
		return err
	}

	return b.fireEvent(r, t, in.GetBody().GetData())
}

// fireEvent starts a build for an event-driven trigger (webhook, Pub/Sub) from the event's JSON payload.
// Disabled triggers and events the trigger's filter rejects start nothing.
func (b *InMemoryBackend) fireEvent(r ref, t *cloudbuildpb.BuildTrigger, payload []byte) error {
	if t.GetDisabled() {
		return nil
	}

	if err := bindWebhook(t, payload); err != nil {
		return err
	}

	if ok, err := triggerFilterMatches(t); err != nil || !ok {
		return err
	}

	_, err := b.runTrigger(r, t, nil)

	return err
}

// triggerFilterMatches evaluates the trigger's CEL filter over its bound substitutions.
func triggerFilterMatches(t *cloudbuildpb.BuildTrigger) (bool, error) {
	if t.GetFilter() == "" {
		return true, nil
	}

	expr, err := compileFilter(t.GetFilter())
	if err != nil {
		return false, errInvalid("Invalid trigger filter: %v.", err)
	}

	vars := map[string]string{"TRIGGER_NAME": t.GetName()}
	if tpl, ok := t.GetBuildTemplate().(*cloudbuildpb.BuildTrigger_Build); ok {
		maps.Copy(vars, tpl.Build.GetSubstitutions())
	}

	maps.Copy(vars, t.GetSubstitutions())

	ok, err := evalFilter(expr, vars)
	if err != nil {
		return false, errInvalid("Trigger filter cannot be evaluated: %v.", err)
	}

	return ok, nil
}

// bindWebhook resolves payload bindings in the trigger's substitutions and its build template's.
func bindWebhook(t *cloudbuildpb.BuildTrigger, body []byte) error {
	subs, err := bindPayload(t.GetSubstitutions(), body)
	if err != nil {
		return err
	}

	t.Substitutions = subs

	if tpl, ok := t.GetBuildTemplate().(*cloudbuildpb.BuildTrigger_Build); ok && tpl.Build != nil {
		tpl.Build.Substitutions, err = bindPayload(tpl.Build.GetSubstitutions(), body)
	}

	return err
}

func checkWebhookSecret(res SecretResolver, version, got string) error {
	if res == nil {
		return errPrecondition("Secret Manager is not available to verify the webhook secret.")
	}

	want, err := res.AccessSecret(version)
	if err != nil {
		return errPrecondition("The webhook secret %q cannot be read: %v", version, err)
	}

	if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(string(want))), []byte(got)) != 1 {
		return status.Error(codes.PermissionDenied, "The webhook secret does not match.")
	}

	return nil
}

// triggerSource is the repository a trigger run builds: the request's repo and revision over the trigger's own
// trigger_template or GitHub repository.
func triggerSource(t *cloudbuildpb.BuildTrigger, src *cloudbuildpb.RepoSource) *cloudbuildpb.Source {
	if gh := t.GetGithub(); gh != nil && gh.GetOwner() != "" && gh.GetName() != "" {
		return &cloudbuildpb.Source{Source: &cloudbuildpb.Source_GitSource{GitSource: &cloudbuildpb.GitSource{
			Url: "https://github.com/" + gh.GetOwner() + "/" + gh.GetName() + ".git", Revision: repoRevision(src),
			Dir: src.GetDir(),
		}}}
	}

	repo, _ := proto.Clone(t.GetTriggerTemplate()).(*cloudbuildpb.RepoSource)
	if repo == nil {
		if src.GetRepoName() == "" {
			return nil
		}

		repo = &cloudbuildpb.RepoSource{}
	}

	if src.GetRepoName() != "" {
		repo.RepoName, repo.ProjectId = src.GetRepoName(), src.GetProjectId()
	}

	if src.GetRevision() != nil {
		repo.Revision = src.GetRevision()
	}

	if src.GetDir() != "" {
		repo.Dir = src.GetDir()
	}

	if repo.GetRepoName() == "" {
		return nil
	}

	return &cloudbuildpb.Source{Source: &cloudbuildpb.Source_RepoSource{RepoSource: repo}}
}
