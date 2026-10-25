package gcpworkflows

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

const (
	defaultWorkflowPage = 500
	maxWorkflowPage     = 1000
	defaultRevisionPage = 20
	maxRevisionPage     = 100
	maskAll             = "*"
	maskDescription     = "description"
	maskLabels          = "labels"
	maskAccount         = "service_account"
	maskSource          = "source_contents"
	maskCryptoKey       = "crypto_key_name"
	maskCallLog         = "call_log_level"
	maskEnv             = "user_env_vars"
	maskHistory         = "execution_history_level"
	revisionRandBytes   = 2
	orderParts          = 2
)

func newRevisionID(n int) string {
	raw := make([]byte, revisionRandBytes)
	_, _ = rand.Read(raw)

	return fmt.Sprintf("%06d-%s", n, hex.EncodeToString(raw)[:revisionTail])
}

func validateWorkflow(w *workflowspb.Workflow) error {
	if w.GetSourceContents() == "" {
		return errInvalid("source_contents is required.")
	}

	if len([]rune(w.GetDescription())) > maxDescription {
		return errInvalid("description must be at most %d characters.", maxDescription)
	}

	if sa := w.GetServiceAccount(); sa != "" && !accountRe.MatchString(sa) {
		return errInvalid("service_account %q is not a valid service account.", sa)
	}

	if k := w.GetCryptoKeyName(); k != "" && !cryptoKeyRe.MatchString(k) {
		return errInvalid("crypto_key_name %q is not a valid Cloud KMS key.", k)
	}

	if err := validateEnv(w.GetUserEnvVars()); err != nil {
		return err
	}

	return validateLabels(w.GetLabels())
}

func validateEnv(env map[string]string) error {
	if len(env) > maxEnvVars {
		return errInvalid("user_env_vars can hold at most %d entries.", maxEnvVars)
	}

	for k, v := range env {
		if k == "" || strings.HasPrefix(k, "GOOGLE") || strings.HasPrefix(k, "WORKFLOWS") {
			return errInvalid("user_env_vars key %q is empty or starts with GOOGLE or WORKFLOWS.", k)
		}

		if len(v) > maxEnvValue {
			return errInvalid("user_env_vars value for %q is longer than %d bytes.", k, maxEnvValue)
		}
	}

	return nil
}

func (b *InMemoryBackend) compile(source string) (*wfinterp.Program, error) {
	p, err := b.engine.Compile(source)
	if err != nil {
		return nil, fmt.Errorf("failed to build: %w", err)
	}

	return p, nil
}

// CreateWorkflow validates and compiles the source, then stores the workflow; the operation is already done.
func (b *InMemoryBackend) CreateWorkflow(in *workflowspb.CreateWorkflowRequest) (*longrunningpb.Operation, error) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if !knownLocation(location) {
		return nil, errInvalid("Location %q is not supported by Workflows.", location)
	}

	if !workflowIDRe.MatchString(in.GetWorkflowId()) {
		return nil, errInvalid("Invalid workflow id %q: use 1-64 letters, digits, underscores and hyphens, "+
			"starting with a letter and ending with a letter or digit.", in.GetWorkflowId())
	}

	if in.GetWorkflow() == nil {
		return nil, errInvalid("workflow is required.")
	}

	if err = validateWorkflow(in.GetWorkflow()); err != nil {
		return nil, err
	}

	name := workflowName(project, location, in.GetWorkflowId())

	b.mu.RLock("CreateWorkflow")
	_, exists := b.workflows[name]
	b.mu.RUnlock()

	if exists {
		return nil, errExists("Workflow %q already exists.", name)
	}

	prog, cerr := b.compile(in.GetWorkflow().GetSourceContents())
	if cerr != nil {
		return b.failed(project, location, name, "create", codes.InvalidArgument, cerr.Error())
	}

	b.mu.Lock("CreateWorkflow")
	defer b.mu.Unlock()

	if _, dup := b.workflows[name]; dup {
		return nil, errExists("Workflow %q already exists.", name)
	}

	w := b.buildWorkflow(project, name, in.GetWorkflow())
	b.workflows[name] = &workflowRec{
		revs:  []*workflowspb.Workflow{w},
		progs: map[string]*wfinterp.Program{w.GetRevisionId(): prog},
	}

	return b.finish(project, location, name, "create", clone(w))
}

func (b *InMemoryBackend) buildWorkflow(project, name string, in *workflowspb.Workflow) *workflowspb.Workflow {
	now := b.ts()

	return &workflowspb.Workflow{
		Name:                  name,
		Description:           in.GetDescription(),
		State:                 workflowspb.Workflow_ACTIVE,
		RevisionId:            newRevisionID(1),
		CreateTime:            now,
		UpdateTime:            now,
		RevisionCreateTime:    now,
		Labels:                in.GetLabels(),
		ServiceAccount:        normalizeAccount(project, in.GetServiceAccount()),
		SourceCode:            &workflowspb.Workflow_SourceContents{SourceContents: in.GetSourceContents()},
		CryptoKeyName:         in.GetCryptoKeyName(),
		CallLogLevel:          in.GetCallLogLevel(),
		UserEnvVars:           in.GetUserEnvVars(),
		ExecutionHistoryLevel: in.GetExecutionHistoryLevel(),
		Tags:                  in.GetTags(),
	}
}

func (r *workflowRec) latest() *workflowspb.Workflow { return r.revs[len(r.revs)-1] }

// forRevision returns the revision with the workflow-wide fields of the latest one.
func (r *workflowRec) forRevision(rev *workflowspb.Workflow) *workflowspb.Workflow {
	out := clone(rev)
	cur := r.latest()
	out.Description, out.Labels = cur.GetDescription(), cur.GetLabels()
	out.CreateTime, out.UpdateTime = cur.GetCreateTime(), cur.GetUpdateTime()

	return out
}

// GetWorkflow returns the latest revision of a workflow, or the revision named by revision_id.
func (b *InMemoryBackend) GetWorkflow(in *workflowspb.GetWorkflowRequest) (*workflowspb.Workflow, error) {
	if _, _, _, err := parseWorkflow(in.GetName()); err != nil {
		return nil, err
	}

	b.mu.RLock("GetWorkflow")
	defer b.mu.RUnlock()

	rec, ok := b.workflows[in.GetName()]
	if !ok {
		return nil, errNotFound("Workflow %q not found.", in.GetName())
	}

	if in.GetRevisionId() == "" {
		return clone(rec.latest()), nil
	}

	for _, rev := range rec.revs {
		if rev.GetRevisionId() == in.GetRevisionId() {
			return rec.forRevision(rev), nil
		}
	}

	return nil, errNotFound("Revision %q of workflow %q not found.", in.GetRevisionId(), in.GetName())
}

func inScope(name, project, location string) bool {
	p, l, _, err := parseWorkflow(name)

	return err == nil && p == project && (location == wildcardLoc || l == location)
}

// ListWorkflows lists the workflows of a location, or of every location for "-".
func (b *InMemoryBackend) ListWorkflows(
	in *workflowspb.ListWorkflowsRequest,
) (*workflowspb.ListWorkflowsResponse, error) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if in.GetFilter() != "" {
		return nil, errInvalid("filter is not supported for workflows.")
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListWorkflows")

	var all []*workflowspb.Workflow

	for name, rec := range b.workflows {
		if inScope(name, project, location) {
			all = append(all, clone(rec.latest()))
		}
	}

	b.mu.RUnlock()

	if err = orderWorkflows(all, in.GetOrderBy()); err != nil {
		return nil, err
	}

	size := min(int(in.GetPageSize()), maxWorkflowPage)
	pg := page.New(all, in.GetPageToken(), size, defaultWorkflowPage)

	return &workflowspb.ListWorkflowsResponse{Workflows: pg.Data, NextPageToken: pg.Next}, nil
}

func orderWorkflows(all []*workflowspb.Workflow, orderBy string) error {
	field, desc := "name", false

	if strings.TrimSpace(orderBy) != "" {
		parts := strings.Fields(orderBy)
		if len(parts) > orderParts || strings.Contains(orderBy, ",") {
			return errInvalid("order_by %q is not supported: use one of name, create_time, update_time.", orderBy)
		}

		field = parts[0]

		if len(parts) == orderParts {
			if parts[1] != dirDescending && parts[1] != "asc" {
				return errInvalid("order_by direction must be asc or desc.")
			}

			desc = parts[1] == dirDescending
		}
	}

	var key func(*workflowspb.Workflow) string

	switch field {
	case "name":
		key = func(w *workflowspb.Workflow) string { return w.GetName() }
	case "create_time":
		key = func(w *workflowspb.Workflow) string {
			return w.GetCreateTime().AsTime().Format("2006-01-02T15:04:05.000000000Z") + w.GetName()
		}
	case "update_time":
		key = func(w *workflowspb.Workflow) string {
			return w.GetUpdateTime().AsTime().Format("2006-01-02T15:04:05.000000000Z") + w.GetName()
		}
	default:
		return errInvalid("order_by field %q is not supported: use one of name, create_time, update_time.", field)
	}

	slices.SortFunc(all, func(a, b *workflowspb.Workflow) int {
		c := strings.Compare(key(a), key(b))
		if desc {
			return -c
		}

		return c
	})

	return nil
}

// ListWorkflowRevisions lists the revisions of a workflow, newest first.
func (b *InMemoryBackend) ListWorkflowRevisions(
	in *workflowspb.ListWorkflowRevisionsRequest,
) (*workflowspb.ListWorkflowRevisionsResponse, error) {
	if _, _, _, err := parseWorkflow(in.GetName()); err != nil {
		return nil, err
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListWorkflowRevisions")

	rec, ok := b.workflows[in.GetName()]
	if !ok {
		b.mu.RUnlock()

		return nil, errNotFound("Workflow %q not found.", in.GetName())
	}

	all := make([]*workflowspb.Workflow, 0, len(rec.revs))
	for _, rev := range slices.Backward(rec.revs) {
		all = append(all, rec.forRevision(rev))
	}

	b.mu.RUnlock()

	size := min(int(in.GetPageSize()), maxRevisionPage)
	pg := page.New(all, in.GetPageToken(), size, defaultRevisionPage)

	return &workflowspb.ListWorkflowRevisionsResponse{Workflows: pg.Data, NextPageToken: pg.Next}, nil
}

// DeleteWorkflow removes a workflow with its revisions and executions.
func (b *InMemoryBackend) DeleteWorkflow(in *workflowspb.DeleteWorkflowRequest) (*longrunningpb.Operation, error) {
	project, location, _, err := parseWorkflow(in.GetName())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteWorkflow")
	defer b.mu.Unlock()

	if _, ok := b.workflows[in.GetName()]; !ok {
		return nil, errNotFound("Workflow %q not found.", in.GetName())
	}

	delete(b.workflows, in.GetName())
	b.dropExecutionsLocked(in.GetName() + "/")

	return b.finish(project, location, in.GetName(), "delete", &emptypb.Empty{})
}

func (b *InMemoryBackend) applyWorkflowMask(next, src *workflowspb.Workflow, paths []string) error {
	if len(paths) == 0 {
		paths = presentWorkflowPaths(src)
	}

	for _, p := range paths {
		switch p {
		case maskAll:
			if err := b.applyWorkflowMask(next, src, allWorkflowPaths()); err != nil {
				return err
			}
		case maskDescription:
			next.Description = src.GetDescription()
		case maskLabels:
			next.Labels = src.GetLabels()
		case maskAccount:
			next.ServiceAccount = src.GetServiceAccount()
		case maskSource:
			next.SourceCode = &workflowspb.Workflow_SourceContents{SourceContents: src.GetSourceContents()}
		case maskCryptoKey:
			next.CryptoKeyName = src.GetCryptoKeyName()
		case maskCallLog:
			next.CallLogLevel = src.GetCallLogLevel()
		case maskEnv:
			next.UserEnvVars = src.GetUserEnvVars()
		case maskHistory:
			next.ExecutionHistoryLevel = src.GetExecutionHistoryLevel()
		default:
			return errInvalid("Unsupported update_mask path %q for Workflow.", p)
		}
	}

	return nil
}

func allWorkflowPaths() []string {
	return []string{
		maskDescription, maskLabels, maskAccount, maskSource, maskCryptoKey, maskCallLog, maskEnv, maskHistory,
	}
}

func presentWorkflowPaths(src *workflowspb.Workflow) []string {
	var paths []string

	add := func(ok bool, p string) {
		if ok {
			paths = append(paths, p)
		}
	}

	add(src.GetDescription() != "", maskDescription)
	add(len(src.GetLabels()) > 0, maskLabels)
	add(src.GetServiceAccount() != "", maskAccount)
	add(src.GetSourceContents() != "", maskSource)
	add(src.GetCryptoKeyName() != "", maskCryptoKey)
	add(src.GetCallLogLevel() != workflowspb.Workflow_CALL_LOG_LEVEL_UNSPECIFIED, maskCallLog)
	add(len(src.GetUserEnvVars()) > 0, maskEnv)
	add(
		src.GetExecutionHistoryLevel() != workflowspb.ExecutionHistoryLevel_EXECUTION_HISTORY_LEVEL_UNSPECIFIED,
		maskHistory,
	)

	return paths
}

// UpdateWorkflow updates the masked fields; changing the source or service account creates a revision.
func (b *InMemoryBackend) UpdateWorkflow(in *workflowspb.UpdateWorkflowRequest) (*longrunningpb.Operation, error) {
	src := in.GetWorkflow()

	project, location, _, err := parseWorkflow(src.GetName())
	if err != nil {
		return nil, err
	}

	b.mu.RLock("UpdateWorkflow")
	rec, ok := b.workflows[src.GetName()]

	var cur *workflowspb.Workflow
	if ok {
		cur = clone(rec.latest())
	}

	b.mu.RUnlock()

	if !ok {
		return nil, errNotFound("Workflow %q not found.", src.GetName())
	}

	next := clone(cur)
	if err = b.applyWorkflowMask(next, clone(src), in.GetUpdateMask().GetPaths()); err != nil {
		return nil, err
	}

	if err = validateWorkflow(next); err != nil {
		return nil, err
	}

	next.ServiceAccount = normalizeAccount(project, next.GetServiceAccount())
	newRevision := next.GetSourceContents() != cur.GetSourceContents() ||
		next.GetServiceAccount() != cur.GetServiceAccount()

	var prog *wfinterp.Program

	if newRevision {
		var cerr error
		if prog, cerr = b.compile(next.GetSourceContents()); cerr != nil {
			return b.failed(project, location, src.GetName(), "update", codes.InvalidArgument, cerr.Error())
		}
	}

	return b.commitUpdate(project, location, next, prog, cur.GetRevisionId())
}

func (b *InMemoryBackend) commitUpdate(
	project, location string, next *workflowspb.Workflow, prog *wfinterp.Program, base string,
) (*longrunningpb.Operation, error) {
	b.mu.Lock("UpdateWorkflow")
	defer b.mu.Unlock()

	rec, ok := b.workflows[next.GetName()]
	if !ok || rec.latest().GetRevisionId() != base {
		return nil, errAborted("The workflow was modified concurrently; retry the update.")
	}

	now := b.ts()
	next.UpdateTime = now

	if prog != nil {
		next.RevisionId = newRevisionID(len(rec.revs) + 1)
		next.RevisionCreateTime = now
		rec.progs[next.GetRevisionId()] = prog
		rec.revs = append(rec.revs, next)
	} else {
		rec.revs[len(rec.revs)-1] = next
	}

	return b.finish(project, location, next.GetName(), "update", clone(next))
}
