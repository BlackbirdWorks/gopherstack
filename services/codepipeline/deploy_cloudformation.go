package codepipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
)

const (
	cfnModeCreateUpdate     = "CREATE_UPDATE"
	cfnModeReplaceOnFailure = "REPLACE_ON_FAILURE"
	cfnModeDeleteOnly       = "DELETE_ONLY"
	cfnModeChangeSetReplace = "CHANGE_SET_REPLACE"
	cfnModeChangeSetExecute = "CHANGE_SET_EXECUTE"

	stackStatusDeleteComplete   = "DELETE_COMPLETE"
	stackStatusDeleteInProgress = "DELETE_IN_PROGRESS"
	changeSetStatusComplete     = "CREATE_COMPLETE"
	changeSetStatusFailed       = "FAILED"
	statusSuffixInProgress      = "_IN_PROGRESS"

	configKeyOutputFileName = "OutputFileName"
)

var errUnresolvableParameter = errors.New("unresolvable parameter override")

// cfnSettled reports the terminal outcome of a stack status: done is false while in progress.
func cfnSettled(status string) (bool, bool) {
	switch status {
	case "CREATE_COMPLETE", "UPDATE_COMPLETE", "IMPORT_COMPLETE":
		return true, true
	}

	return !strings.HasSuffix(status, statusSuffixInProgress), false
}

func cfnFailedState(status string) bool {
	switch status {
	case "ROLLBACK_FAILED", "CREATE_FAILED", "DELETE_FAILED", "UPDATE_ROLLBACK_FAILED", "ROLLBACK_COMPLETE":
		return true
	}

	return false
}

// runCloudFormationAction starts a Deploy/CloudFormation action and settles it as far as the stack allows;
// reconcileAsyncActions drives it the rest of the way.
func (b *InMemoryBackend) runCloudFormationAction(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) {
	if b.stackDeployer == nil {
		failActionf(ae, failureConfigurationError, "CloudFormation is not available in this emulator instance")

		return
	}

	ae.Status = statusInProgress

	b.advanceCloudFormation(ctx, p, execID, action, ae)
}

func (b *InMemoryBackend) advanceCloudFormation(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) {
	cfg := action.Configuration
	stack := cfg["StackName"]

	switch cfg["ActionMode"] {
	case cfnModeDeleteOnly:
		b.cfnDelete(ctx, stack, ae)
	case cfnModeChangeSetReplace:
		b.cfnChangeSetReplace(ctx, p, execID, action, ae)
	case cfnModeChangeSetExecute:
		b.cfnChangeSetExecute(ctx, p, execID, action, ae)
	case cfnModeCreateUpdate, cfnModeReplaceOnFailure:
		b.cfnCreateOrUpdate(ctx, p, execID, action, ae)
	default:
		failActionf(ae, failureConfigurationError, "Unsupported CloudFormation ActionMode %q", cfg["ActionMode"])
	}
}

func (b *InMemoryBackend) cfnDelete(ctx context.Context, name string, ae *ActionExecution) {
	st, err := b.stackDeployer.DescribeStack(ctx, name)
	if err == nil && ae.ExternalExecutionID == "" {
		var started bool

		if started, err = b.cfnBeginDelete(ctx, name, st, ae); started || err != nil {
			if err != nil {
				failActionf(ae, failureJobFailed, "%v", err)
			}

			return
		}

		st, err = b.stackDeployer.DescribeStack(ctx, name)
	}

	switch {
	case err != nil:
		failActionf(ae, failureJobFailed, "%v", err)
	case st == nil || st.Status == stackStatusDeleteComplete:
		ae.Status = statusSucceeded
	case st.Status == "DELETE_FAILED":
		failActionf(ae, failureJobFailed, "%s", stackFailure(name, st))
	}
}

// cfnBeginDelete issues the delete. It reports true when the action already succeeded because there is nothing to
// delete.
func (b *InMemoryBackend) cfnBeginDelete(
	ctx context.Context, name string, st *StackState, ae *ActionExecution,
) (bool, error) {
	if st == nil || st.Status == stackStatusDeleteComplete {
		ae.Status = statusSucceeded

		return true, nil
	}

	if st.Status != stackStatusDeleteInProgress {
		if err := b.stackDeployer.DeleteStack(ctx, name); err != nil {
			return false, err
		}
	}

	ae.ExternalExecutionID = name

	return false, nil
}

func stackFailure(name string, st *StackState) string {
	if st.Reason != "" {
		return fmt.Sprintf("Stack [%s] is in %s state: %s", name, st.Status, st.Reason)
	}

	return fmt.Sprintf("Stack [%s] is in %s state", name, st.Status)
}

func (b *InMemoryBackend) cfnCreateOrUpdate(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) {
	name := action.Configuration["StackName"]

	if ae.ExternalExecutionID == "" {
		if !b.cfnStartDeployment(ctx, p, execID, action, ae) || ae.Status != statusInProgress {
			return
		}
	}

	b.cfnSettleStack(ctx, p, execID, action, ae, name)
}

// cfnStartDeployment issues the create or update. It returns false when the action already finished or must wait
// for the stack to leave a transient state before it can proceed.
func (b *InMemoryBackend) cfnStartDeployment(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) bool {
	name := action.Configuration["StackName"]

	st, ready, err := b.cfnPrepareStack(ctx, name, action.Configuration["ActionMode"])
	if err != nil {
		failActionf(ae, failureJobFailed, "%v", err)

		return false
	}

	if !ready {
		return false
	}

	req, err := b.cfnRequest(ctx, p, execID, action)
	if err != nil {
		failActionf(ae, failureConfigurationError, "%v", err)

		return false
	}

	if st == nil {
		err = b.stackDeployer.CreateStack(ctx, req)
	} else {
		err = b.stackDeployer.UpdateStack(ctx, req)
	}

	ae.ExternalExecutionID = name

	if errors.Is(err, ErrNoStackUpdates) {
		return true
	}

	if err != nil {
		failActionf(ae, failureJobFailed, "%v", err)

		return false
	}

	return true
}

// cfnPrepareStack returns the stack to update (nil to create) and whether the deployment may start now.
// REPLACE_ON_FAILURE deletes a failed stack first and waits for the deletion.
func (b *InMemoryBackend) cfnPrepareStack(ctx context.Context, name, mode string) (*StackState, bool, error) {
	st, err := b.stackDeployer.DescribeStack(ctx, name)
	if err != nil {
		return nil, false, err
	}

	if st != nil && st.Status == stackStatusDeleteComplete {
		st = nil
	}

	if st != nil && mode == cfnModeReplaceOnFailure && cfnFailedState(st.Status) {
		if st, err = b.cfnDeleteFailedStack(ctx, name, st); err != nil || st != nil {
			return nil, false, err
		}
	}

	if st != nil && strings.HasSuffix(st.Status, statusSuffixInProgress) {
		return nil, false, nil
	}

	return st, true, nil
}

// cfnDeleteFailedStack deletes st and returns the stack's state afterwards (nil once it is gone).
func (b *InMemoryBackend) cfnDeleteFailedStack(ctx context.Context, name string, st *StackState) (*StackState, error) {
	if st.Status != stackStatusDeleteInProgress {
		if err := b.stackDeployer.DeleteStack(ctx, name); err != nil {
			return nil, err
		}
	}

	after, err := b.stackDeployer.DescribeStack(ctx, name)
	if err != nil {
		return nil, err
	}

	if after == nil || after.Status == stackStatusDeleteComplete {
		return nil, nil //nolint:nilnil // gone
	}

	return after, nil
}

// cfnSettleStack finishes the action once the stack reaches a terminal status.
func (b *InMemoryBackend) cfnSettleStack(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution, name string,
) {
	st, err := b.stackDeployer.DescribeStack(ctx, name)
	if err != nil {
		failActionf(ae, failureJobFailed, "%v", err)

		return
	}

	if st == nil {
		failActionf(ae, failureJobFailed, "Stack [%s] does not exist", name)

		return
	}

	done, ok := cfnSettled(st.Status)

	switch {
	case !done:
		return
	case !ok:
		failActionf(ae, failureJobFailed, "%s", stackFailure(name, st))
	default:
		b.cfnPublishOutputs(ctx, p, execID, action, ae, st)
	}
}

func (b *InMemoryBackend) cfnPublishOutputs(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution, st *StackState,
) {
	ae.Status = statusSucceeded
	ae.OutputVariables = make(map[string]string, len(st.Outputs))

	maps.Copy(ae.OutputVariables, st.Outputs)

	file := action.Configuration[configKeyOutputFileName]
	if file == "" {
		return
	}

	body, err := json.Marshal(st.Outputs)
	if err == nil {
		err = b.putOutputFiles(ctx, p, execID, action, map[string][]byte{file: body})
	}

	if err != nil {
		failActionf(ae, failureJobFailed, "Unable to write stack outputs to %s: %v", file, err)
	}
}

func (b *InMemoryBackend) cfnChangeSetReplace(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) {
	name, csName := action.Configuration["StackName"], action.Configuration["ChangeSetName"]

	if ae.ExternalExecutionID == "" {
		req, err := b.cfnRequest(ctx, p, execID, action)
		if err != nil {
			failActionf(ae, failureConfigurationError, "%v", err)

			return
		}

		st, err := b.stackDeployer.DescribeStack(ctx, name)
		if err == nil {
			err = b.cfnReplaceChangeSet(ctx, req, csName, st == nil || st.Status == stackStatusDeleteComplete)
		}

		if err != nil {
			failActionf(ae, failureJobFailed, "%v", err)

			return
		}

		ae.ExternalExecutionID = name
	}

	cs, err := b.stackDeployer.DescribeChangeSet(ctx, name, csName)

	switch {
	case err != nil:
		failActionf(ae, failureJobFailed, "%v", err)
	case cs == nil:
		failActionf(ae, failureJobFailed, "Change set [%s] does not exist", csName)
	case cs.Status == changeSetStatusFailed:
		failActionf(ae, failureJobFailed, "Change set [%s] failed: %s", csName, cs.Reason)
	case cs.Status == changeSetStatusComplete:
		ae.Status = statusSucceeded
	}
}

func (b *InMemoryBackend) cfnReplaceChangeSet(ctx context.Context, req StackRequest, csName string, forNew bool) error {
	existing, err := b.stackDeployer.DescribeChangeSet(ctx, req.StackName, csName)
	if err != nil {
		return err
	}

	if existing != nil {
		if err = b.stackDeployer.DeleteChangeSet(ctx, req.StackName, csName); err != nil {
			return err
		}
	}

	return b.stackDeployer.CreateChangeSet(ctx, req, csName, forNew)
}

func (b *InMemoryBackend) cfnChangeSetExecute(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) {
	name, csName := action.Configuration["StackName"], action.Configuration["ChangeSetName"]

	if ae.ExternalExecutionID == "" {
		cs, err := b.stackDeployer.DescribeChangeSet(ctx, name, csName)

		switch {
		case err != nil:
			failActionf(ae, failureJobFailed, "%v", err)

			return
		case cs == nil:
			failActionf(ae, failureJobFailed, "Change set [%s] does not exist for stack [%s]", csName, name)

			return
		}

		if err = b.stackDeployer.ExecuteChangeSet(ctx, name, csName); err != nil {
			failActionf(ae, failureJobFailed, "%v", err)

			return
		}

		ae.ExternalExecutionID = name
	}

	b.cfnSettleStack(ctx, p, execID, action, ae, name)
}

// cfnRequest assembles the stack request from the action configuration and its input artifacts.
func (b *InMemoryBackend) cfnRequest(
	ctx context.Context, p *Pipeline, execID string, action Action,
) (StackRequest, error) {
	cfg := action.Configuration
	req := StackRequest{
		StackName: cfg["StackName"],
		RoleARN:   cfg["RoleArn"],
		Tags:      map[string]string{},
	}

	for c := range strings.SplitSeq(cfg["Capabilities"], ",") {
		if c = strings.TrimSpace(c); c != "" {
			req.Capabilities = append(req.Capabilities, c)
		}
	}

	template, err := b.artifactRef(ctx, p, execID, action, cfg["TemplatePath"])
	if err != nil {
		return req, fmt.Errorf("unable to read template %q: %w", cfg["TemplatePath"], err)
	}

	req.TemplateBody = string(template)
	req.Parameters = map[string]string{}

	if ref := cfg["TemplateConfiguration"]; ref != "" {
		if err = b.applyTemplateConfiguration(ctx, p, execID, action, ref, &req); err != nil {
			return req, err
		}
	}

	if raw := cfg["ParameterOverrides"]; raw != "" {
		if err = b.applyParameterOverrides(ctx, p, execID, action, raw, &req); err != nil {
			return req, err
		}
	}

	return req, nil
}

func (b *InMemoryBackend) applyTemplateConfiguration(
	ctx context.Context, p *Pipeline, execID string, action Action, ref string, req *StackRequest,
) error {
	body, err := b.artifactRef(ctx, p, execID, action, ref)
	if err != nil {
		return fmt.Errorf("unable to read template configuration %q: %w", ref, err)
	}

	var doc struct {
		Parameters  map[string]any  `json:"Parameters"`
		Tags        map[string]any  `json:"Tags"`
		StackPolicy json.RawMessage `json:"StackPolicy"`
	}

	if err = json.Unmarshal(body, &doc); err != nil {
		return fmt.Errorf("template configuration %q is not valid JSON: %w", ref, err)
	}

	for k, v := range doc.Parameters {
		req.Parameters[k] = scalarString(v)
	}

	for k, v := range doc.Tags {
		req.Tags[k] = scalarString(v)
	}

	if len(doc.StackPolicy) > 0 {
		req.StackPolicy = string(doc.StackPolicy)
	}

	return nil
}

func scalarString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}

	return fmt.Sprint(v)
}

// applyParameterOverrides resolves the ParameterOverrides JSON object, expanding Fn::GetArtifactAtt and
// Fn::GetParam against the action's input artifacts.
func (b *InMemoryBackend) applyParameterOverrides(
	ctx context.Context, p *Pipeline, execID string, action Action, raw string, req *StackRequest,
) error {
	var overrides map[string]any
	if err := json.Unmarshal([]byte(raw), &overrides); err != nil {
		return fmt.Errorf("ParameterOverrides is not a valid JSON object: %w", err)
	}

	for k, v := range overrides {
		fn, isFn := v.(map[string]any)
		if !isFn {
			req.Parameters[k] = scalarString(v)

			continue
		}

		val, err := b.resolveParameterFunction(ctx, p, execID, action, fn)
		if err != nil {
			return fmt.Errorf("%w %q: %w", errUnresolvableParameter, k, err)
		}

		req.Parameters[k] = val
	}

	return nil
}

func (b *InMemoryBackend) resolveParameterFunction(
	ctx context.Context, p *Pipeline, execID string, action Action, fn map[string]any,
) (string, error) {
	if args, ok := fn["Fn::GetArtifactAtt"].([]any); ok && len(args) == 2 {
		return b.getArtifactAtt(p, execID, action, scalarString(args[0]), scalarString(args[1]))
	}

	if args, ok := fn["Fn::GetParam"].([]any); ok && len(args) == 3 {
		body, err := b.artifactFile(ctx, p, execID, action, scalarString(args[0]), scalarString(args[1]))
		if err != nil {
			return "", err
		}

		var doc map[string]any
		if err = json.Unmarshal(body, &doc); err != nil {
			return "", fmt.Errorf("%s is not a JSON object: %w", scalarString(args[1]), err)
		}

		val, found := doc[scalarString(args[2])]
		if !found {
			return "", fmt.Errorf(
				"%w: key %q not found in %s",
				ErrValidation,
				scalarString(args[2]),
				scalarString(args[1]),
			)
		}

		return scalarString(val), nil
	}

	return "", fmt.Errorf("%w: unsupported function %v", ErrValidation, fn)
}

func (b *InMemoryBackend) getArtifactAtt(
	p *Pipeline,
	execID string,
	action Action,
	artifact, attr string,
) (string, error) {
	bucket, key, ok := artifactLocation(p, action, execID, artifact)
	if !ok {
		return "", fmt.Errorf("%w: pipeline has no artifact store location", ErrValidation)
	}

	switch attr {
	case "BucketName":
		return bucket, nil
	case "ObjectKey":
		return key, nil
	case "URL":
		return "https://s3." + b.regionOf(action, p) + ".amazonaws.com/" + bucket + "/" + key, nil
	}

	return "", fmt.Errorf("%w: unknown artifact attribute %q", ErrValidation, attr)
}

func (b *InMemoryBackend) regionOf(_ Action, p *Pipeline) string {
	if p.region != "" {
		return p.region
	}

	return b.region
}
