package codepipeline

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	conditionTypeBeforeEntry = "BEFORE_ENTRY"
	conditionTypeOnSuccess   = "ON_SUCCESS"
	conditionTypeOnFailure   = "ON_FAILURE"

	conditionStatusFailed     = "Failed"
	conditionStatusOverridden = "Overridden"

	resultFail     = "FAIL"
	resultRollback = "ROLLBACK"
	resultRetry    = "RETRY"
	resultSkip     = "SKIP"

	ruleProviderVariableCheck = "VariableCheck"
	ruleProviderLambdaInvoke  = "LambdaInvoke"

	triggerTypeAutomatedRollback = "AutomatedRollback"
	retryTriggerAutomated        = "AutomatedStageRetry"
)

// RuleRun is one execution of a condition rule within a pipeline execution.
type RuleRun struct {
	StartTime             time.Time         `json:"startTime"`
	LastUpdateTime        time.Time         `json:"lastUpdateTime"`
	Configuration         map[string]string `json:"configuration,omitempty"`
	ResolvedConfiguration map[string]string `json:"resolvedConfiguration,omitempty"`
	RuleTypeID            ActionTypeID      `json:"ruleTypeId"`
	RuleExecutionID       string            `json:"ruleExecutionId"`
	RuleName              string            `json:"ruleName"`
	Status                string            `json:"status"`
	Summary               string            `json:"summary,omitempty"`
	ErrorCode             string            `json:"errorCode,omitempty"`
	UpdatedBy             string            `json:"updatedBy,omitempty"`
	RoleArn               string            `json:"roleArn,omitempty"`
	Region                string            `json:"region,omitempty"`
	PipelineVersion       int               `json:"pipelineVersion"`
}

// ConditionExecution is the evaluation of one declared Condition.
type ConditionExecution struct {
	LastStatusChange time.Time  `json:"lastStatusChange"`
	Status           string     `json:"status"`
	Summary          string     `json:"summary,omitempty"`
	Result           string     `json:"result,omitempty"`
	Rules            []*RuleRun `json:"rules"`
}

// ConditionRun is the evaluation of a stage's BEFORE_ENTRY, ON_SUCCESS or ON_FAILURE conditions.
type ConditionRun struct {
	LastStatusChange    time.Time             `json:"lastStatusChange"`
	PipelineExecutionID string                `json:"pipelineExecutionId"`
	StageName           string                `json:"stageName"`
	ConditionType       string                `json:"conditionType"`
	Status              string                `json:"status"`
	Summary             string                `json:"summary,omitempty"`
	Result              string                `json:"result,omitempty"`
	Conditions          []*ConditionExecution `json:"conditions"`
	AutoRetried         bool                  `json:"autoRetried,omitempty"`
}

var (
	errRuleConfig       = errors.New("invalid rule configuration")
	errRuleNotSatisfied = errors.New("rule not satisfied")
)

func validResult(r string) bool {
	switch r {
	case resultFail, resultRollback, resultRetry, resultSkip:
		return true
	}

	return false
}

// validateStageConditions checks the structural constraints the SDK documents for stage conditions.
func validateStageConditions(stages []Stage) error {
	for _, s := range stages {
		var all []Condition
		if s.BeforeEntry != nil {
			all = append(all, s.BeforeEntry.Conditions...)
		}

		if s.OnSuccess != nil {
			all = append(all, s.OnSuccess.Conditions...)
		}

		if s.OnFailure != nil {
			all = append(all, s.OnFailure.Conditions...)

			if r := s.OnFailure.Result; r != "" && !validResult(r) {
				return fmt.Errorf("%w: stage %q: invalid result %q", ErrInvalidStructure, s.Name, r)
			}
		}

		for _, c := range all {
			if err := validateCondition(s.Name, c); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateCondition(stageName string, c Condition) error {
	if c.Result != "" && !validResult(c.Result) {
		return fmt.Errorf("%w: stage %q: invalid condition result %q", ErrInvalidStructure, stageName, c.Result)
	}

	for _, r := range c.Rules {
		if r.Name == "" || r.RuleTypeID.Provider == "" {
			return fmt.Errorf("%w: stage %q: rule name and ruleTypeId provider are required",
				ErrInvalidStructure, stageName)
		}
	}

	return nil
}

func (b *InMemoryBackend) conditionRunsStore(region string) map[string][]*ConditionRun {
	if b.conditionRuns[region] == nil {
		b.conditionRuns[region] = make(map[string][]*ConditionRun)
	}

	return b.conditionRuns[region]
}

func (b *InMemoryBackend) conditionRunsStoreRO(region string) map[string][]*ConditionRun {
	return b.conditionRuns[region]
}

// findConditionRun returns the recorded run for an execution's stage condition type, or nil.
func findConditionRun(runs []*ConditionRun, executionID, stageName, condType string) *ConditionRun {
	for _, r := range runs {
		if r.PipelineExecutionID == executionID && r.StageName == stageName && r.ConditionType == condType {
			return r
		}
	}

	return nil
}

// evaluateConditions runs every rule of conds, records the run, and returns it. Callers hold b.mu.Lock.
func (b *InMemoryBackend) evaluateConditions(
	region string, p *Pipeline, exec *PipelineExecution, stageName, condType string, conds []Condition,
) *ConditionRun {
	now := time.Now().UTC()
	run := &ConditionRun{
		PipelineExecutionID: exec.PipelineExecutionID,
		StageName:           stageName,
		ConditionType:       condType,
		LastStatusChange:    now,
		Status:              statusSucceeded,
		Summary:             "All conditions met",
	}

	for _, c := range conds {
		ce := b.evaluateCondition(region, p, exec, c, now)
		run.Conditions = append(run.Conditions, ce)

		if ce.Status != statusSucceeded && run.Status == statusSucceeded {
			run.Status = conditionStatusFailed
			run.Summary = ce.Summary
			run.Result = ce.Result
		}
	}

	store := b.conditionRunsStore(region)
	store[p.Declaration.Name] = append(store[p.Declaration.Name], run)

	return run
}

func (b *InMemoryBackend) evaluateCondition(
	region string, p *Pipeline, exec *PipelineExecution, c Condition, now time.Time,
) *ConditionExecution {
	ce := &ConditionExecution{
		LastStatusChange: now,
		Status:           statusSucceeded,
		Summary:          "All rules succeeded",
		Result:           c.Result,
		Rules:            make([]*RuleRun, 0, len(c.Rules)),
	}

	for _, rule := range c.Rules {
		rr := b.runRule(region, p, exec, rule, now)
		ce.Rules = append(ce.Rules, rr)

		if rr.Status != statusSucceeded && ce.Status == statusSucceeded {
			ce.Status = conditionStatusFailed
			ce.Summary = fmt.Sprintf("Rule %s failed: %s", rr.RuleName, rr.Summary)
		}
	}

	return ce
}

// runRule executes one rule. VariableCheck compares resolved values; LambdaInvoke calls the wired Lambda
// backend; the remaining providers have no real target to consult and succeed.
func (b *InMemoryBackend) runRule(
	region string, p *Pipeline, exec *PipelineExecution, rule Rule, now time.Time,
) *RuleRun {
	rr := &RuleRun{
		RuleExecutionID: uuid.NewString(),
		RuleName:        rule.Name,
		RuleTypeID:      rule.RuleTypeID,
		Configuration:   copyStringMap(rule.Configuration),
		RoleArn:         rule.RoleArn,
		Region:          rule.Region,
		StartTime:       now,
		LastUpdateTime:  now,
		Status:          statusSucceeded,
		PipelineVersion: p.Declaration.Version,
	}

	resolver := b.referenceResolver(region, p, exec)
	rr.ResolvedConfiguration = make(map[string]string, len(rule.Configuration))

	for k, v := range rule.Configuration {
		rr.ResolvedConfiguration[k] = resolver(v)
	}

	var err error

	switch rule.RuleTypeID.Provider {
	case ruleProviderVariableCheck:
		err = checkVariable(rr.ResolvedConfiguration)
	case ruleProviderLambdaInvoke:
		err = b.invokeRuleLambda(region, rr.ResolvedConfiguration)
	}

	if err != nil {
		rr.Status = statusFailed
		rr.Summary = err.Error()
		rr.ErrorCode = "RuleFailed"
	}

	return rr
}

func (b *InMemoryBackend) invokeRuleLambda(region string, cfg map[string]string) error {
	fn := cfg[configKeyFunctionName]
	if fn == "" || b.lambdaBackend == nil {
		return nil
	}

	payload := []byte("{}")
	if up := cfg["UserParameters"]; up != "" {
		payload = []byte(up)
	}

	ctx := b.regionContext(region)
	if _, _, err := b.lambdaBackend.InvokeFunction(ctx, fn, "RequestResponse", payload); err != nil {
		return fmt.Errorf("lambda invocation failed: %w", err)
	}

	return nil
}

var referencePattern = regexp.MustCompile(`#\{([^}]+)\}`)

// referenceResolver substitutes #{codepipeline.PipelineExecutionId}, #{variables.Name} and
// #{Namespace.Variable} references; unknown references are left verbatim.
func (b *InMemoryBackend) referenceResolver(region string, p *Pipeline, exec *PipelineExecution) func(string) string {
	known := map[string]string{
		"codepipeline.PipelineExecutionId": exec.PipelineExecutionID,
	}

	for _, v := range exec.Variables {
		known["variables."+v.Name] = v.ResolvedValue
	}

	namespaces := make(map[string]string)

	for _, st := range p.Declaration.Stages {
		for _, a := range st.Actions {
			if a.Namespace != "" {
				namespaces[st.Name+"/"+a.Name] = a.Namespace
			}
		}
	}

	for _, ae := range b.actionExecutionsStoreRO(region)[p.Declaration.Name] {
		if ae.PipelineExecutionID != exec.PipelineExecutionID {
			continue
		}

		if ns := namespaces[ae.StageName+"/"+ae.ActionName]; ns != "" {
			for k, v := range ae.OutputVariables {
				known[ns+"."+k] = v
			}
		}
	}

	return func(s string) string {
		return referencePattern.ReplaceAllStringFunc(s, func(m string) string {
			if v, ok := known[m[2:len(m)-1]]; ok {
				return v
			}

			return m
		})
	}
}

// checkVariable evaluates a VariableCheck rule's Variable/Operator/Value configuration.
func checkVariable(cfg map[string]string) error {
	variable, hasVar := cfg["Variable"]
	op := strings.ToUpper(cfg["Operator"])
	value, hasValue := cfg["Value"]

	if !hasVar || op == "" || !hasValue {
		return fmt.Errorf("%w: Variable, Operator and Value are required", errRuleConfig)
	}

	ok, err := compareValues(op, variable, value)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("%w: %q %s %q", errRuleNotSatisfied, variable, op, value)
	}

	return nil
}

func compareValues(op, left, right string) (bool, error) {
	switch op {
	case "EQ":
		return left == right, nil
	case "NE":
		return left != right, nil
	case "CONTAINS":
		return strings.Contains(left, right), nil
	case "MATCHES":
		re, err := regexp.Compile(right)
		if err != nil {
			return false, fmt.Errorf("%w: invalid MATCHES pattern: %w", errRuleConfig, err)
		}

		return re.MatchString(left), nil
	case "GT", "GTE", "LT", "LTE":
		return compareNumbers(op, left, right)
	default:
		return false, fmt.Errorf("%w: unsupported Operator %q", errRuleConfig, op)
	}
}

func compareNumbers(op, left, right string) (bool, error) {
	l, lerr := strconv.ParseFloat(left, 64)
	r, rerr := strconv.ParseFloat(right, 64)

	if lerr != nil || rerr != nil {
		return false, fmt.Errorf("%w: %s requires numeric operands", errRuleConfig, op)
	}

	switch op {
	case "GT":
		return l > r, nil
	case "GTE":
		return l >= r, nil
	case "LT":
		return l < r, nil
	default:
		return l <= r, nil
	}
}

// enterStage applies BEFORE_ENTRY conditions; it returns skip=true when the stage is skipped and halt=true
// when the execution fails at the gate.
func (b *InMemoryBackend) enterStage(
	region string, p *Pipeline, exec *PipelineExecution, stage Stage,
) (bool, bool) {
	if stage.BeforeEntry == nil || len(stage.BeforeEntry.Conditions) == 0 {
		return false, false
	}

	runs := b.conditionRunsStore(region)[p.Declaration.Name]

	run := findConditionRun(runs, exec.PipelineExecutionID, stage.Name, conditionTypeBeforeEntry)
	if run == nil {
		run = b.evaluateConditions(
			region, p, exec, stage.Name, conditionTypeBeforeEntry, stage.BeforeEntry.Conditions,
		)
	}

	if run.Status != conditionStatusFailed {
		return false, false
	}

	if run.Result == resultSkip {
		return true, false
	}

	failExecution(exec, fmt.Sprintf("Entry conditions for stage %s were not met: %s", stage.Name, run.Summary))

	return false, true
}

// succeedStage applies ON_SUCCESS conditions after every action of the stage succeeded; it returns
// true when the execution must stop at the gate.
func (b *InMemoryBackend) succeedStage(region string, p *Pipeline, exec *PipelineExecution, stage Stage) bool {
	if stage.OnSuccess == nil || len(stage.OnSuccess.Conditions) == 0 {
		return false
	}

	runs := b.conditionRunsStore(region)[p.Declaration.Name]

	run := findConditionRun(runs, exec.PipelineExecutionID, stage.Name, conditionTypeOnSuccess)
	if run == nil {
		run = b.evaluateConditions(
			region, p, exec, stage.Name, conditionTypeOnSuccess, stage.OnSuccess.Conditions,
		)
	}

	if run.Status != conditionStatusFailed {
		return false
	}

	if run.Result == resultRollback {
		b.rollbackFailedStage(region, p, exec, stage)
	}

	failExecution(exec, fmt.Sprintf("Success conditions for stage %s were not met: %s", stage.Name, run.Summary))

	return true
}

// failStage applies the stage's failure handling once per execution; it returns true when the failed
// actions were reset for an automatic retry and the stage should be re-driven.
func (b *InMemoryBackend) failStage(region string, p *Pipeline, exec *PipelineExecution, stage Stage) bool {
	fc := stage.OnFailure
	if fc == nil {
		return false
	}

	runs := b.conditionRunsStore(region)[p.Declaration.Name]
	if findConditionRun(runs, exec.PipelineExecutionID, stage.Name, conditionTypeOnFailure) != nil {
		return false
	}

	run := b.evaluateConditions(region, p, exec, stage.Name, conditionTypeOnFailure, fc.Conditions)
	result := fc.Result

	for _, ce := range run.Conditions {
		if ce.Status == statusSucceeded && ce.Result != "" {
			result = ce.Result

			break
		}
	}

	if len(fc.Conditions) > 0 && run.Status == conditionStatusFailed && !anyConditionMet(run) {
		return false
	}

	switch result {
	case resultRetry:
		run.AutoRetried = true
		allActions := fc.RetryConfiguration != nil && fc.RetryConfiguration.RetryMode == stageRetryModeAllActions

		return b.dropStageActions(region, p.Declaration.Name, exec.PipelineExecutionID, stage.Name, allActions)
	case resultRollback:
		b.rollbackFailedStage(region, p, exec, stage)
	}

	return false
}

func anyConditionMet(run *ConditionRun) bool {
	for _, ce := range run.Conditions {
		if ce.Status == statusSucceeded {
			return true
		}
	}

	return false
}

// rollbackFailedStage starts an AutomatedRollback execution targeting the latest earlier execution that
// completed stage successfully; with no such execution nothing is rolled back.
func (b *InMemoryBackend) rollbackFailedStage(region string, p *Pipeline, exec *PipelineExecution, stage Stage) {
	execs := b.executionsStore(region)[p.Declaration.Name]
	actionExecs := b.actionExecutionsStore(region)[p.Declaration.Name]

	for _, cand := range slices.Backward(execs) {
		if cand.PipelineExecutionID == exec.PipelineExecutionID || cand.ExecutionType == executionTypeRollback {
			continue
		}

		if stageSucceededInExecution(actionExecs, &stage, cand.PipelineExecutionID) {
			b.newRollbackExecution(region, p, &stage, cand.PipelineExecutionID, triggerTypeAutomatedRollback)

			return
		}
	}
}

// failExecution marks exec Failed with a human-readable StatusSummary.
func failExecution(exec *PipelineExecution, summary string) {
	exec.Status = statusFailed
	exec.StatusSummary = summary
}

// dropStageActions removes the failed (or, with all, every) action execution of a stage so they run again.
// It reports whether anything was removed.
func (b *InMemoryBackend) dropStageActions(region, pipelineName, executionID, stageName string, all bool) bool {
	store := b.actionExecutionsStore(region)
	kept := make([]*ActionExecution, 0, len(store[pipelineName]))
	dropped := false

	for _, ae := range store[pipelineName] {
		inStage := ae.PipelineExecutionID == executionID && ae.StageName == stageName
		if inStage && (all || ae.Status == statusFailed) {
			dropped = true

			continue
		}

		kept = append(kept, ae)
	}

	store[pipelineName] = kept

	return dropped
}
