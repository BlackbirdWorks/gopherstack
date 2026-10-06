package ssm

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// automationDoc is the minimal shape of an SSM automation (schemaVersion 0.3)
// document needed to derive its steps.
type automationDoc struct {
	MainSteps []automationDocStep `json:"mainSteps"`
}

type automationDocStep struct {
	IsCritical  *bool  `json:"isCritical"`
	IsEnd       *bool  `json:"isEnd"`
	MaxAttempts *int32 `json:"maxAttempts"`
	Name        string `json:"name"`
	Action      string `json:"action"`
	NextStep    string `json:"nextStep"`
	OnFailure   string `json:"onFailure"`
}

// extractAutomationSteps parses an automation document body and returns its
// ordered steps as pending step executions. When the body cannot be parsed or
// declares no steps, a single synthetic step is returned so that every
// execution advances through at least one observable step.
func extractAutomationSteps(docName, content string) []AutomationStepExec {
	steps := parseAutomationDocSteps(content)
	if len(steps) == 0 {
		name := docName
		if name == "" {
			name = "executeAutomation"
		}

		return []AutomationStepExec{
			{
				StepName:        name,
				Action:          "aws:executeAutomation",
				StepStatus:      automationStatusPending,
				StepExecutionID: uuid.NewString(),
			},
		}
	}

	out := make([]AutomationStepExec, 0, len(steps))
	for _, s := range steps {
		out = append(out, AutomationStepExec{
			StepName:        s.Name,
			Action:          s.Action,
			StepStatus:      automationStatusPending,
			StepExecutionID: uuid.NewString(),
			IsCritical:      stepBoolDefault(s.IsCritical, true),
			IsEnd:           stepBoolDefault(s.IsEnd, false),
			MaxAttempts:     stepMaxAttempts(s.MaxAttempts),
			NextStep:        s.NextStep,
			OnFailure:       stepOnFailure(s.OnFailure),
		})
	}

	return out
}

func stepBoolDefault(v *bool, def bool) *bool {
	if v != nil {
		return v
	}

	return &def
}

func stepMaxAttempts(v *int32) *int32 {
	if v != nil {
		return v
	}

	one := int32(1)

	return &one
}

func stepOnFailure(v string) string {
	if v == "" {
		return "Abort"
	}

	return v
}

// automationProgressCounters tallies step outcomes for GetAutomationExecution.
func automationProgressCounters(steps []AutomationStepExec) *ProgressCounters {
	pc := &ProgressCounters{}

	for i := range steps {
		pc.TotalSteps++

		switch steps[i].StepStatus {
		case automationStatusSuccess:
			pc.SuccessSteps++
		case automationStatusFailed:
			pc.FailedSteps++
		case automationStatusCancelled:
			pc.CancelledSteps++
		case automationStatusTimedOut:
			pc.TimedOutSteps++
		}
	}

	return pc
}

func parseAutomationDocSteps(content string) []automationDocStep {
	if content == "" {
		return nil
	}

	var doc automationDoc
	if err := json.Unmarshal([]byte(content), &doc); err != nil {
		return nil
	}

	return doc.MainSteps
}

// buildAutomationSteps looks up the named document (if present) to derive the
// step list; falls back to a synthetic single step otherwise. Must be called
// with b.mu held.
func (b *InMemoryBackend) buildAutomationSteps(region, docName string) []AutomationStepExec {
	var content string
	if doc, ok := b.documentsStore(region).Get(docName); ok {
		content = doc.Content
	}

	return extractAutomationSteps(docName, content)
}

// completeAutomationLocked drives every step to Success and marks the execution
// Success with an end time. Must be called with b.mu held.
func completeAutomationLocked(exec *AutomationExecution, now time.Time) {
	for i := range exec.Steps {
		exec.Steps[i].StepStatus = automationStatusSuccess
		exec.Steps[i].ExecutionStartTime = UnixTimeFloat(now)
		exec.Steps[i].ExecutionEndTime = UnixTimeFloat(now)
	}

	exec.Status = automationStatusSuccess
	exec.completeAfter = 0
	exec.EndTime = UnixTimeFloat(now)
}

// materializeAutomationLocked lazily completes an InProgress execution whose
// exec delay has elapsed. Must be called with b.mu held.
func materializeAutomationLocked(exec *AutomationExecution, now time.Time) {
	if exec == nil || exec.Status != automationStatusInProgress {
		return
	}

	if exec.completeAfter == 0 || UnixTimeFloat(now) >= exec.completeAfter {
		completeAutomationLocked(exec, now)
	}
}
