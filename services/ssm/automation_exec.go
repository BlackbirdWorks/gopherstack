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
	Name   string `json:"name"`
	Action string `json:"action"`
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
		})
	}

	return out
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
	if doc, ok := b.documentsStore(region)[docName]; ok {
		content = doc.Content
	}

	return extractAutomationSteps(docName, content)
}

// completeAutomationLocked drives every step to Success and marks the execution
// Success with an end time. Must be called with b.mu held.
func completeAutomationLocked(exec *AutomationExecution, now time.Time) {
	for i := range exec.Steps {
		start := now
		end := now
		exec.Steps[i].StepStatus = automationStatusSuccess
		exec.Steps[i].ExecutionStartTime = &start
		exec.Steps[i].ExecutionEndTime = &end
	}

	exec.Status = automationStatusSuccess
	exec.completeAfter = 0
	exec.EndTime = &now
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
