package stepfunctions

// EBStartExecutionAdapter wraps InMemoryBackend to satisfy the EventBridge
// StepFunctionsExecutor interface (which returns only error, not *Execution).
type EBStartExecutionAdapter struct {
	B *InMemoryBackend
}

// StartExecution starts an execution and discards the Execution struct,
// returning only the error so it fits the eventbridge.StepFunctionsExecutor interface.
func (a *EBStartExecutionAdapter) StartExecution(stateMachineARN, name, input string) error {
	_, err := a.B.StartExecution(stateMachineARN, name, input)

	return err
}
