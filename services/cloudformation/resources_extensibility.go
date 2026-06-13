package cloudformation

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
)

// customResourceTimeout is how long to wait for a custom resource Lambda callback.
const customResourceTimeout = 30 * time.Second

// waitConditionEmulatorTimeout is how long WaitCondition will block before auto-succeeding
// in emulator mode (i.e. no real workload has sent a signal).
const waitConditionEmulatorTimeout = 100 * time.Millisecond

// cfnCustomResourceEvent is the event payload sent to a Lambda-backed custom resource.
type cfnCustomResourceEvent struct {
	RequestType        string         `json:"RequestType"`
	ResponseURL        string         `json:"ResponseURL"`
	StackID            string         `json:"StackId"`
	RequestID          string         `json:"RequestId"`
	ResourceType       string         `json:"ResourceType"`
	LogicalResourceID  string         `json:"LogicalResourceId"`
	PhysicalResourceID string         `json:"PhysicalResourceId,omitempty"`
	ResourceProperties map[string]any `json:"ResourceProperties"`
}

// cfnCustomResourceResponse is the response a custom resource Lambda sends via HTTP PUT.
type cfnCustomResourceResponse struct {
	Status             string         `json:"Status"`
	Reason             string         `json:"Reason,omitempty"`
	PhysicalResourceID string         `json:"PhysicalResourceId"`
	StackID            string         `json:"StackId,omitempty"`
	RequestID          string         `json:"RequestId,omitempty"`
	LogicalResourceID  string         `json:"LogicalResourceId,omitempty"`
	Data               map[string]any `json:"Data,omitempty"`
	NoEcho             bool           `json:"NoEcho,omitempty"`
}

// WCSignal is a single signal received by a WaitConditionHandle.
type WCSignal struct {
	UniqueID string
	Status   string
	Data     string
	Reason   string
}

// WaitConditionStore manages wait condition signals independently of the CFN backend mutex.
// It uses its own mutex so signals can be injected from goroutines that don't hold the CFN lock.
type WaitConditionStore struct {
	mu      sync.Mutex
	sigs    map[string][]WCSignal    // handleToken → signals received
	notify  map[string][]chan struct{} // handleToken → listener channels
}

// NewWaitConditionStore creates a new empty WaitConditionStore.
func NewWaitConditionStore() *WaitConditionStore {
	return &WaitConditionStore{
		sigs:   make(map[string][]WCSignal),
		notify: make(map[string][]chan struct{}),
	}
}

// Signal records a signal for the given handle token and wakes any waiting callers.
func (s *WaitConditionStore) Signal(token string, sig WCSignal) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sigs[token] = append(s.sigs[token], sig)

	for _, ch := range s.notify[token] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// signalCount returns the number of SUCCESS signals currently stored for token.
func (s *WaitConditionStore) signalCount(token string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for _, sig := range s.sigs[token] {
		if sig.Status == "SUCCESS" {
			count++
		}
	}

	return count
}

// Wait blocks until at least count SUCCESS signals have been received for token,
// or until ctx is cancelled, or until emulatorTimeout elapses (in which case it
// succeeds anyway — no real workload is running in the emulator).
func (s *WaitConditionStore) Wait(ctx context.Context, token string, count int, emulatorTimeout time.Duration) error {
	// Fast path: signals already present.
	if s.signalCount(token) >= count {
		return nil
	}

	// Register a notification channel.
	ch := make(chan struct{}, 1)

	s.mu.Lock()
	s.notify[token] = append(s.notify[token], ch)
	s.mu.Unlock()

	// Cleanup: remove our channel when done.
	defer func() {
		s.mu.Lock()
		notifiers := s.notify[token]
		for i, c := range notifiers {
			if c == ch {
				s.notify[token] = append(notifiers[:i], notifiers[i+1:]...)
				break
			}
		}
		s.mu.Unlock()
	}()

	emTimer := time.NewTimer(emulatorTimeout)
	defer emTimer.Stop()

	for {
		select {
		case <-ch:
			if s.signalCount(token) >= count {
				return nil
			}
		case <-emTimer.C:
			// Emulator auto-success: no real workload to signal.
			return nil
		case <-ctx.Done():
			return fmt.Errorf("wait condition context cancelled: %w", ctx.Err())
		}
	}
}

// MacroRecord stores a registered CloudFormation macro.
type MacroRecord struct {
	Name        string
	FunctionARN string
	Description string
}

// ---- CustomResource / Custom::* ----

func isLambdaARN(s string) bool {
	return strings.HasPrefix(s, "arn:") && strings.Contains(s, ":lambda:")
}

func isSNSARN(s string) bool {
	return strings.HasPrefix(s, "arn:") && strings.Contains(s, ":sns:")
}

// createCustomResource handles AWS::CloudFormation::CustomResource and Custom::* resource types.
// It invokes the backing Lambda (or SNS) with the CFN custom resource event and waits for the
// response via a temporary local HTTP server that receives the cfn-response PUT callback.
func (rc *ResourceCreator) createCustomResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	serviceToken, _ := props["ServiceToken"].(string)
	if serviceToken != "" {
		serviceToken = strProp(props, "ServiceToken", params, physicalIDs)
	}

	requestID := uuid.New().String()
	// Default physical ID; overridden by Lambda response if provided.
	defaultPhysID := logicalID + "-" + requestID[:8]

	if serviceToken == "" {
		// No ServiceToken: emit a stub physical ID.
		return defaultPhysID, nil
	}

	stackID, _ := physicalIDs["_StackId"]
	if stackID == "" {
		stackID = "arn:aws:cloudformation:us-east-1:000000000000:stack/unknown/unknown"
	}

	// Start a temporary HTTP server to receive the cfn-response PUT.
	respCh := make(chan cfnCustomResourceResponse, 1)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		// No listener available; fall through to stub.
		return defaultPhysID, nil
	}

	port := ln.Addr().(*net.TCPAddr).Port
	path := "/cfn-response/" + requestID
	responseURL := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)

	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var resp cfnCustomResourceResponse
		_ = json.NewDecoder(r.Body).Decode(&resp)
		select {
		case respCh <- resp:
		default:
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Handler: mux}

	go func() { _ = srv.Serve(ln) }()

	defer func() { _ = srv.Shutdown(context.Background()) }()

	// Build the event payload.
	event := cfnCustomResourceEvent{
		RequestType:        "Create",
		ResponseURL:        responseURL,
		StackID:            stackID,
		RequestID:          requestID,
		ResourceType:       resourceType,
		LogicalResourceID:  logicalID,
		ResourceProperties: props,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return defaultPhysID, nil
	}

	// Invoke the backing service. Track whether an invocation was dispatched
	// successfully enough to expect an async callback via ResponseURL.
	expectCallback := false

	switch {
	case isLambdaARN(serviceToken) && rc.backends.Lambda != nil:
		_, statusCode, invokeErr := rc.backends.Lambda.Backend.InvokeFunction(
			ctx, serviceToken, lambdabackend.InvocationTypeEvent, payload,
		)
		// Async (Event) invocation returns 202 on successful dispatch.
		// Errors or non-2xx mean the Lambda won't send a callback.
		if invokeErr == nil && statusCode >= 200 && statusCode < 300 {
			expectCallback = true
		}
	case isSNSARN(serviceToken) && rc.backends.SNS != nil:
		_, publishErr := rc.backends.SNS.Backend.Publish(serviceToken, string(payload), "", "", map[string]snsbackend.MessageAttribute{})
		if publishErr == nil {
			expectCallback = true
		}
	}

	// If no dispatch succeeded, return stub immediately (no callback will arrive).
	if !expectCallback {
		return defaultPhysID, nil
	}

	// Wait for the Lambda to PUT the response.
	select {
	case resp := <-respCh:
		return rc.applyCustomResourceResponse(logicalID, requestID, resp, physicalIDs)
	case <-time.After(customResourceTimeout):
		// Timeout: emulator succeeds with stub ID (no real Lambda running in unit tests).
		return defaultPhysID, nil
	case <-ctx.Done():
		return defaultPhysID, nil
	}
}

// applyCustomResourceResponse processes a successful Lambda callback and stores Data outputs.
// Data attributes are stored in physicalIDs as "logicalID/Key" → value so that
// subsequent Fn::GetAtt [logicalID, Key] resolutions can retrieve them.
func (rc *ResourceCreator) applyCustomResourceResponse(
	logicalID, requestID string,
	resp cfnCustomResourceResponse,
	physicalIDs map[string]string,
) (string, error) {
	if resp.Status == "FAILED" {
		reason := resp.Reason
		if reason == "" {
			reason = "resource reported FAILED with no reason"
		}

		return "", fmt.Errorf("custom resource %s failed: %s", logicalID, reason)
	}

	physID := resp.PhysicalResourceID
	if physID == "" {
		physID = logicalID + "-" + requestID[:8]
	}

	// Store Data outputs for later Fn::GetAtt resolution.
	for k, v := range resp.Data {
		physicalIDs[logicalID+"/"+k] = fmt.Sprintf("%v", v)
	}

	return physID, nil
}

// updateCustomResource invokes the backing Lambda/SNS with an Update event for a custom resource.
// oldProps is the previous resource properties (sent as OldResourceProperties per the CFN spec).
func (rc *ResourceCreator) updateCustomResource(
	ctx context.Context,
	logicalID, resourceType, physicalID string,
	props, oldProps map[string]any,
) error {
	serviceToken, _ := props["ServiceToken"].(string)
	if serviceToken == "" {
		// Try old props if new props have no ServiceToken.
		serviceToken, _ = oldProps["ServiceToken"].(string)
	}
	if serviceToken == "" {
		return nil
	}

	requestID := uuid.New().String()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil
	}

	port := ln.Addr().(*net.TCPAddr).Port
	path := "/cfn-response/" + requestID
	responseURL := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)

	respCh := make(chan cfnCustomResourceResponse, 1)
	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var resp cfnCustomResourceResponse
		_ = json.NewDecoder(r.Body).Decode(&resp)
		select {
		case respCh <- resp:
		default:
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Handler: mux}

	go func() { _ = srv.Serve(ln) }()

	defer func() { _ = srv.Shutdown(context.Background()) }()

	// Build the Update event payload per the CFN custom resource protocol.
	type updateEvent struct {
		RequestType           string         `json:"RequestType"`
		ResponseURL           string         `json:"ResponseURL"`
		StackID               string         `json:"StackId"`
		RequestID             string         `json:"RequestId"`
		ResourceType          string         `json:"ResourceType"`
		LogicalResourceID     string         `json:"LogicalResourceId"`
		PhysicalResourceID    string         `json:"PhysicalResourceId"`
		ResourceProperties    map[string]any `json:"ResourceProperties"`
		OldResourceProperties map[string]any `json:"OldResourceProperties"`
	}

	event := updateEvent{
		RequestType:           "Update",
		ResponseURL:           responseURL,
		StackID:               "arn:aws:cloudformation:us-east-1:000000000000:stack/unknown/unknown",
		RequestID:             requestID,
		ResourceType:          resourceType,
		LogicalResourceID:     logicalID,
		PhysicalResourceID:    physicalID,
		ResourceProperties:    props,
		OldResourceProperties: oldProps,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return nil
	}

	updateExpectCallback := false

	switch {
	case isLambdaARN(serviceToken) && rc.backends.Lambda != nil:
		_, sc, invokeErr := rc.backends.Lambda.Backend.InvokeFunction(
			ctx, serviceToken, lambdabackend.InvocationTypeEvent, payload,
		)
		if invokeErr == nil && sc >= 200 && sc < 300 {
			updateExpectCallback = true
		}
	case isSNSARN(serviceToken) && rc.backends.SNS != nil:
		_, publishErr := rc.backends.SNS.Backend.Publish(serviceToken, string(payload), "", "", map[string]snsbackend.MessageAttribute{})
		if publishErr == nil {
			updateExpectCallback = true
		}
	}

	if !updateExpectCallback {
		return nil
	}

	select {
	case resp := <-respCh:
		if resp.Status == "FAILED" {
			reason := resp.Reason
			if reason == "" {
				reason = "resource reported FAILED with no reason"
			}

			return fmt.Errorf("custom resource update %s failed: %s", logicalID, reason)
		}

		return nil
	case <-time.After(customResourceTimeout):
		return nil
	case <-ctx.Done():
		return nil
	}
}

// deleteCustomResource invokes the backing Lambda/SNS with a Delete event for a custom resource.
func (rc *ResourceCreator) deleteCustomResource(
	ctx context.Context,
	logicalID, resourceType, physicalID string,
	props map[string]any,
) error {
	serviceToken, _ := props["ServiceToken"].(string)
	if serviceToken == "" {
		return nil
	}

	requestID := uuid.New().String()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil
	}

	port := ln.Addr().(*net.TCPAddr).Port
	path := "/cfn-response/" + requestID
	responseURL := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)

	respCh := make(chan cfnCustomResourceResponse, 1)
	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		var resp cfnCustomResourceResponse
		_ = json.NewDecoder(r.Body).Decode(&resp)
		select {
		case respCh <- resp:
		default:
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Handler: mux}

	go func() { _ = srv.Serve(ln) }()

	defer func() { _ = srv.Shutdown(context.Background()) }()

	event := cfnCustomResourceEvent{
		RequestType:        "Delete",
		ResponseURL:        responseURL,
		RequestID:          requestID,
		ResourceType:       resourceType,
		LogicalResourceID:  logicalID,
		PhysicalResourceID: physicalID,
		ResourceProperties: props,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return nil
	}

	deleteExpectCallback := false

	switch {
	case isLambdaARN(serviceToken) && rc.backends.Lambda != nil:
		_, sc, invokeErr := rc.backends.Lambda.Backend.InvokeFunction(
			ctx, serviceToken, lambdabackend.InvocationTypeEvent, payload,
		)
		if invokeErr == nil && sc >= 200 && sc < 300 {
			deleteExpectCallback = true
		}
	case isSNSARN(serviceToken) && rc.backends.SNS != nil:
		_, publishErr := rc.backends.SNS.Backend.Publish(serviceToken, string(payload), "", "", map[string]snsbackend.MessageAttribute{})
		if publishErr == nil {
			deleteExpectCallback = true
		}
	}

	if !deleteExpectCallback {
		return nil
	}

	// Wait for Delete response (fire-and-forget if timeout).
	select {
	case resp := <-respCh:
		if resp.Status == "FAILED" {
			return fmt.Errorf("custom resource delete %s failed: %s", logicalID, resp.Reason)
		}

		return nil
	case <-time.After(customResourceTimeout):
		return nil
	case <-ctx.Done():
		return nil
	}
}

// ---- WaitConditionHandle ----

// createWaitConditionHandle creates an AWS::CloudFormation::WaitConditionHandle.
// Returns a token used to correlate future signals from WaitCondition.
func (rc *ResourceCreator) createWaitConditionHandle(logicalID string) (string, error) {
	token := uuid.New().String()
	// Physical ID encodes the signal token so WaitCondition can retrieve it.
	physID := "wchandle-" + token

	if rc.backends.WaitConditions != nil {
		// Ensure the token entry exists in the store.
		rc.backends.WaitConditions.mu.Lock()
		if _, ok := rc.backends.WaitConditions.sigs[token]; !ok {
			rc.backends.WaitConditions.sigs[token] = nil
		}
		rc.backends.WaitConditions.mu.Unlock()
	}

	return physID, nil
}

// ---- WaitCondition ----

// createWaitCondition handles AWS::CloudFormation::WaitCondition.
// It blocks until Count signals arrive on the handle token, until the emulator
// auto-success timeout fires, or until ctx is cancelled.
func (rc *ResourceCreator) createWaitCondition(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	// Extract the handle physical ID via Handle property (a Ref to a WaitConditionHandle).
	handleRef := strProp(props, "Handle", params, physicalIDs)

	// Count is the number of SUCCESS signals required (default 1).
	count := 1
	if c := strProp(props, "Count", params, physicalIDs); c != "" {
		_, err := fmt.Sscanf(c, "%d", &count)
		if err != nil || count < 1 {
			count = 1
		}
	}

	physID := logicalID + "-wc-" + uuid.New().String()[:8]

	store := rc.backends.WaitConditions
	if store == nil {
		return physID, nil
	}

	// Extract the token from the handle physical ID ("wchandle-<token>").
	token := strings.TrimPrefix(handleRef, "wchandle-")
	if token == handleRef {
		// Handle not a recognized format; auto-succeed.
		return physID, nil
	}

	if err := store.Wait(ctx, token, count, waitConditionEmulatorTimeout); err != nil {
		return "", fmt.Errorf("WaitCondition %s: %w", logicalID, err)
	}

	return physID, nil
}

// ---- Macro ----

// createMacro handles AWS::CloudFormation::Macro resource creation.
// Registers the macro name → Lambda function ARN in the backend macro registry.
func (rc *ResourceCreator) createMacro(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	fnARN := strProp(props, "FunctionName", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	if rc.backends.MacroRegistry != nil {
		rc.backends.MacroRegistry.register(name, fnARN, description)
	}

	return name, nil
}

// deleteMacro handles AWS::CloudFormation::Macro resource deletion.
func (rc *ResourceCreator) deleteMacro(physicalID string) error {
	if rc.backends.MacroRegistry != nil {
		rc.backends.MacroRegistry.remove(physicalID)
	}

	return nil
}

// MacroRegistry stores registered CloudFormation macros.
type MacroRegistry struct {
	mu     sync.RWMutex
	macros map[string]*MacroRecord // name → record
}

// NewMacroRegistry creates an empty MacroRegistry.
func NewMacroRegistry() *MacroRegistry {
	return &MacroRegistry{macros: make(map[string]*MacroRecord)}
}

func (r *MacroRegistry) register(name, functionARN, description string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.macros[name] = &MacroRecord{
		Name:        name,
		FunctionARN: functionARN,
		Description: description,
	}
}

func (r *MacroRegistry) remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.macros, name)
}

// Get returns the macro record for name, or nil if not found.
func (r *MacroRegistry) Get(name string) *MacroRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.macros[name]
}

// InvokeMacro invokes a registered macro Lambda to transform a template fragment.
// fragmentJSON is the JSON-encoded fragment; params are the template parameter values.
// Returns the transformed fragment JSON, or fragmentJSON unmodified if the macro
// Lambda is unavailable.
func (r *MacroRegistry) InvokeMacro(
	ctx context.Context,
	lambdaBackend *lambdabackend.Handler,
	macroName string,
	fragmentJSON []byte,
	params map[string]string,
) ([]byte, error) {
	if lambdaBackend == nil {
		return fragmentJSON, nil
	}

	rec := r.Get(macroName)
	if rec == nil || rec.FunctionARN == "" {
		return fragmentJSON, nil
	}

	// Build the macro invocation event per AWS spec.
	event := map[string]any{
		"region":                   "us-east-1",
		"accountId":                "000000000000",
		"fragment":                 json.RawMessage(fragmentJSON),
		"transformId":              macroName,
		"params":                   map[string]any{},
		"templateParameterValues":  params,
		"requestedTransforms":      []string{macroName},
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fragmentJSON, nil
	}

	result, _, err := lambdaBackend.Backend.InvokeFunction(
		ctx, rec.FunctionARN, lambdabackend.InvocationTypeRequestResponse, payload,
	)
	if err != nil || len(result) == 0 {
		return fragmentJSON, nil
	}

	// Parse macro response: {"status": "success", "fragment": {...}}
	var resp struct {
		Status   string          `json:"status"`
		Fragment json.RawMessage `json:"fragment"`
		ErrorMessage string      `json:"errorMessage,omitempty"`
	}

	if err := json.Unmarshal(result, &resp); err != nil {
		return fragmentJSON, nil
	}

	if strings.EqualFold(resp.Status, "success") && len(resp.Fragment) > 0 {
		return resp.Fragment, nil
	}

	return fragmentJSON, nil
}

// createCFNExtensibilityResource dispatches CFN extensibility resource types.
func (rc *ResourceCreator) createCFNExtensibilityResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	switch {
	case resourceType == "AWS::CloudFormation::CustomResource" || strings.HasPrefix(resourceType, "Custom::"):
		return rc.createCustomResource(ctx, logicalID, resourceType, props, params, physicalIDs)
	case resourceType == "AWS::CloudFormation::WaitConditionHandle":
		return rc.createWaitConditionHandle(logicalID)
	case resourceType == "AWS::CloudFormation::WaitCondition":
		return rc.createWaitCondition(ctx, logicalID, props, params, physicalIDs)
	case resourceType == "AWS::CloudFormation::Macro":
		return rc.createMacro(logicalID, props, params, physicalIDs)
	default:
		return logicalID + "-stub", nil
	}
}

// updateCFNExtensibilityResource dispatches CFN extensibility resource type updates.
// Returns (handled, error): handled=true if the type was recognized and an update event sent.
func (rc *ResourceCreator) updateCFNExtensibilityResource(
	ctx context.Context,
	logicalID, resourceType, physicalID string,
	props, oldProps map[string]any,
) (bool, error) {
	switch {
	case resourceType == "AWS::CloudFormation::CustomResource" || strings.HasPrefix(resourceType, "Custom::"):
		return true, rc.updateCustomResource(ctx, logicalID, resourceType, physicalID, props, oldProps)
	case resourceType == "AWS::CloudFormation::WaitConditionHandle",
		resourceType == "AWS::CloudFormation::WaitCondition",
		resourceType == "AWS::CloudFormation::Macro":
		return true, nil
	default:
		return false, nil
	}
}

// deleteCFNExtensibilityResource dispatches CFN extensibility resource type deletions.
func (rc *ResourceCreator) deleteCFNExtensibilityResource(
	ctx context.Context,
	logicalID, resourceType, physicalID string,
	props map[string]any,
) (bool, error) {
	switch {
	case resourceType == "AWS::CloudFormation::CustomResource" || strings.HasPrefix(resourceType, "Custom::"):
		return true, rc.deleteCustomResource(ctx, logicalID, resourceType, physicalID, props)
	case resourceType == "AWS::CloudFormation::WaitConditionHandle":
		return true, nil
	case resourceType == "AWS::CloudFormation::WaitCondition":
		return true, nil
	case resourceType == "AWS::CloudFormation::Macro":
		return true, rc.deleteMacro(physicalID)
	default:
		return false, nil
	}
}

// getCustomResourceAttribute resolves Fn::GetAtt for a Custom resource logical ID.
// Data outputs are stored in physicalIDs as "logicalID/Key" during creation.
func getCustomResourceAttribute(logicalID, attrName string, physicalIDs map[string]string) string {
	key := logicalID + "/" + attrName
	if v, ok := physicalIDs[key]; ok {
		return v
	}

	return ""
}
