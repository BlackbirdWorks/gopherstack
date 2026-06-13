package cloudformation_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
)

// newExtensibilityBackends creates a ServiceBackends suitable for extensibility tests.
func newExtensibilityBackends() *cloudformation.ServiceBackends {
	b := &cloudformation.ServiceBackends{
		WaitConditions: cloudformation.NewWaitConditionStore(),
		MacroRegistry:  cloudformation.NewMacroRegistry(),
		AccountID:      "000000000000",
		Region:         "us-east-1",
	}

	return b
}

// newExtensibilityBackendsWithLambda adds a real Lambda backend.
func newExtensibilityBackendsWithLambda() *cloudformation.ServiceBackends {
	b := newExtensibilityBackends()
	lambdaBk := lambdabackend.NewInMemoryBackend(nil, nil, lambdabackend.DefaultSettings(), "000000000000", "us-east-1")
	b.Lambda = lambdabackend.NewHandler(lambdaBk)

	return b
}

// newExtensibilityBackendsWithSNS adds a real SNS backend.
func newExtensibilityBackendsWithSNS() *cloudformation.ServiceBackends {
	b := newExtensibilityBackends()
	b.SNS = snsbackend.NewHandler(snsbackend.NewInMemoryBackend())

	return b
}

// ---- WaitConditionStore ----

func TestWaitConditionStore_SignalAndWait(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		count   int
		signals int
		wantErr bool
	}{
		{
			name:    "single signal satisfies count 1",
			count:   1,
			signals: 1,
			wantErr: false,
		},
		{
			name:    "multiple signals satisfy count 3",
			count:   3,
			signals: 3,
			wantErr: false,
		},
		{
			name:    "signals pre-loaded before wait",
			count:   2,
			signals: 2,
			wantErr: false,
		},
		{
			name:    "no signals causes emulator auto-success",
			count:   1,
			signals: 0,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := cloudformation.NewWaitConditionStore()
			token := "test-token-" + tt.name

			// Pre-load signals.
			for i := range tt.signals {
				store.Signal(token, cloudformation.WCSignal{
					UniqueID: fmt.Sprintf("signal-%d", i),
					Status:   "SUCCESS",
					Data:     "ok",
				})
			}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			err := store.Wait(ctx, token, tt.count, 50*time.Millisecond)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestWaitConditionStore_AsyncSignal(t *testing.T) {
	t.Parallel()

	store := cloudformation.NewWaitConditionStore()
	token := "async-token"

	// Signal from a goroutine after a short delay.
	go func() {
		time.Sleep(20 * time.Millisecond)
		store.Signal(token, cloudformation.WCSignal{UniqueID: "u1", Status: "SUCCESS", Data: "data"})
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Use a large emulator timeout so we wait for the goroutine signal.
	err := store.Wait(ctx, token, 1, 2*time.Second)
	require.NoError(t, err)
}

func TestWaitConditionStore_ContextCancel(t *testing.T) {
	t.Parallel()

	store := cloudformation.NewWaitConditionStore()
	token := "cancel-token"

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel immediately

	// With a very large emulator timeout, cancelled context should return error.
	err := store.Wait(ctx, token, 1, 24*time.Hour)
	require.Error(t, err)
}

// ---- Custom Resource: nil backends (stub path) ----

func TestResourceCreator_CustomResource_NilBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
		props        map[string]any
	}{
		{
			name:         "AWS::CloudFormation::CustomResource stub",
			resourceType: "AWS::CloudFormation::CustomResource",
			props:        map[string]any{"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:my-fn"},
		},
		{
			name:         "Custom::MyType stub",
			resourceType: "Custom::MyType",
			props:        map[string]any{"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:my-fn"},
		},
		{
			name:         "Custom::Resource no ServiceToken",
			resourceType: "Custom::Resource",
			props:        map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := cloudformation.NewResourceCreator(nil)
			physID, err := rc.Create(t.Context(), "MyRes", tt.resourceType, tt.props, nil, make(map[string]string))
			require.NoError(t, err)
			assert.NotEmpty(t, physID)
		})
	}
}

// ---- Custom Resource: with real Lambda backend ----

func TestResourceCreator_CustomResource_LambdaBackend_NoFunctionRegistered(t *testing.T) {
	t.Parallel()

	// Lambda backend has no function registered → invoke will fail silently → stub physID.
	backends := newExtensibilityBackendsWithLambda()
	rc := cloudformation.NewResourceCreator(backends)

	physID, err := rc.Create(t.Context(), "MyCustom", "Custom::Widget",
		map[string]any{"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:nonexistent"},
		nil, make(map[string]string))

	require.NoError(t, err)
	assert.NotEmpty(t, physID)
}

// TestResourceCreator_CustomResource_ResponseURL_RoundTrip verifies that a custom
// resource invocation that externally PUTs to the ResponseURL returns the Data outputs.
// We simulate the Lambda callback by directly sending an HTTP PUT to the ResponseURL
// that the resource creator hosts during Create.
func TestResourceCreator_CustomResource_ResponseURL_RoundTrip(t *testing.T) {
	t.Parallel()

	// We need to intercept the ResponseURL before the resource creator times out.
	// Strategy: use the SNS path (SNS topic doesn't exist → publish errors silently),
	// and separately start a goroutine that reads the lambda event and PUTs to the URL.
	//
	// Instead, build a Lambda backend with a mock StorageBackend that captures the
	// ResponseURL from the invocation payload and sends a SUCCESS response.

	capturedURL := make(chan string, 1)

	mockLambda := &mockLambdaStorageBackend{
		invokeFunc: func(ctx context.Context, _ string, _ lambdabackend.InvocationType, payload []byte) ([]byte, int, error) {
			var event struct {
				ResponseURL string `json:"ResponseURL"`
			}
			if err := json.Unmarshal(payload, &event); err == nil && event.ResponseURL != "" {
				select {
				case capturedURL <- event.ResponseURL:
				default:
				}
			}

			return nil, 200, nil
		},
	}

	backends := newExtensibilityBackends()
	backends.Lambda = lambdabackend.NewHandler(mockLambda)

	rc := cloudformation.NewResourceCreator(backends)

	physIDs := map[string]string{
		"_StackId": "arn:aws:cloudformation:us-east-1:000000000000:stack/my-stack/abc",
	}

	// Run Create in background.
	resultCh := make(chan struct {
		physID string
		err    error
	}, 1)

	go func() {
		physID, err := rc.Create(t.Context(), "MyWidget", "Custom::Widget",
			map[string]any{
				"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:cfn-responder",
				"Color":        "blue",
			},
			nil, physIDs)
		resultCh <- struct {
			physID string
			err    error
		}{physID, err}
	}()

	// Wait for the ResponseURL to be captured, then send a SUCCESS response.
	select {
	case url := <-capturedURL:
		resp := map[string]any{
			"Status":             "SUCCESS",
			"PhysicalResourceId": "my-widget-phys-001",
			"Data":               map[string]any{"BucketName": "my-bucket"},
		}

		body, _ := json.Marshal(resp)
		client := &http.Client{Timeout: 5 * time.Second}
		putReq, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, url,
			newBytesReader(body))
		putReq.ContentLength = int64(len(body))

		res, err := client.Do(putReq)
		require.NoError(t, err)
		res.Body.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for ResponseURL")
	}

	// Collect the Create result.
	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		assert.Equal(t, "my-widget-phys-001", result.physID)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Create result")
	}
}

// mockLambdaStorageBackend is a minimal StorageBackend that captures invocations.
type mockLambdaStorageBackend struct {
	invokeFunc func(ctx context.Context, name string, invType lambdabackend.InvocationType, payload []byte) ([]byte, int, error)
}

func (m *mockLambdaStorageBackend) CreateFunction(fn *lambdabackend.FunctionConfiguration) error {
	return nil
}

func (m *mockLambdaStorageBackend) GetFunction(name string) (*lambdabackend.FunctionConfiguration, error) {
	return nil, fmt.Errorf("not found: %s", name)
}

func (m *mockLambdaStorageBackend) ListFunctions(marker string, maxItems int) page.Page[*lambdabackend.FunctionConfiguration] {
	return page.Page[*lambdabackend.FunctionConfiguration]{}
}

func (m *mockLambdaStorageBackend) DeleteFunction(name string) error   { return nil }
func (m *mockLambdaStorageBackend) UpdateFunction(_ *lambdabackend.FunctionConfiguration) error {
	return nil
}

func (m *mockLambdaStorageBackend) InvokeFunction(ctx context.Context, name string, invType lambdabackend.InvocationType, payload []byte) ([]byte, int, error) {
	if m.invokeFunc != nil {
		return m.invokeFunc(ctx, name, invType, payload)
	}

	return nil, 200, nil
}

func (m *mockLambdaStorageBackend) Purge(_ context.Context, _ time.Time) {}

// newBytesReader returns an io.Reader that reads from b.
func newBytesReader(b []byte) *bytesReader {
	return &bytesReader{b: b}
}

type bytesReader struct {
	b   []byte
	pos int
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.b) {
		return 0, io.EOF
	}

	n := copy(p, r.b[r.pos:])
	r.pos += n

	return n, nil
}

// ---- WaitConditionHandle + WaitCondition ----

func TestResourceCreator_WaitConditionHandle_ReturnsPhysID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "basic handle creation"},
		{name: "second handle has unique ID"},
	}

	backends := newExtensibilityBackends()
	rc := cloudformation.NewResourceCreator(backends)

	seen := map[string]bool{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			physID, err := rc.Create(t.Context(), "MyHandle", "AWS::CloudFormation::WaitConditionHandle",
				nil, nil, make(map[string]string))
			require.NoError(t, err)
			assert.NotEmpty(t, physID)
			assert.False(t, seen[physID], "physID must be unique across creations")
			seen[physID] = true
		})
	}
}

func TestResourceCreator_WaitCondition_AutoSucceedsWithNoSignals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count string
	}{
		{name: "count 1 auto-succeeds", count: "1"},
		{name: "count 3 auto-succeeds", count: "3"},
		{name: "count default auto-succeeds", count: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtensibilityBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physIDs := make(map[string]string)

			// Create handle first.
			handlePhysID, err := rc.Create(t.Context(), "MyHandle", "AWS::CloudFormation::WaitConditionHandle",
				nil, nil, physIDs)
			require.NoError(t, err)

			physIDs["MyHandle"] = handlePhysID

			props := map[string]any{
				"Handle":  handlePhysID,
				"Timeout": "300",
			}
			if tt.count != "" {
				props["Count"] = tt.count
			}

			wcPhysID, err := rc.Create(t.Context(), "MyWC", "AWS::CloudFormation::WaitCondition",
				props, nil, physIDs)
			require.NoError(t, err)
			assert.NotEmpty(t, wcPhysID)
		})
	}
}

func TestResourceCreator_WaitCondition_SucceedsWithPreloadedSignal(t *testing.T) {
	t.Parallel()

	backends := newExtensibilityBackends()
	rc := cloudformation.NewResourceCreator(backends)

	physIDs := make(map[string]string)

	// Create handle.
	handlePhysID, err := rc.Create(t.Context(), "MyHandle", "AWS::CloudFormation::WaitConditionHandle",
		nil, nil, physIDs)
	require.NoError(t, err)

	physIDs["MyHandle"] = handlePhysID

	// Pre-signal the handle (extract token from physID "wchandle-<token>").
	token := handlePhysID[len("wchandle-"):]
	backends.WaitConditions.Signal(token, cloudformation.WCSignal{
		UniqueID: "signal-1",
		Status:   "SUCCESS",
		Data:     "ok",
	})

	wcPhysID, err := rc.Create(t.Context(), "MyWC", "AWS::CloudFormation::WaitCondition",
		map[string]any{
			"Handle":  handlePhysID,
			"Count":   "1",
			"Timeout": "300",
		},
		nil, physIDs)

	require.NoError(t, err)
	assert.NotEmpty(t, wcPhysID)
}

// ---- Macro ----

func TestResourceCreator_Macro_Registration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		logicalID   string
		props       map[string]any
		wantMacName string
	}{
		{
			name:        "explicit Name property",
			logicalID:   "MyMacro",
			props:       map[string]any{"Name": "MyCustomMacro", "FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:macro-fn"},
			wantMacName: "MyCustomMacro",
		},
		{
			name:        "Name defaults to logicalID",
			logicalID:   "FallbackMacro",
			props:       map[string]any{"FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:macro-fn"},
			wantMacName: "FallbackMacro",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtensibilityBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::CloudFormation::Macro",
				tt.props, nil, make(map[string]string))
			require.NoError(t, err)
			assert.Equal(t, tt.wantMacName, physID)

			rec := backends.MacroRegistry.Get(tt.wantMacName)
			require.NotNil(t, rec)
			assert.Equal(t, tt.wantMacName, rec.Name)
		})
	}
}

func TestResourceCreator_Macro_Delete(t *testing.T) {
	t.Parallel()

	backends := newExtensibilityBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Register a macro.
	_, err := rc.Create(t.Context(), "MyMacro", "AWS::CloudFormation::Macro",
		map[string]any{"Name": "MyMacro", "FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:fn"},
		nil, make(map[string]string))
	require.NoError(t, err)

	rec := backends.MacroRegistry.Get("MyMacro")
	require.NotNil(t, rec, "macro should be registered")

	// Delete it.
	err = rc.Delete(t.Context(), "AWS::CloudFormation::Macro", "MyMacro", nil)
	require.NoError(t, err)

	rec = backends.MacroRegistry.Get("MyMacro")
	assert.Nil(t, rec, "macro should be removed after delete")
}

// ---- Full stack: WaitCondition in CFN template ----

func TestStack_WaitConditionHandle_InTemplate(t *testing.T) {
	t.Parallel()

	backends := newExtensibilityBackends()
	rc := cloudformation.NewResourceCreator(backends)
	b := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", rc)

	template := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "WaitHandle": {
      "Type": "AWS::CloudFormation::WaitConditionHandle"
    },
    "WaitCond": {
      "Type": "AWS::CloudFormation::WaitCondition",
      "DependsOn": ["WaitHandle"],
      "Properties": {
        "Handle": {"Ref": "WaitHandle"},
        "Count": "1",
        "Timeout": "10"
      }
    }
  }
}`

	stack, err := b.CreateStack(t.Context(), "wc-test", template, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
}

// ---- Full stack: CustomResource (no Lambda) in CFN template ----

func TestStack_CustomResource_NoLambda_StubSuccess(t *testing.T) {
	t.Parallel()

	// Without a Lambda backend, custom resource creation stubs out with a physID.
	backends := newExtensibilityBackends()
	rc := cloudformation.NewResourceCreator(backends)
	b := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", rc)

	template := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyCustom": {
      "Type": "Custom::MyWidget",
      "Properties": {
        "ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:nonexistent",
        "Color": "red"
      }
    }
  }
}`

	stack, err := b.CreateStack(t.Context(), "custom-test", template, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
}

// ---- Full stack: Macro registration in CFN template ----

func TestStack_Macro_RegisteredViaTemplate(t *testing.T) {
	t.Parallel()

	backends := newExtensibilityBackends()
	rc := cloudformation.NewResourceCreator(backends)
	b := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", rc)

	template := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyMacro": {
      "Type": "AWS::CloudFormation::Macro",
      "Properties": {
        "Name": "StringTransform",
        "FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:string-transform"
      }
    }
  }
}`

	stack, err := b.CreateStack(t.Context(), "macro-test", template, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

	rec := backends.MacroRegistry.Get("StringTransform")
	require.NotNil(t, rec)
	assert.Equal(t, "StringTransform", rec.Name)
	assert.Contains(t, rec.FunctionARN, "string-transform")
}

// ---- SNS-backed custom resource ----

func TestResourceCreator_CustomResource_SNSBacked(t *testing.T) {
	t.Parallel()

	backends := newExtensibilityBackendsWithSNS()
	rc := cloudformation.NewResourceCreator(backends)

	// SNS topic doesn't exist → publish fails silently → stub physID.
	physID, err := rc.Create(t.Context(), "MyCustom", "AWS::CloudFormation::CustomResource",
		map[string]any{
			"ServiceToken": "arn:aws:sns:us-east-1:000000000000:my-topic",
			"Key":          "Value",
		},
		nil, make(map[string]string))

	require.NoError(t, err)
	assert.NotEmpty(t, physID)
}

// ---- Custom Resource: FAILED response ----

func TestResourceCreator_CustomResource_FAILEDResponse(t *testing.T) {
	t.Parallel()

	capturedURL := make(chan string, 1)

	mockLambda := &mockLambdaStorageBackend{
		invokeFunc: func(ctx context.Context, _ string, _ lambdabackend.InvocationType, payload []byte) ([]byte, int, error) {
			var event struct {
				ResponseURL string `json:"ResponseURL"`
			}
			if err := json.Unmarshal(payload, &event); err == nil {
				select {
				case capturedURL <- event.ResponseURL:
				default:
				}
			}
			return nil, 200, nil
		},
	}

	backends := newExtensibilityBackends()
	backends.Lambda = lambdabackend.NewHandler(mockLambda)
	rc := cloudformation.NewResourceCreator(backends)

	physIDs := map[string]string{
		"_StackId": "arn:aws:cloudformation:us-east-1:000000000000:stack/test/abc",
	}

	resultCh := make(chan struct {
		physID string
		err    error
	}, 1)

	go func() {
		physID, err := rc.Create(t.Context(), "FailingResource", "Custom::Failing",
			map[string]any{
				"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:cfn-failing",
				"Input":        "bad-data",
			},
			nil, physIDs)
		resultCh <- struct {
			physID string
			err    error
		}{physID, err}
	}()

	select {
	case url := <-capturedURL:
		resp := map[string]any{
			"Status":             "FAILED",
			"Reason":             "Validation error: bad-data is not allowed",
			"PhysicalResourceId": "failing-resource-001",
		}
		body, _ := json.Marshal(resp)
		client := &http.Client{Timeout: 5 * time.Second}
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, url, newBytesReader(body))
		req.ContentLength = int64(len(body))
		res, err := client.Do(req)
		require.NoError(t, err)
		res.Body.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for ResponseURL")
	}

	select {
	case result := <-resultCh:
		require.Error(t, result.err, "FAILED response should return error")
		assert.Contains(t, result.err.Error(), "bad-data is not allowed")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Create result")
	}
}

// ---- Custom Resource: Data outputs via GetAtt ----

func TestStack_CustomResource_GetAtt_DataOutputs(t *testing.T) {
	t.Parallel()

	capturedURL := make(chan string, 1)

	mockLambda := &mockLambdaStorageBackend{
		invokeFunc: func(ctx context.Context, _ string, _ lambdabackend.InvocationType, payload []byte) ([]byte, int, error) {
			var event struct {
				ResponseURL string `json:"ResponseURL"`
			}
			if err := json.Unmarshal(payload, &event); err == nil {
				select {
				case capturedURL <- event.ResponseURL:
				default:
				}
			}
			return nil, 200, nil
		},
	}

	backends := newExtensibilityBackends()
	backends.Lambda = lambdabackend.NewHandler(mockLambda)
	rc := cloudformation.NewResourceCreator(backends)
	b := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", rc)

	template := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyCustom": {
      "Type": "Custom::Bucket",
      "Properties": {
        "ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:cfn-data-fn",
        "BucketPrefix": "my-prefix"
      }
    }
  },
  "Outputs": {
    "BucketName": {
      "Value": { "Fn::GetAtt": ["MyCustom", "BucketName"] }
    }
  }
}`

	// Create in background; then simulate Lambda callback.
	stackCh := make(chan struct {
		stack *cloudformation.Stack
		err   error
	}, 1)

	go func() {
		stack, err := b.CreateStack(t.Context(), "getatt-test", template, nil, cloudformation.StackOptions{})
		stackCh <- struct {
			stack *cloudformation.Stack
			err   error
		}{stack, err}
	}()

	select {
	case url := <-capturedURL:
		resp := map[string]any{
			"Status":             "SUCCESS",
			"PhysicalResourceId": "bucket-resource-phys",
			"Data": map[string]any{
				"BucketName": "my-prefix-generated-bucket",
				"BucketArn":  "arn:aws:s3:::my-prefix-generated-bucket",
			},
		}
		body, _ := json.Marshal(resp)
		client := &http.Client{Timeout: 5 * time.Second}
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, url, newBytesReader(body))
		req.ContentLength = int64(len(body))
		res, err := client.Do(req)
		require.NoError(t, err)
		res.Body.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for ResponseURL")
	}

	select {
	case result := <-stackCh:
		require.NoError(t, result.err)
		require.NotNil(t, result.stack)
		assert.Equal(t, "CREATE_COMPLETE", result.stack.StackStatus)

		found := false
		for _, out := range result.stack.Outputs {
			if out.OutputKey == "BucketName" {
				assert.Equal(t, "my-prefix-generated-bucket", out.OutputValue)
				found = true
				break
			}
		}
		assert.True(t, found, "BucketName output should exist in stack outputs")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for CreateStack result")
	}
}

// ---- MacroRegistry: InvokeMacro with mock Lambda ----

func TestMacroRegistry_InvokeMacro_WithMockLambda(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		macroName    string
		fragment     map[string]any
		lambdaResp   map[string]any
		wantFragment map[string]any
	}{
		{
			name:      "macro transforms fragment successfully",
			macroName: "UpperCase",
			fragment:  map[string]any{"key": "value"},
			lambdaResp: map[string]any{
				"status":   "success",
				"fragment": map[string]any{"KEY": "VALUE"},
			},
			wantFragment: map[string]any{"KEY": "VALUE"},
		},
		{
			name:      "macro returns failure passes through original",
			macroName: "BrokenMacro",
			fragment:  map[string]any{"original": true},
			lambdaResp: map[string]any{
				"status":       "failure",
				"errorMessage": "macro processing error",
			},
			wantFragment: map[string]any{"original": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockLambda := &mockLambdaStorageBackend{
				invokeFunc: func(ctx context.Context, _ string, _ lambdabackend.InvocationType, payload []byte) ([]byte, int, error) {
					respBody, _ := json.Marshal(tt.lambdaResp)
					return respBody, 200, nil
				},
			}

			registry := cloudformation.NewMacroRegistry()
			registry.RegisterForTest(tt.macroName, "arn:aws:lambda:us-east-1:000000000000:function:macro-fn", "")

			fragmentJSON, _ := json.Marshal(tt.fragment)
			result, err := registry.InvokeMacro(t.Context(), lambdabackend.NewHandler(mockLambda), tt.macroName, fragmentJSON, nil)
			require.NoError(t, err)

			var got map[string]any
			require.NoError(t, json.Unmarshal(result, &got))
			assert.Equal(t, tt.wantFragment, got)
		})
	}
}

// ---- MacroRegistry: InvokeMacro no Lambda ----

func TestMacroRegistry_InvokeMacro_NoLambdaBackend(t *testing.T) {
	t.Parallel()

	registry := cloudformation.NewMacroRegistry()
	registry.RegisterForTest("MyMacro", "arn:aws:lambda:us-east-1:000000000000:function:fn", "")

	fragment := map[string]any{"unchanged": true}
	fragmentJSON, _ := json.Marshal(fragment)

	// nil lambda backend → returns fragment unchanged.
	result, err := registry.InvokeMacro(t.Context(), nil, "MyMacro", fragmentJSON, nil)
	require.NoError(t, err)
	assert.Equal(t, fragmentJSON, result)
}

// ---- MacroRegistry: InvokeMacro unknown macro ----

func TestMacroRegistry_InvokeMacro_UnknownMacro(t *testing.T) {
	t.Parallel()

	registry := cloudformation.NewMacroRegistry()

	fragment := map[string]any{"pass": "through"}
	fragmentJSON, _ := json.Marshal(fragment)

	mockLambda := &mockLambdaStorageBackend{}
	result, err := registry.InvokeMacro(t.Context(), lambdabackend.NewHandler(mockLambda), "DoesNotExist", fragmentJSON, nil)
	require.NoError(t, err)
	assert.Equal(t, fragmentJSON, result)
}

// ---- WaitConditionStore: token isolation ----

func TestWaitConditionStore_TokenIsolation(t *testing.T) {
	t.Parallel()

	store := cloudformation.NewWaitConditionStore()

	// Signal token-A but not token-B.
	store.Signal("token-A", cloudformation.WCSignal{UniqueID: "u1", Status: "SUCCESS"})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// token-A should succeed immediately.
	err := store.Wait(ctx, "token-A", 1, 50*time.Millisecond)
	require.NoError(t, err)

	// token-B has no signals: auto-succeeds via emulator timeout.
	err = store.Wait(ctx, "token-B", 1, 30*time.Millisecond)
	require.NoError(t, err)
}

// ---- WaitConditionStore: FAILURE signal not counted ----

func TestWaitConditionStore_FailureSignalNotCounted(t *testing.T) {
	t.Parallel()

	store := cloudformation.NewWaitConditionStore()

	// Send a FAILURE signal — should not count toward SUCCESS count.
	store.Signal("token", cloudformation.WCSignal{UniqueID: "f1", Status: "FAILURE", Reason: "deploy failed"})

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	// Only SUCCESS counts; emulator auto-succeed after short timeout.
	err := store.Wait(ctx, "token", 1, 30*time.Millisecond)
	require.NoError(t, err, "emulator auto-succeed should fire since no SUCCESS signals")
}

// ---- Stack: WaitCondition + Custom resource in same template ----

func TestStack_WaitConditionAndCustomResource_Combined(t *testing.T) {
	t.Parallel()

	backends := newExtensibilityBackends()
	rc := cloudformation.NewResourceCreator(backends)
	b := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", rc)

	template := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "WaitHandle": {
      "Type": "AWS::CloudFormation::WaitConditionHandle"
    },
    "WaitCond": {
      "Type": "AWS::CloudFormation::WaitCondition",
      "DependsOn": ["WaitHandle"],
      "Properties": {
        "Handle": {"Ref": "WaitHandle"},
        "Count": "1",
        "Timeout": "10"
      }
    },
    "MyCustomResource": {
      "Type": "Custom::Widget",
      "Properties": {
        "ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:absent",
        "Color": "blue"
      }
    },
    "MyMacro": {
      "Type": "AWS::CloudFormation::Macro",
      "Properties": {
        "Name": "MyTransform",
        "FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:transform-fn"
      }
    }
  }
}`

	stack, err := b.CreateStack(t.Context(), "combined-test", template, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
}

// ---- Custom resource: Delete sends correct event ----

func TestResourceCreator_CustomResource_DeleteEvent(t *testing.T) {
	t.Parallel()

	type capturedEvent struct {
		RequestType        string `json:"RequestType"`
		PhysicalResourceID string `json:"PhysicalResourceId"`
		LogicalResourceID  string `json:"LogicalResourceId"`
		ResourceType       string `json:"ResourceType"`
	}

	events := make(chan capturedEvent, 1)

	mockLambda := &mockLambdaStorageBackend{
		invokeFunc: func(ctx context.Context, _ string, _ lambdabackend.InvocationType, payload []byte) ([]byte, int, error) {
			var ev struct {
				RequestType        string `json:"RequestType"`
				PhysicalResourceID string `json:"PhysicalResourceId"`
				LogicalResourceID  string `json:"LogicalResourceId"`
				ResourceType       string `json:"ResourceType"`
				ResponseURL        string `json:"ResponseURL"`
			}
			if err := json.Unmarshal(payload, &ev); err == nil {
				select {
				case events <- capturedEvent{
					RequestType:        ev.RequestType,
					PhysicalResourceID: ev.PhysicalResourceID,
					LogicalResourceID:  ev.LogicalResourceID,
					ResourceType:       ev.ResourceType,
				}:
				default:
				}
				// Simulate immediate response for Delete (fire-and-forget in real CFN).
				if ev.ResponseURL != "" {
					go func(url string) {
						resp := map[string]any{
							"Status":             "SUCCESS",
							"PhysicalResourceId": ev.PhysicalResourceID,
						}
						body, _ := json.Marshal(resp)
						client := &http.Client{Timeout: 2 * time.Second}
						req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, newBytesReader(body))
						if err != nil {
							return
						}
						req.ContentLength = int64(len(body))
						res, err := client.Do(req)
						if err == nil {
							res.Body.Close()
						}
					}(ev.ResponseURL)
				}
			}
			return nil, 200, nil
		},
	}

	backends := newExtensibilityBackends()
	backends.Lambda = lambdabackend.NewHandler(mockLambda)
	rc := cloudformation.NewResourceCreator(backends)

	err := rc.Delete(t.Context(), "Custom::Widget", "widget-phys-001",
		map[string]any{
			"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:cfn-fn",
			"Color":        "red",
		})
	require.NoError(t, err)

	select {
	case ev := <-events:
		assert.Equal(t, "Delete", ev.RequestType, "Delete should send RequestType=Delete")
		assert.Equal(t, "widget-phys-001", ev.PhysicalResourceID)
		assert.Equal(t, "Custom::Widget", ev.ResourceType)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Delete event")
	}
}

// ---- Custom Resource: Update lifecycle ----

func TestResourceCreator_CustomResource_UpdateEvent(t *testing.T) {
	t.Parallel()

	type capturedUpdate struct {
		RequestType           string         `json:"RequestType"`
		PhysicalResourceID    string         `json:"PhysicalResourceId"`
		ResourceProperties    map[string]any `json:"ResourceProperties"`
		OldResourceProperties map[string]any `json:"OldResourceProperties"`
	}

	updates := make(chan capturedUpdate, 1)

	mockLambda := &mockLambdaStorageBackend{
		invokeFunc: func(ctx context.Context, _ string, _ lambdabackend.InvocationType, payload []byte) ([]byte, int, error) {
			var ev struct {
				RequestType           string         `json:"RequestType"`
				PhysicalResourceID    string         `json:"PhysicalResourceId"`
				ResourceProperties    map[string]any `json:"ResourceProperties"`
				OldResourceProperties map[string]any `json:"OldResourceProperties"`
				ResponseURL           string         `json:"ResponseURL"`
			}
			if err := json.Unmarshal(payload, &ev); err == nil && ev.RequestType == "Update" {
				select {
				case updates <- capturedUpdate{
					RequestType:           ev.RequestType,
					PhysicalResourceID:    ev.PhysicalResourceID,
					ResourceProperties:    ev.ResourceProperties,
					OldResourceProperties: ev.OldResourceProperties,
				}:
				default:
				}
				// Respond immediately via goroutine.
				if ev.ResponseURL != "" {
					go func(url string) {
						resp := map[string]any{
							"Status":             "SUCCESS",
							"PhysicalResourceId": ev.PhysicalResourceID,
						}
						body, _ := json.Marshal(resp)
						client := &http.Client{Timeout: 2 * time.Second}
						req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, newBytesReader(body))
						if err != nil {
							return
						}
						req.ContentLength = int64(len(body))
						res, err := client.Do(req)
						if err == nil {
							res.Body.Close()
						}
					}(ev.ResponseURL)
				}
			}
			return nil, 200, nil
		},
	}

	backends := newExtensibilityBackends()
	backends.Lambda = lambdabackend.NewHandler(mockLambda)
	rc := cloudformation.NewResourceCreator(backends)

	err := rc.Update(t.Context(), "MyWidget", "Custom::Widget", "widget-phys-001",
		map[string]any{
			"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:cfn-fn",
			"Color":        "green",
		},
		map[string]any{
			"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:cfn-fn",
			"Color":        "blue",
		},
	)
	require.NoError(t, err)

	select {
	case ev := <-updates:
		assert.Equal(t, "Update", ev.RequestType)
		assert.Equal(t, "widget-phys-001", ev.PhysicalResourceID)
		assert.Equal(t, "green", ev.ResourceProperties["Color"])
		assert.Equal(t, "blue", ev.OldResourceProperties["Color"])
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Update event")
	}
}

func TestResourceCreator_Update_NonExtensibilityType_IsNoop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
	}{
		{name: "S3 bucket", resourceType: "AWS::S3::Bucket"},
		{name: "DynamoDB table", resourceType: "AWS::DynamoDB::Table"},
		{name: "SNS topic", resourceType: "AWS::SNS::Topic"},
		{name: "unknown type", resourceType: "AWS::Unknown::Resource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtensibilityBackends()
			rc := cloudformation.NewResourceCreator(backends)

			err := rc.Update(t.Context(), "Res", tt.resourceType, "phys-001",
				map[string]any{"Key": "new"},
				map[string]any{"Key": "old"},
			)
			require.NoError(t, err, "Update of non-extensibility type should be a no-op")
		})
	}
}

func TestStack_CustomResource_UpdateStack_SendsUpdateEvent(t *testing.T) {
	t.Parallel()

	requestTypes := make(chan string, 10)

	mockLambda := &mockLambdaStorageBackend{
		invokeFunc: func(ctx context.Context, _ string, _ lambdabackend.InvocationType, payload []byte) ([]byte, int, error) {
			var ev struct {
				RequestType        string `json:"RequestType"`
				PhysicalResourceID string `json:"PhysicalResourceId"`
				ResponseURL        string `json:"ResponseURL"`
			}
			if err := json.Unmarshal(payload, &ev); err == nil {
				select {
				case requestTypes <- ev.RequestType:
				default:
				}
				if ev.ResponseURL != "" {
					go func(url, physID string) {
						resp := map[string]any{
							"Status":             "SUCCESS",
							"PhysicalResourceId": physID,
						}
						body, _ := json.Marshal(resp)
						client := &http.Client{Timeout: 2 * time.Second}
						req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, newBytesReader(body))
						if err != nil {
							return
						}
						req.ContentLength = int64(len(body))
						res, err := client.Do(req)
						if err == nil {
							res.Body.Close()
						}
					}(ev.ResponseURL, ev.PhysicalResourceID)
				}
			}
			return nil, 200, nil
		},
	}

	backends := newExtensibilityBackends()
	backends.Lambda = lambdabackend.NewHandler(mockLambda)
	rc := cloudformation.NewResourceCreator(backends)
	b := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", rc)

	templateV1 := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyWidget": {
      "Type": "Custom::Widget",
      "Properties": {
        "ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:widget-fn",
        "Color": "blue"
      }
    }
  }
}`
	templateV2 := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyWidget": {
      "Type": "Custom::Widget",
      "Properties": {
        "ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:widget-fn",
        "Color": "green"
      }
    }
  }
}`

	// Create.
	_, err := b.CreateStack(t.Context(), "update-test", templateV1, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	// Drain the Create event.
	select {
	case rt := <-requestTypes:
		assert.Equal(t, "Create", rt)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Create event")
	}

	// Update with changed properties.
	_, err = b.UpdateStack(t.Context(), "update-test", templateV2, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	// Should receive an Update event.
	select {
	case rt := <-requestTypes:
		assert.Equal(t, "Update", rt, "UpdateStack should send RequestType=Update for Custom::")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Update event from UpdateStack")
	}
}

// ---- isCFNExtensibilityType coverage via Create ----

func TestResourceCreator_CFNExtensibilityTypes_AllHandled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
		props        map[string]any
	}{
		{
			name:         "AWS::CloudFormation::CustomResource",
			resourceType: "AWS::CloudFormation::CustomResource",
			props:        map[string]any{},
		},
		{
			name:         "Custom::Foo",
			resourceType: "Custom::Foo",
			props:        map[string]any{},
		},
		{
			name:         "AWS::CloudFormation::WaitConditionHandle",
			resourceType: "AWS::CloudFormation::WaitConditionHandle",
			props:        map[string]any{},
		},
		{
			name:         "AWS::CloudFormation::WaitCondition",
			resourceType: "AWS::CloudFormation::WaitCondition",
			props:        map[string]any{"Count": "1"},
		},
		{
			name:         "AWS::CloudFormation::Macro",
			resourceType: "AWS::CloudFormation::Macro",
			props:        map[string]any{"FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:fn"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtensibilityBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), "Res", tt.resourceType, tt.props, nil, make(map[string]string))
			require.NoError(t, err, "extensibility type should not return error")
			assert.NotEmpty(t, physID)
		})
	}
}
