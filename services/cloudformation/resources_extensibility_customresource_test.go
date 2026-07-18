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
)

// ---- Custom Resource: nil backends (stub path) ----

func TestResourceCreator_CustomResource_NilBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		resourceType string
	}{
		{
			name:         "AWS::CloudFormation::CustomResource stub",
			resourceType: "AWS::CloudFormation::CustomResource",
			props: map[string]any{
				"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
			},
		},
		{
			name:         "Custom::MyType stub",
			resourceType: "Custom::MyType",
			props: map[string]any{
				"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
			},
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
			physID, err := rc.Create(
				t.Context(),
				"MyRes",
				tt.resourceType,
				tt.props,
				nil,
				make(map[string]string),
			)
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

	physID, err := rc.Create(
		t.Context(),
		"MyCustom",
		"Custom::Widget",
		map[string]any{
			"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:nonexistent",
		},
		nil,
		make(map[string]string),
	)

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
		invokeFunc: func(_ context.Context, _ string, _ lambdabackend.InvocationType, payload []byte) ([]byte, int, error) {
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
		err    error
		physID string
	}, 1)

	go func() {
		physID, err := rc.Create(t.Context(), "MyWidget", "Custom::Widget",
			map[string]any{
				"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:cfn-responder",
				"Color":        "blue",
			},
			nil, physIDs)
		resultCh <- struct {
			err    error
			physID string
		}{err: err, physID: physID}
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
	invokeFunc func(
		ctx context.Context,
		name string,
		invType lambdabackend.InvocationType,
		payload []byte,
	) ([]byte, int, error)
}

func (m *mockLambdaStorageBackend) CreateFunction(_ *lambdabackend.FunctionConfiguration) error {
	return nil
}

func (m *mockLambdaStorageBackend) GetFunction(
	name string,
) (*lambdabackend.FunctionConfiguration, error) {
	return nil, fmt.Errorf("%s: %w", name, errFunctionNotFound)
}

func (m *mockLambdaStorageBackend) ListFunctions(
	_ string,
	maxItems int,
) page.Page[*lambdabackend.FunctionConfiguration] {
	_ = maxItems

	return page.Page[*lambdabackend.FunctionConfiguration]{}
}

func (m *mockLambdaStorageBackend) DeleteFunction(_ string) error { return nil }
func (m *mockLambdaStorageBackend) UpdateFunction(_ *lambdabackend.FunctionConfiguration) error {
	return nil
}

func (m *mockLambdaStorageBackend) InvokeFunction(
	ctx context.Context,
	name string,
	invType lambdabackend.InvocationType,
	payload []byte,
) ([]byte, int, error) {
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

	stack, err := b.CreateStack(
		t.Context(),
		"custom-test",
		template,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
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
		invokeFunc: func(_ context.Context, _ string, _ lambdabackend.InvocationType, payload []byte) ([]byte, int, error) {
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
		err    error
		physID string
	}, 1)

	go func() {
		physID, err := rc.Create(t.Context(), "FailingResource", "Custom::Failing",
			map[string]any{
				"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:cfn-failing",
				"Input":        "bad-data",
			},
			nil, physIDs)
		resultCh <- struct {
			err    error
			physID string
		}{err: err, physID: physID}
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
		invokeFunc: func(_ context.Context, _ string, _ lambdabackend.InvocationType, payload []byte) ([]byte, int, error) {
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
		stack, err := b.CreateStack(
			t.Context(),
			"getatt-test",
			template,
			nil,
			cloudformation.StackOptions{},
		)
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

	stack, err := b.CreateStack(
		t.Context(),
		"combined-test",
		template,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
}

// ---- isCFNExtensibilityType coverage via Create ----

func TestResourceCreator_CFNExtensibilityTypes_AllHandled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		resourceType string
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
			props: map[string]any{
				"FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:fn",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtensibilityBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				"Res",
				tt.resourceType,
				tt.props,
				nil,
				make(map[string]string),
			)
			require.NoError(t, err, "extensibility type should not return error")
			assert.NotEmpty(t, physID)
		})
	}
}
