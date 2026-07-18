package cloudformation_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

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

			if err := json.Unmarshal(payload, &ev); err != nil {
				return nil, 0, err
			}

			select {
			case events <- capturedEvent{
				RequestType:        ev.RequestType,
				PhysicalResourceID: ev.PhysicalResourceID,
				LogicalResourceID:  ev.LogicalResourceID,
				ResourceType:       ev.ResourceType,
			}:
			default:
			}

			if ev.ResponseURL == "" {
				return nil, 200, nil
			}

			// Simulate immediate response for Delete (fire-and-forget in real CFN).
			go func(url string) {
				resp := map[string]any{
					"Status":             "SUCCESS",
					"PhysicalResourceId": ev.PhysicalResourceID,
				}
				body, _ := json.Marshal(resp)
				client := &http.Client{Timeout: 2 * time.Second}
				req, reqErr := http.NewRequestWithContext(
					ctx,
					http.MethodPut,
					url,
					newBytesReader(body),
				)
				if reqErr != nil {
					return
				}
				req.ContentLength = int64(len(body))
				res, doErr := client.Do(req)
				if doErr == nil {
					res.Body.Close()
				}
			}(ev.ResponseURL)

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
		ResourceProperties    map[string]any `json:"ResourceProperties"`
		OldResourceProperties map[string]any `json:"OldResourceProperties"`
		RequestType           string         `json:"RequestType"`
		PhysicalResourceID    string         `json:"PhysicalResourceId"`
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

			if err := json.Unmarshal(payload, &ev); err != nil {
				return nil, 0, err
			}

			if ev.RequestType != "Update" {
				return nil, 200, nil
			}

			select {
			case updates <- capturedUpdate{
				RequestType:           ev.RequestType,
				PhysicalResourceID:    ev.PhysicalResourceID,
				ResourceProperties:    ev.ResourceProperties,
				OldResourceProperties: ev.OldResourceProperties,
			}:
			default:
			}

			if ev.ResponseURL == "" {
				return nil, 200, nil
			}

			// Respond immediately via goroutine.
			go func(url string) {
				resp := map[string]any{
					"Status":             "SUCCESS",
					"PhysicalResourceId": ev.PhysicalResourceID,
				}
				body, _ := json.Marshal(resp)
				client := &http.Client{Timeout: 2 * time.Second}
				req, reqErr := http.NewRequestWithContext(
					ctx,
					http.MethodPut,
					url,
					newBytesReader(body),
				)
				if reqErr != nil {
					return
				}
				req.ContentLength = int64(len(body))
				res, doErr := client.Do(req)
				if doErr == nil {
					res.Body.Close()
				}
			}(ev.ResponseURL)

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

			if err := json.Unmarshal(payload, &ev); err != nil {
				return nil, 0, err
			}

			select {
			case requestTypes <- ev.RequestType:
			default:
			}

			if ev.ResponseURL == "" {
				return nil, 200, nil
			}

			go func(url, physID string) {
				resp := map[string]any{
					"Status":             "SUCCESS",
					"PhysicalResourceId": physID,
				}
				body, _ := json.Marshal(resp)
				client := &http.Client{Timeout: 2 * time.Second}
				req, reqErr := http.NewRequestWithContext(
					ctx,
					http.MethodPut,
					url,
					newBytesReader(body),
				)
				if reqErr != nil {
					return
				}
				req.ContentLength = int64(len(body))
				res, doErr := client.Do(req)
				if doErr == nil {
					res.Body.Close()
				}
			}(ev.ResponseURL, ev.PhysicalResourceID)

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
	_, err := b.CreateStack(
		t.Context(),
		"update-test",
		templateV1,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)

	// Drain the Create event.
	select {
	case rt := <-requestTypes:
		assert.Equal(t, "Create", rt)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Create event")
	}

	// Update with changed properties.
	_, err = b.UpdateStack(
		t.Context(),
		"update-test",
		templateV2,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)

	// Should receive an Update event.
	select {
	case rt := <-requestTypes:
		assert.Equal(t, "Update", rt, "UpdateStack should send RequestType=Update for Custom::")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Update event from UpdateStack")
	}
}
