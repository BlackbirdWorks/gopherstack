package cloudformation_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
)

var errSimulatedCreate = errors.New("simulated queue creation failure")

func newBackend() *cloudformation.InMemoryBackend {
	return cloudformation.NewInMemoryBackendWithConfig(
		"000000000000",
		"us-east-1",
		cloudformation.NewResourceCreator(nil),
	)
}

func newHandler() *cloudformation.Handler {
	return cloudformation.NewHandler(newBackend())
}

func postForm(t *testing.T, h *cloudformation.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, req.ParseForm())
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

const simpleTemplate = `{"AWSTemplateFormatVersion":"2010-09-09",` +
	`"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}}}}`

// modifiedTemplate is simpleTemplate plus an additional resource; a change set
// created from it against a simpleTemplate stack yields a real Add change.
const modifiedTemplate = `{"AWSTemplateFormatVersion":"2010-09-09",` +
	`"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}},` +
	`"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{}}}}`

const templateWithParams = `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "BucketName": {
      "Type": "String",
      "Default": "default-bucket"
    }
  },
  "Resources": {
    "MyBucket": {
      "Type": "AWS::S3::Bucket",
      "Properties": {
        "BucketName": {"Ref": "BucketName"}
      }
    }
  },
  "Outputs": {
    "BucketOut": {
      "Value": {"Ref": "BucketName"},
      "Description": "The bucket name"
    }
  }
}`

const yamlTemplate = `
AWSTemplateFormatVersion: "2010-09-09"
Description: "YAML template"
Resources:
  MyQueue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: my-yaml-queue
`

// extractField is a helper to extract a simple XML element value from a response body.
func extractField(body, tag string) string {
	open := "<" + tag + ">"
	closeTag := "</" + tag + ">"
	start := strings.Index(body, open)
	if start == -1 {
		return ""
	}
	start += len(open)
	end := strings.Index(body[start:], closeTag)
	if end == -1 {
		return ""
	}

	return body[start : start+end]
}
