package cloudformation_test

import (
	"encoding/xml"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type batchDescribeTypeConfigurationsResponse struct {
	XMLName xml.Name `xml:"BatchDescribeTypeConfigurationsResponse"`
	Result  struct {
		TypeConfigurations []struct {
			TypeName               string `xml:"TypeName"`
			Configuration          string `xml:"Configuration"`
			IsDefaultConfiguration bool   `xml:"IsDefaultConfiguration"`
		} `xml:"TypeConfigurations>member"`
		Errors []struct {
			ErrorCode                   string `xml:"ErrorCode"`
			ErrorMessage                string `xml:"ErrorMessage"`
			TypeConfigurationIdentifier struct {
				TypeName string `xml:"TypeName"`
			} `xml:"TypeConfigurationIdentifier"`
		} `xml:"Errors>member"`
		UnprocessedTypeConfigurations []struct {
			TypeName string `xml:"TypeName"`
		} `xml:"UnprocessedTypeConfigurations>member"`
	} `xml:"BatchDescribeTypeConfigurationsResult"`
}

// TestHandler_BatchDescribeTypeConfigurations drives real query-encoded
// TypeConfigurationIdentifiers.member.N.TypeName request bodies (verified
// against serializers.go:7114 -- the list is of structs, not scalars) and
// asserts on the real XML response's Errors and UnprocessedTypeConfigurations
// fields (api_op_BatchDescribeTypeConfigurations.go:47,55).
func TestHandler_BatchDescribeTypeConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		identifierField   string
		identifierValue   string
		wantErrorCode     string
		wantConfiguration string
		wantConfigured    bool
		wantUnprocessed   bool
	}{
		{
			name:              "configured type returns its configuration",
			identifierField:   "TypeName",
			identifierValue:   "Acme::Demo::Widget",
			wantConfigured:    true,
			wantConfiguration: `{"key":"value"}`,
		},
		{
			name:            "unknown type name reports an error",
			identifierField: "TypeName",
			identifierValue: "Acme::Demo::DoesNotExist",
			wantErrorCode:   "TypeNotFoundException",
		},
		{
			name:            "identifier with no name or arn is unprocessed",
			identifierField: "TypeConfigurationAlias",
			identifierValue: "some-alias",
			wantUnprocessed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			postForm(t, h, url.Values{
				"Action":               {"RegisterType"},
				"TypeName":             {"Acme::Demo::Widget"},
				"SchemaHandlerPackage": {"s3://bucket/schema.zip"},
			}.Encode())
			postForm(t, h, url.Values{
				"Action":        {"SetTypeConfiguration"},
				"TypeName":      {"Acme::Demo::Widget"},
				"Configuration": {`{"key":"value"}`},
			}.Encode())

			form := url.Values{
				"Action": {"BatchDescribeTypeConfigurations"},
				"TypeConfigurationIdentifiers.member.1." + tt.identifierField: {tt.identifierValue},
			}
			rec := postForm(t, h, form.Encode())
			require.Equal(t, 200, rec.Code, "body: %s", rec.Body.String())

			var resp batchDescribeTypeConfigurationsResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			if tt.wantConfigured {
				require.Len(t, resp.Result.TypeConfigurations, 1)
				assert.Equal(t, tt.identifierValue, resp.Result.TypeConfigurations[0].TypeName)
				assert.Equal(t, tt.wantConfiguration, resp.Result.TypeConfigurations[0].Configuration)
				assert.False(t, resp.Result.TypeConfigurations[0].IsDefaultConfiguration)
				assert.Empty(t, resp.Result.Errors)
				assert.Empty(t, resp.Result.UnprocessedTypeConfigurations)
			}

			if tt.wantErrorCode != "" {
				require.Len(t, resp.Result.Errors, 1)
				assert.Equal(t, tt.wantErrorCode, resp.Result.Errors[0].ErrorCode)
				assert.Equal(t, tt.identifierValue, resp.Result.Errors[0].TypeConfigurationIdentifier.TypeName)
				assert.NotEmpty(t, resp.Result.Errors[0].ErrorMessage)
				assert.Empty(t, resp.Result.TypeConfigurations)
			}

			if tt.wantUnprocessed {
				require.Len(t, resp.Result.UnprocessedTypeConfigurations, 1)
				assert.Empty(t, resp.Result.TypeConfigurations)
				assert.Empty(t, resp.Result.Errors)
			}
		})
	}
}
