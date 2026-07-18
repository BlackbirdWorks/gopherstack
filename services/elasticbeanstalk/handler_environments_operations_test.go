package elasticbeanstalk_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_AssociateEnvironmentOperationsRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
		setupEnv   bool
	}{
		{
			name:     "success",
			setupEnv: true,
			body: "Version=2010-12-01&Action=AssociateEnvironmentOperationsRole" +
				"&EnvironmentName=my-env&OperationsRole=arn:aws:iam::123:role/ops",
			wantStatus: http.StatusOK,
		},
		{
			name: "missing environment name",
			body: "Version=2010-12-01&Action=AssociateEnvironmentOperationsRole" +
				"&OperationsRole=arn:aws:iam::123:role/ops",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing operations role",
			body:       "Version=2010-12-01&Action=AssociateEnvironmentOperationsRole&EnvironmentName=my-env",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "environment not found",
			body: "Version=2010-12-01&Action=AssociateEnvironmentOperationsRole" +
				"&EnvironmentName=nonexistent&OperationsRole=arn:aws:iam::123:role/ops",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setupEnv {
				postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app&EnvironmentName=my-env",
				)
			}

			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_AssociateEnvironmentOperationsRole_PersistsRole verifies the role is
// persisted on the environment and readable back from the backend.
func TestHandler_AssociateEnvironmentOperationsRole_PersistsRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=my-env")

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=AssociateEnvironmentOperationsRole"+
			"&EnvironmentName=my-env&OperationsRole=arn:aws:iam::123:role/MyRole")
	require.Equal(t, http.StatusOK, rec.Code)

	// Now retrieve the environment and verify it has the role stored.
	envs := h.Backend.DescribeEnvironments(context.Background(), "app", []string{"my-env"}, nil)
	require.Len(t, envs, 1)
	assert.Equal(t, "arn:aws:iam::123:role/MyRole", envs[0].OperationsRole)
}

func TestHandler_CheckDNSAvailability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cnamePrefix string
		wantStatus  int
		setupEnv    bool
		wantAvail   bool
	}{
		{
			name:        "available prefix",
			cnamePrefix: "free-prefix",
			wantStatus:  http.StatusOK,
			wantAvail:   true,
		},
		{
			name:        "taken prefix matches env name",
			cnamePrefix: "my-env",
			setupEnv:    true,
			wantStatus:  http.StatusOK,
			wantAvail:   false,
		},
		{
			name:       "missing cname prefix",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.setupEnv {
				postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app&EnvironmentName=my-env",
				)
			}

			body := "Version=2010-12-01&Action=CheckDNSAvailability"
			if tt.cnamePrefix != "" {
				body += "&CNAMEPrefix=" + tt.cnamePrefix
			}

			rec := postEBForm(t, h, body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					CheckDNSAvailabilityResult struct {
						FullyQualifiedCNAME string `xml:"FullyQualifiedCNAME"`
						Available           bool   `xml:"Available"`
					} `xml:"CheckDNSAvailabilityResult"`
				}

				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantAvail, out.CheckDNSAvailabilityResult.Available)
				assert.Contains(t, out.CheckDNSAvailabilityResult.FullyQualifiedCNAME, tt.cnamePrefix)
			}
		})
	}
}

// TestHandler_CheckDNSAvailability_AfterEnvironmentCreation verifies the CNAME
// prefix used by an existing environment is reported unavailable, and an unused
// prefix is reported available.
func TestHandler_CheckDNSAvailability_AfterEnvironmentCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cnamePrefix string
		wantAvail   string
		setupEnv    bool
	}{
		{
			name:        "used CNAME reported unavailable",
			cnamePrefix: "myenv",
			setupEnv:    true,
			wantAvail:   "<Available>false</Available>",
		},
		{
			name:        "free CNAME reported available",
			cnamePrefix: "unused-prefix",
			wantAvail:   "<Available>true</Available>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setupEnv {
				postEBForm(t, h,
					"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app"+
						"&EnvironmentName=myenv&CNAMEPrefix=myenv")
			}

			rec := postEBForm(t, h, "Version=2010-12-01&Action=CheckDNSAvailability&CNAMEPrefix="+tt.cnamePrefix)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantAvail)
		})
	}
}

// TestHandler_CheckDNSAvailability_ConflictIncludesFQDN verifies the fully
// qualified CNAME is returned alongside the unavailable result.
func TestHandler_CheckDNSAvailability_ConflictIncludesFQDN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=my-prefix")

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=CheckDNSAvailability&CNAMEPrefix=my-prefix")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Available>false</Available>")
	assert.Contains(t, rec.Body.String(), "my-prefix.us-east-1.elasticbeanstalk.com")
}

// TestHandler_CheckDNSAvailability_AvailableIncludesFQDN verifies the fully
// qualified CNAME is returned alongside the available result.
func TestHandler_CheckDNSAvailability_AvailableIncludesFQDN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=CheckDNSAvailability&CNAMEPrefix=free-prefix")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Available>true</Available>")
	assert.Contains(t, rec.Body.String(), "free-prefix.us-east-1.elasticbeanstalk.com")
}
