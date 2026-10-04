package cloudtrail

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderManagementEventMatchesJSONMarshal(t *testing.T) {
	t.Parallel()

	base := managementEventDetail{
		UserIdentity: managementEventIdentity{
			Type: "IAMUser", PrincipalID: "AKID", AccessKeyID: "AKID", AccountID: "000000000000",
		},
		EventVersion:       eventVersion,
		EventTime:          "2026-01-01T00:00:00.123456789Z",
		EventSource:        "s3.amazonaws.com",
		EventName:          "CreateBucket",
		AwsRegion:          "us-east-1",
		RequestID:          "11111111-1111-1111-1111-111111111111",
		EventID:            "22222222-2222-2222-2222-222222222222",
		EventType:          "AwsApiCall",
		RecipientAccountID: "000000000000",
		EventCategory:      eventCategoryManagement,
		ManagementEvent:    true,
	}

	tests := []struct {
		mutate func(d *managementEventDetail)
		name   string
	}{
		{name: "plain", mutate: func(*managementEventDetail) {}},
		{name: "anonymous", mutate: func(d *managementEventDetail) {
			d.UserIdentity = managementEventIdentity{Type: "AWSAccount", AccountID: "000000000000"}
		}},
		{name: "no_account", mutate: func(d *managementEventDetail) {
			d.UserIdentity.AccountID = ""
			d.RecipientAccountID = ""
		}},
		{name: "error_plain", mutate: func(d *managementEventDetail) {
			d.ErrorCode, d.ErrorMessage = "ResourceNotFoundException", "Table not found"
		}},
		{name: "error_quotes", mutate: func(d *managementEventDetail) { d.ErrorMessage = `bad "x" \ y` }},
		{name: "error_html", mutate: func(d *managementEventDetail) { d.ErrorMessage = "a<b>&c" }},
		{name: "error_unicode", mutate: func(d *managementEventDetail) { d.ErrorMessage = "café   \n" }},
		{name: "invalid_utf8", mutate: func(d *managementEventDetail) { d.EventName = "x\xffy" }},
		{name: "read_only", mutate: func(d *managementEventDetail) { d.ReadOnly = true }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := base
			tt.mutate(&d)

			want, err := json.Marshal(d)
			require.NoError(t, err)
			require.Equal(t, string(want), renderManagementEvent(&d))
		})
	}
}
