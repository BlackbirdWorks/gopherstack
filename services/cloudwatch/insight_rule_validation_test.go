package cloudwatch_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validInsightRuleDefinition is a well-formed Contributor Insights rule
// document satisfying every structural rule validateInsightRuleDefinition
// enforces (Schema.Name/Version, LogFormat, LogGroupNames, Contribution.Keys).
const validInsightRuleDefinition = `{"Schema":{"Name":"CloudWatchLogRule","Version":1},` +
	`"LogFormat":"JSON","LogGroupNames":["/aws/lambda/my-fn"],"Contribution":{"Keys":["$.ip"]}}`

// validInsightRuleDefinitionV2Sum is a well-formed CloudWatchLogRule2 document
// using the AggregateOn=Sum/Contribution.ValueOf pairing.
const validInsightRuleDefinitionV2Sum = `{"Schema":{"Name":"CloudWatchLogRule2","Version":1},` +
	`"LogFormat":"JSON","LogGroupNames":["/aws/lambda/my-fn"],"AggregateOn":"Sum",` +
	`"Contribution":{"Keys":["$.ip"],"ValueOf":"$.bytes"}}`

// TestPutInsightRule_DefinitionValidation locks the RuleDefinition JSON-body
// validation added to close the "accepted verbatim" gap (bd gopherstack-3ro's
// insight-rule sibling): a malformed or missing RuleDefinition must be rejected
// with InvalidParameterValue instead of being silently stored. Extended (bd
// gopherstack-lrmf) to cover the deep Contributor Insights Rule Syntax checks:
// Schema.Name/Version, LogFormat, LogGroupNames, Contribution.Keys, and
// CloudWatchLogRule2's AggregateOn/ValueOf pairing.
func TestPutInsightRule_DefinitionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		formSet    bool
		wantCode   int
	}{
		{
			name:       "valid document",
			definition: validInsightRuleDefinition,
			formSet:    true,
			wantCode:   http.StatusOK,
		},
		{
			name:       "valid v2 aggregate sum document",
			definition: validInsightRuleDefinitionV2Sum,
			formSet:    true,
			wantCode:   http.StatusOK,
		},
		{
			name:       "empty object missing every required field",
			definition: "{}",
			formSet:    true,
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "missing Schema",
			definition: `{"LogFormat":"JSON","LogGroupNames":["g"],"Contribution":{"Keys":["k"]}}`,
			formSet:    true,
			wantCode:   http.StatusBadRequest,
		},
		{
			name: "invalid Schema.Name",
			definition: `{"Schema":{"Name":"NotARealSchema","Version":1},"LogFormat":"JSON",` +
				`"LogGroupNames":["g"],"Contribution":{"Keys":["k"]}}`,
			formSet:  true,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "wrong Schema.Version",
			definition: `{"Schema":{"Name":"CloudWatchLogRule","Version":2},"LogFormat":"JSON",` +
				`"LogGroupNames":["g"],"Contribution":{"Keys":["k"]}}`,
			formSet:  true,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid LogFormat",
			definition: `{"Schema":{"Name":"CloudWatchLogRule","Version":1},"LogFormat":"XML",` +
				`"LogGroupNames":["g"],"Contribution":{"Keys":["k"]}}`,
			formSet:  true,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing LogGroupNames",
			definition: `{"Schema":{"Name":"CloudWatchLogRule","Version":1},"LogFormat":"JSON",` +
				`"Contribution":{"Keys":["k"]}}`,
			formSet:  true,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty Contribution.Keys",
			definition: `{"Schema":{"Name":"CloudWatchLogRule","Version":1},"LogFormat":"JSON",` +
				`"LogGroupNames":["g"],"Contribution":{"Keys":[]}}`,
			formSet:  true,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "too many Contribution.Keys",
			definition: `{"Schema":{"Name":"CloudWatchLogRule","Version":1},"LogFormat":"JSON",` +
				`"LogGroupNames":["g"],"Contribution":{"Keys":["a","b","c","d","e"]}}`,
			formSet:  true,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "AggregateOn Sum without ValueOf",
			definition: `{"Schema":{"Name":"CloudWatchLogRule2","Version":1},"LogFormat":"JSON",` +
				`"LogGroupNames":["g"],"AggregateOn":"Sum","Contribution":{"Keys":["k"]}}`,
			formSet:  true,
			wantCode: http.StatusBadRequest,
		},
		{
			// AggregateOn is accepted alongside the base CloudWatchLogRule schema
			// too -- not cross-checked against Schema.Name (see
			// validateAggregateOn's doc comment for why).
			name: "AggregateOn Count on base schema is accepted",
			definition: `{"Schema":{"Name":"CloudWatchLogRule","Version":1},"LogFormat":"JSON",` +
				`"LogGroupNames":["g"],"AggregateOn":"Count","Contribution":{"Keys":["k"]}}`,
			formSet:  true,
			wantCode: http.StatusOK,
		},
		{
			name: "invalid AggregateOn value",
			definition: `{"Schema":{"Name":"CloudWatchLogRule","Version":1},"LogFormat":"JSON",` +
				`"LogGroupNames":["g"],"AggregateOn":"Average","Contribution":{"Keys":["k"]}}`,
			formSet:  true,
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "not JSON at all",
			definition: "test",
			formSet:    true,
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "malformed JSON",
			definition: `{"Schema":`,
			formSet:    true,
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "JSON array instead of object",
			definition: `["a","b"]`,
			formSet:    true,
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "JSON scalar instead of object",
			definition: `"just a string"`,
			formSet:    true,
			wantCode:   http.StatusBadRequest,
		},
		{
			name:     "missing entirely",
			formSet:  false,
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "empty string",
			definition: "",
			formSet:    true,
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()

			body := "Action=PutInsightRule&RuleName=" + url.QueryEscape(t.Name()) + "&RuleState=ENABLED"
			if tc.formSet {
				body += "&RuleDefinition=" + url.QueryEscape(tc.definition)
			}

			rec := postForm(t, h, body)
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// TestPutInsightRule_DefinitionValidation_OnUpdate locks that the same
// validation applies when re-PUTting an existing rule name. Real CloudWatch has
// no separate UpdateInsightRule operation -- PutInsightRule is create-or-update
// (same op, no pre-existence requirement), so a second PutInsightRule call
// against an existing rule name must be validated exactly like the first.
func TestPutInsightRule_DefinitionValidation_OnUpdate(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postForm(
		t,
		h,
		"Action=PutInsightRule&RuleName=upd-rule&RuleState=ENABLED&RuleDefinition="+
			url.QueryEscape(validInsightRuleDefinition),
	)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = postForm(
		t,
		h,
		"Action=PutInsightRule&RuleName=upd-rule&RuleState=ENABLED&RuleDefinition=not-json",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

// TestPutManagedInsightRules_BypassesDefinitionValidation locks that the managed
// insight rule path is unaffected by the RuleDefinition JSON validation: managed
// rules store a plain TemplateName string (not JSON) in the Definition field, so
// PutManagedInsightRules must not route through the new validation.
func TestPutManagedInsightRules_BypassesDefinitionValidation(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postForm(t, h, "Action=PutManagedInsightRules"+
		"&ManagedRules.member.1.RuleName=managed-rule"+
		"&ManagedRules.member.1.TemplateName=MostActiveIPs")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "FailureCode")
}

// TestCBORPutInsightRule_DefinitionValidation locks the same RuleDefinition
// validation on the rpc-v2-cbor wire path, which is an independent encoder from
// the XML/query-protocol path and must be checked separately (per the service's
// PARITY.md note: "Every op has two independent encoders... a fix in one does
// not imply the other is correct").
func TestCBORPutInsightRule_DefinitionValidation(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postCBOR(t, h, "PutInsightRule", cbor.Map{
		"RuleName":       cbor.String("cbor-rule"),
		"RuleState":      cbor.String("ENABLED"),
		"RuleDefinition": cbor.String("not-json"),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = postCBOR(t, h, "PutInsightRule", cbor.Map{
		"RuleName":       cbor.String("cbor-rule"),
		"RuleState":      cbor.String("ENABLED"),
		"RuleDefinition": cbor.String(validInsightRuleDefinition),
	})
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// Real CloudWatch has no UpdateInsightRule op -- PutInsightRule is
	// create-or-update, so re-PUTting the same rule name must be validated too.
	rec = postCBOR(t, h, "PutInsightRule", cbor.Map{
		"RuleName":       cbor.String("cbor-rule"),
		"RuleState":      cbor.String("ENABLED"),
		"RuleDefinition": cbor.String("still-not-json"),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
