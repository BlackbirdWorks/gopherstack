package lambdaweb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/stretchr/testify/require"
)

func TestPutResourcePolicy_PublicPolicies(t *testing.T) {
	t.Parallel()

	const (
		head = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",`
		tail = `"Action":"lambda:InvokeWebFunctionEndpoint",` +
			`"Resource":"arn:aws:lambda:us-east-1:123456789012:web-function/app/endpoint/prod"`
	)

	tests := []struct {
		name   string
		policy string
		code   string
	}{
		{name: "wildcard principal", policy: head + `"Principal":"*",` + tail + `}]}`, code: "ValidationException"},
		{
			name:   "wildcard aws principal",
			policy: head + `"Principal":{"AWS":"*"},` + tail + `}]}`,
			code:   "ValidationException",
		},
		{
			name:   "wildcard in arn",
			policy: head + `"Principal":{"AWS":"arn:aws:iam::*:root"},` + tail + `}]}`,
			code:   "ValidationException",
		},
		{
			name: "source ip is not enough", code: "ValidationException",
			policy: head + `"Principal":"*",` + tail + `,"Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}`,
		},
		{
			name: "federated", code: "ValidationException",
			policy: head + `"Principal":{"Federated":"accounts.google.com"},` + tail + `}]}`,
		},
		{
			name: "not principal allow", code: "ValidationException",
			policy: head + `"NotPrincipal":{"AWS":"111122223333"},` + tail + `}]}`,
		},
		{
			name:   "wildcard account condition value",
			policy: head + `"Principal":"*",` + tail + `,"Condition":{"StringEquals":{"aws:PrincipalAccount":"11*"}}}]}`,
			code:   "ValidationException",
		},
		{
			name:   "negated operator",
			policy: head + `"Principal":"*",` + tail + `,"Condition":{"StringNotEquals":{"aws:PrincipalAccount":"1"}}}]}`,
			code:   "ValidationException",
		},
		{
			name: "account condition",
			policy: head + `"Principal":"*",` + tail +
				`,"Condition":{"StringEquals":{"aws:PrincipalAccount":"111122223333"}}}]}`,
		},
		{
			name: "org condition",
			policy: head + `"Principal":{"AWS":"*"},` + tail +
				`,"Condition":{"StringEquals":{"aws:PrincipalOrgID":"o-a1b2c3d4e5"}}}]}`,
		},
		{
			name: "source arn wildcard in resource part",
			policy: head + `"Principal":{"Service":"cloudfront.amazonaws.com"},` + tail +
				`,"Condition":{"ArnLike":{"aws:SourceArn":"arn:aws:cloudfront::111122223333:distribution/*"}}}]}`,
		},
		{
			name: "wildcard source arn account",
			policy: head + `"Principal":"*",` + tail +
				`,"Condition":{"ArnLike":{"aws:SourceArn":"arn:aws:cloudfront::*:distribution/x"}}}]}`,
			code: "ValidationException",
		},
		{
			name: "aws service flag", policy: head + `"Principal":"*",` + tail +
				`,"Condition":{"Bool":{"aws:PrincipalIsAWSService":"true"}}}]}`,
		},
		{name: "deny wildcard", policy: `{"Statement":[{"Effect":"Deny","Principal":"*",` + tail + `}]}`},
		{
			name:   "specific accounts",
			policy: head + `"Principal":{"AWS":["111122223333","444455556666"]},` + tail + `}]}`,
		},
	}

	env := newTestEnv(t)
	created := env.createFunction(t, "app")

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := env.client.PutResourcePolicy(t.Context(), &lwsdk.PutResourcePolicyInput{
				ResourceArn: created.FunctionArn, Policy: aws.String(tc.policy),
			})
			if tc.code == "" {
				require.NoError(t, err)

				return
			}

			assertAPIErrorCode(t, err, tc.code)
		})
	}
}
