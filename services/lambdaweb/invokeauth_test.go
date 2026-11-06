package lambdaweb_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

const (
	otherAccount = "999988887777"
	invokeAct    = "lambda:InvokeWebFunctionEndpoint"
)

type fakeResolver struct{ principal *awsmeta.Principal }

func (f fakeResolver) ResolvePrincipal(context.Context, string, string) (*awsmeta.Principal, bool) {
	return f.principal, f.principal != nil
}

func policyJSON(stmt string) string {
	return `{"Version":"2012-10-17","Statement":[` + stmt + `]}`
}

func TestInvokeAuth_ResourcePolicy(t *testing.T) {
	t.Parallel()

	endpointARN := "arn:aws:lambda:us-east-1:123456789012:web-function/secure/endpoint/prod"
	sameUser := &awsmeta.Principal{Arn: "arn:aws:iam::123456789012:user/alice", AccountID: testAccountID}
	crossUser := &awsmeta.Principal{Arn: "arn:aws:iam::" + otherAccount + ":user/bob", AccountID: otherAccount}

	tests := []struct {
		principal *awsmeta.Principal
		name      string
		policy    string
		want      int
	}{
		{name: "no policy same account", want: http.StatusOK},
		{name: "no policy cross account", principal: crossUser, want: http.StatusForbidden},
		{
			name: "deny everyone",
			policy: policyJSON(
				`{"Effect":"Deny","Principal":"*","Action":"` + invokeAct + `","Resource":"` + endpointARN + `"}`,
			),
			want: http.StatusForbidden,
		},
		{
			name: "deny a named user", principal: sameUser,
			policy: policyJSON(`{"Effect":"Deny","Principal":{"AWS":"` + sameUser.Arn + `"},"Action":"` + invokeAct +
				`","Resource":"` + endpointARN + `"}`),
			want: http.StatusForbidden,
		},
		{
			name: "deny a different user", principal: sameUser,
			policy: policyJSON(
				`{"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::123456789012:user/carol"},"Action":"` +
					invokeAct + `","Resource":"` + endpointARN + `"}`,
			),
			want: http.StatusOK,
		},
		{
			name: "deny another action", principal: sameUser,
			policy: policyJSON(`{"Effect":"Deny","Principal":"*","Action":"lambda:GetWebFunction","Resource":"*"}`),
			want:   http.StatusOK,
		},
		{
			name: "deny other endpoint", principal: sameUser,
			policy: policyJSON(`{"Effect":"Deny","Principal":"*","Action":"` + invokeAct +
				`","Resource":"arn:aws:lambda:us-east-1:123456789012:web-function/secure/endpoint/other"}`),
			want: http.StatusOK,
		},
		{
			name: "allow cross account principal", principal: crossUser,
			policy: policyJSON(`{"Effect":"Allow","Principal":{"AWS":"` + otherAccount + `"},"Action":"` + invokeAct +
				`","Resource":"` + endpointARN + `"}`),
			want: http.StatusOK,
		},
		{
			name: "allow other account only", principal: crossUser,
			policy: policyJSON(`{"Effect":"Allow","Principal":{"AWS":"111122223333"},"Action":"` + invokeAct +
				`","Resource":"` + endpointARN + `"}`),
			want: http.StatusForbidden,
		},
		{
			name: "allow wildcard action cross account", principal: crossUser,
			policy: policyJSON(`{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"lambda:*","Resource":"*"}`),
			want:   http.StatusOK,
		},
		{
			name: "deny by source ip condition",
			policy: policyJSON(`{"Effect":"Deny","Principal":"*","Action":"` + invokeAct + `","Resource":"*",` +
				`"Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.0/8"}}}`),
			want: http.StatusForbidden,
		},
		{
			name: "deny condition not met",
			policy: policyJSON(`{"Effect":"Deny","Principal":"*","Action":"` + invokeAct + `","Resource":"*",` +
				`"Condition":{"IpAddress":{"aws:SourceIp":"203.0.113.0/24"}}}`),
			want: http.StatusOK,
		},
		{
			name: "deny by principal account condition", principal: crossUser,
			policy: policyJSON(`{"Effect":"Deny","Principal":"*","Action":"` + invokeAct + `","Resource":"*",` +
				`"Condition":{"StringEquals":{"aws:PrincipalAccount":"` + otherAccount + `"}}}`),
			want: http.StatusForbidden,
		},
		{
			name: "not principal denies others", principal: crossUser,
			policy: policyJSON(`{"Effect":"Allow","NotPrincipal":{"AWS":"111122223333"},"Action":"` + invokeAct +
				`","Resource":"*"}`),
			want: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, _ := newIamEnv(t)
			d.backend.SetPrincipalResolver(fakeResolver{principal: tc.principal})

			if tc.policy != "" {
				_, err := d.client.PutResourcePolicy(t.Context(), &lwsdk.PutResourcePolicyInput{
					ResourceArn: aws.String("arn:aws:lambda:us-east-1:123456789012:web-function/secure"),
					Policy:      aws.String(tc.policy),
				})
				require.NoError(t, err)
			}

			resp := d.do(t, http.MethodGet, d.domain, "/", signedRequest(t, "lambda"))
			assert.Equal(t, tc.want, resp.StatusCode)
		})
	}
}

func TestInvokeAuth_PolicyAppliesImmediatelyAfterDelete(t *testing.T) {
	t.Parallel()

	d, _ := newIamEnv(t)
	fn := aws.String("arn:aws:lambda:us-east-1:123456789012:web-function/secure")

	_, err := d.client.PutResourcePolicy(t.Context(), &lwsdk.PutResourcePolicyInput{
		ResourceArn: fn,
		Policy: aws.String(policyJSON(`{"Effect":"Deny","Principal":"*","Action":"` + invokeAct +
			`","Resource":"*"}`)),
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusForbidden, d.do(t, http.MethodGet, d.domain, "/", signedRequest(t, "lambda")).StatusCode)

	_, err = d.client.DeleteResourcePolicy(t.Context(), &lwsdk.DeleteResourcePolicyInput{ResourceArn: fn})
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, d.do(t, http.MethodGet, d.domain, "/", signedRequest(t, "lambda")).StatusCode)
}

func TestInvokeAuth_ApplicationManagedIgnoresPolicy(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)

	_, err := d.client.PutResourcePolicy(t.Context(), &lwsdk.PutResourcePolicyInput{
		ResourceArn: aws.String("arn:aws:lambda:us-east-1:123456789012:web-function/app"),
		Policy: aws.String(policyJSON(`{"Effect":"Deny","Principal":"*","Action":"` + invokeAct +
			`","Resource":"*"}`)),
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, d.get(t).StatusCode)
}
