package sdkwire

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const idWithPrefix = "/hostedzone/Z123"

func TestRoute53IDSanitizeParity(t *testing.T) {
	t.Parallel()

	svc, ok := Lookup("route53")
	require.True(t, ok)

	cc := &capturingClient{}
	client := newSDKClient(sdkClients()["route53"], cc)

	var failures []string

	checked := 0

	for _, op := range sdkOps(client) {
		in := fill(op.in, 0, map[reflect.Type]int{})

		for i := range in.Elem().NumField() {
			f := in.Elem().Field(i)
			if f.Kind() == reflect.Pointer && f.Type().Elem().Kind() == reflect.String {
				f.Set(reflect.ValueOf(new(idWithPrefix)))
			}
		}

		params, _ := toValue(in)

		cc.req = nil
		_ = callSDK(op, in)
		require.NotNil(t, cc.req, op.name)

		tgt := Target{
			Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
			Endpoint:    testEndpoint, Region: "us-east-1",
		}

		req, err := svc.buildRequest(context.Background(), tgt, op.name, svc.Ops[op.name], sanitize(svc.Ops[op.name], params))
		require.NoError(t, err, op.name)

		if len(svc.Ops[op.name].Sanitize) > 0 {
			checked++
		}

		if d := requestDiff(cc.req, cc.reqBody, req, readBody(req)); len(d) > 0 {
			failures = append(failures, op.name+": "+d[0])
		}
	}

	assert.Empty(t, failures)
	assert.Positive(t, checked)
}

func readBody(r *http.Request) []byte {
	if r.Body == nil {
		return nil
	}

	buf := make([]byte, 0, 512)
	tmp := make([]byte, 512)

	for {
		n, err := r.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)

		if err != nil {
			return buf
		}
	}
}

func TestRoute53ErrorParity(t *testing.T) {
	t.Parallel()

	svc, ok := Lookup("route53")
	require.True(t, ok)

	tests := []struct {
		name   string
		body   string
		status int
	}{
		{
			name: "no_such_hosted_zone", status: http.StatusNotFound,
			body: `<?xml version="1.0"?><ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">` +
				`<Error><Type>Sender</Type><Code>NoSuchHostedZone</Code><Message>No hosted zone found</Message></Error>` +
				`<RequestId>r1</RequestId></ErrorResponse>`,
		},
		{
			name: "invalid_change_batch", status: http.StatusBadRequest,
			body: `<?xml version="1.0"?><InvalidChangeBatch xmlns="https://route53.amazonaws.com/doc/2013-04-01/">` +
				`<Messages><Message>dup</Message></Messages><RequestId>r2</RequestId></InvalidChangeBatch>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cc := &capturingClient{
				respStatus: tt.status, respBody: []byte(tt.body),
				respHeader: http.Header{"Content-Type": {"text/xml"}},
			}
			client := newSDKClient(sdkClients()["route53"], cc)

			var sdkOp sdkOp

			for _, op := range sdkOps(client) {
				if op.name == "GetHostedZone" {
					sdkOp = op
				}
			}

			sdkErr := callSDK(sdkOp, fill(sdkOp.in, 0, map[reflect.Type]int{}))
			require.Error(t, sdkErr)

			var want smithy.APIError

			require.ErrorAs(t, sdkErr, &want)

			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/xml")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			})
			tgt := Target{
				Handler: handler, Endpoint: testEndpoint, Region: "us-east-1",
				Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
			}

			_, err := svc.Call(context.Background(), tgt, "GetHostedZone", map[string]any{"Id": "Z1"})
			require.Error(t, err)

			var got smithy.APIError

			require.True(t, errors.As(err, &got), "%v", err)
			assert.Equal(t, want.ErrorCode(), got.ErrorCode())
			assert.Equal(t, want.ErrorMessage(), got.ErrorMessage())
		})
	}
}
