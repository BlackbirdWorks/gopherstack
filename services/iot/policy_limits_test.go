package iot_test

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func docOfSize(chars int) string {
	const frame = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"%"}]}`

	pad := chars - (len(frame) - 1)

	return "  " + strings.Replace(frame, "%", strings.Repeat("a", pad), 1) + "\n  "
}

func TestPolicyDocumentSizeLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		chars int
		bad   bool
	}{
		{name: "at limit", chars: 2048},
		{name: "over limit", chars: 2049, bad: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newIoTHandlerBatch1(t)
			rec := iotRequest(
				t, h, http.MethodPost, "/policies/p", map[string]any{"policyDocument": docOfSize(tt.chars)},
			)

			if !tt.bad {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			code, _ := iotErr(t, rec.Body.Bytes())
			assert.Equal(t, "MalformedPolicyException", code)
		})
	}
}

func TestAttachPolicyPerPrincipalLimit(t *testing.T) {
	t.Parallel()

	const cert = "arn:aws:iot:us-east-1:000000000000:cert/abc"

	tests := []struct {
		name    string
		target  string
		wantErr bool
		extra   int
	}{
		{name: "tenth policy", target: cert, extra: 9},
		{name: "eleventh policy", target: cert, extra: 10, wantErr: true},
		{name: "thing group is not limited", target: "arn:aws:iot:us-east-1:000000000000:thinggroup/g", extra: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newIoTTestClient(t)

			for i := range tt.extra + 1 {
				name := "p" + strconv.Itoa(i)
				_, err := client.CreatePolicy(t.Context(), &iotsdk.CreatePolicyInput{
					PolicyName: aws.String(name), PolicyDocument: aws.String(validPolicyDoc),
				})
				require.NoError(t, err)
			}

			for i := range tt.extra {
				_, err := client.AttachPolicy(t.Context(), &iotsdk.AttachPolicyInput{
					PolicyName: aws.String("p" + strconv.Itoa(i)), Target: aws.String(tt.target),
				})
				require.NoError(t, err)
			}

			_, err := client.AttachPolicy(t.Context(), &iotsdk.AttachPolicyInput{
				PolicyName: aws.String("p" + strconv.Itoa(tt.extra)), Target: aws.String(tt.target),
			})

			if tt.wantErr {
				var le *iottypes.LimitExceededException
				require.ErrorAs(t, err, &le)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestAttachPolicyLimitConcurrent(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()

	for i := range 30 {
		_, err := b.CreatePolicy(&iot.CreatePolicyInput{
			PolicyName: "p" + strconv.Itoa(i), PolicyDocument: validPolicyDoc,
		})
		require.NoError(t, err)
	}

	var wg sync.WaitGroup

	for i := range 30 {
		wg.Go(func() {
			_ = b.AttachPolicy(&iot.AttachPolicyInput{
				PolicyName: "p" + strconv.Itoa(i), Target: "arn:aws:iot:us-east-1:0:cert/x",
			})
		})
	}

	wg.Wait()

	n := 0

	for i := range 30 {
		n += len(b.ListPolicyPrincipals("p" + strconv.Itoa(i)))
	}

	assert.Equal(t, 10, n)
}
