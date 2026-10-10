package cloudwatchlogs_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

const (
	emailID  = "arn:aws:dataprotection::aws:data-identifier/EmailAddress"
	cardID   = "arn:aws:dataprotection::aws:data-identifier/CreditCardNumber"
	auditOp  = `"Operation":{"Audit":{"FindingsDestination":{}}}`
	redactOp = `"Operation":{"Deidentify":{"MaskConfig":{}}}`

	maskPolicyDoc = `{"Name":"p","Version":"2021-06-01",` +
		`"Configuration":{"CustomDataIdentifier":[{"Name":"OrderId","Regex":"ORD-[0-9]{4}"}]},` +
		`"Statement":[{"Sid":"audit","DataIdentifier":["` + emailID + `"],` + auditOp + `},` +
		`{"Sid":"redact","DataIdentifier":["` + emailID + `","` + cardID + `","OrderId"],` + redactOp + `}]}`
)

// PutDataProtectionPolicy masks matching data on read unless Unmask is set (api_op_PutDataProtectionPolicy.go).
func TestDataProtection_MasksOnRead(t *testing.T) {
	t.Parallel()

	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
	ctx := t.Context()

	_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("/dp")})
	require.NoError(t, err)
	_, err = client.CreateLogStream(ctx, &cwlsdk.CreateLogStreamInput{
		LogGroupName: aws.String("/dp"), LogStreamName: aws.String("s"),
	})
	require.NoError(t, err)

	put := func(msg string) int64 {
		now := time.Now().UnixMilli()
		_, putErr := client.PutLogEvents(ctx, &cwlsdk.PutLogEventsInput{
			LogGroupName: aws.String("/dp"), LogStreamName: aws.String("s"),
			LogEvents: []cwltypes.InputLogEvent{{Message: aws.String(msg), Timestamp: aws.Int64(now)}},
		})
		require.NoError(t, putErr)

		return now
	}

	const before = "before alice@example.com"

	ingested := put(before)

	require.Eventually(t, func() bool { return time.Now().UnixMilli() > ingested }, 5*time.Second, time.Millisecond)

	_, err = client.PutDataProtectionPolicy(ctx, &cwlsdk.PutDataProtectionPolicyInput{
		LogGroupIdentifier: aws.String("/dp"), PolicyDocument: aws.String(maskPolicyDoc),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return time.Now().UnixMilli() > ingested+1 }, 5*time.Second, time.Millisecond)

	put("mail bob@example.com card 4111 1111 1111 1111 order ORD-1234 ip 10.0.0.1 num 4111 1111 1111 1112")

	read := func(unmask bool) []string {
		out, getErr := client.GetLogEvents(ctx, &cwlsdk.GetLogEventsInput{
			LogGroupName: aws.String("/dp"), LogStreamName: aws.String("s"),
			StartFromHead: aws.Bool(true), Unmask: unmask,
		})
		require.NoError(t, getErr)

		msgs := make([]string, 0, len(out.Events))
		for _, e := range out.Events {
			msgs = append(msgs, aws.ToString(e.Message))
		}

		return msgs
	}

	masked := read(false)
	require.Len(t, masked, 2)
	assert.Equal(t, before, masked[0], "events ingested before the policy stay unmasked")
	assert.Equal(t,
		"mail *************** card ******************* order ******** ip 10.0.0.1 num 4111 1111 1111 1112",
		masked[1])

	unmasked := read(true)
	assert.Contains(t, unmasked[1], "bob@example.com")
	assert.Contains(t, unmasked[1], "ORD-1234")

	filtered, err := client.FilterLogEvents(ctx, &cwlsdk.FilterLogEventsInput{LogGroupName: aws.String("/dp")})
	require.NoError(t, err)
	require.Len(t, filtered.Events, 2)
	assert.NotContains(t, aws.ToString(filtered.Events[1].Message), "bob@example.com")

	filteredUnmasked, err := client.FilterLogEvents(ctx, &cwlsdk.FilterLogEventsInput{
		LogGroupName: aws.String("/dp"), Unmask: true,
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(filteredUnmasked.Events[1].Message), "bob@example.com")

	_, err = client.DeleteDataProtectionPolicy(ctx, &cwlsdk.DeleteDataProtectionPolicyInput{
		LogGroupIdentifier: aws.String("/dp"),
	})
	require.NoError(t, err)
	assert.Contains(t, read(false)[1], "bob@example.com", "deleting the policy stops masking")
}

func TestDataProtection_PolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		doc     string
		wantErr bool
	}{
		{name: "valid", doc: maskPolicyDoc},
		{name: "not-json", doc: "{nope", wantErr: true},
		{
			name:    "bad-regex",
			doc:     `{"Configuration":{"CustomDataIdentifier":[{"Name":"x","Regex":"("}]}}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
			_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("/v")})
			require.NoError(t, err)

			_, err = client.PutDataProtectionPolicy(t.Context(), &cwlsdk.PutDataProtectionPolicyInput{
				LogGroupIdentifier: aws.String("/v"), PolicyDocument: aws.String(tt.doc),
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParameterException")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDataProtection_ManagedIdentifiers(t *testing.T) {
	t.Parallel()

	const (
		secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		pem    = "-----BEGIN OPENSSH PRIVATE KEY-----\nabc\ndef\n-----END OPENSSH PRIVATE KEY-----"
	)

	tests := []struct {
		name string
		id   string
		in   string
		want string
	}{
		{name: "ssn", id: "Ssn-US", in: "ssn 123-45-6789 ok", want: "ssn *********** ok"},
		{name: "ssn-invalid", id: "Ssn-US", in: "no 000-12-3456", want: "no 000-12-3456"},
		{
			name: "secret-key", id: "AwsSecretKey", in: "aws_secret_access_key=" + secret,
			want: "aws_secret_access_key=" + strings.Repeat("*", len(secret)),
		},
		{
			name: "openssh", id: "OpenSSHPrivateKey", in: "k " + pem + " z",
			want: "k " + strings.Repeat("*", len(pem)) + " z",
		},
		{name: "cvv", id: "CreditCardSecurityCode", in: "cvv: 123 done", want: "cvv: *** done"},
		{name: "expiry", id: "CreditCardExpiration", in: "exp 04/2028 x", want: "exp ******* x"},
		{name: "no-match", id: "Ssn-US", in: "nothing here", want: "nothing here"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
			ctx := t.Context()

			_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("/m")})
			require.NoError(t, err)
			_, err = client.CreateLogStream(ctx, &cwlsdk.CreateLogStreamInput{
				LogGroupName: aws.String("/m"), LogStreamName: aws.String("s"),
			})
			require.NoError(t, err)

			doc := `{"Name":"p","Version":"2021-06-01","Statement":[{"Sid":"r","DataIdentifier":` +
				`["arn:aws:dataprotection::aws:data-identifier/` + tt.id + `"],` + redactOp + `}]}`
			_, err = client.PutDataProtectionPolicy(ctx, &cwlsdk.PutDataProtectionPolicyInput{
				LogGroupIdentifier: aws.String("/m"), PolicyDocument: aws.String(doc),
			})
			require.NoError(t, err)

			_, err = client.PutLogEvents(ctx, &cwlsdk.PutLogEventsInput{
				LogGroupName: aws.String("/m"), LogStreamName: aws.String("s"),
				LogEvents: []cwltypes.InputLogEvent{{
					Message: aws.String(tt.in), Timestamp: aws.Int64(time.Now().UnixMilli()),
				}},
			})
			require.NoError(t, err)

			out, err := client.GetLogEvents(ctx, &cwlsdk.GetLogEventsInput{
				LogGroupName: aws.String("/m"), LogStreamName: aws.String("s"), StartFromHead: aws.Bool(true),
			})
			require.NoError(t, err)
			require.Len(t, out.Events, 1)
			assert.Equal(t, tt.want, aws.ToString(out.Events[0].Message))
		})
	}
}
