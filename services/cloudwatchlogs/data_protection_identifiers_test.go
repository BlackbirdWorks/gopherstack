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

func stars(s string) string { return strings.Repeat("*", len(s)) }

// Identifier names follow the CloudWatch Logs managed data identifiers table (Identifier-CC form).
func TestDataProtection_FormatIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
		in   string
		want string
	}{
		{
			name: "cpf valid",
			id:   "CpfCode-BR",
			in:   "cpf 529.982.247-25 ok",
			want: "cpf " + stars("529.982.247-25") + " ok",
		},
		{name: "cpf plain digits", id: "CpfCode-BR", in: "52998224725", want: stars("52998224725")},
		{name: "cpf bad check digit", id: "CpfCode-BR", in: "cpf 529.982.247-26", want: "cpf 529.982.247-26"},
		{name: "cpf repeated digits", id: "CpfCode-BR", in: "111.111.111-11", want: "111.111.111-11"},
		{name: "cnpj valid", id: "Cnpj-BR", in: "11.222.333/0001-81", want: stars("11.222.333/0001-81")},
		{name: "cnpj bad check digit", id: "Cnpj-BR", in: "11.222.333/0001-82", want: "11.222.333/0001-82"},
		{name: "cep", id: "CepCode-BR", in: "at 01310-100.", want: "at " + stars("01310-100") + "."},
		{name: "nino valid", id: "NationalInsuranceNumber-GB", in: "ni AB123456C", want: "ni " + stars("AB123456C")},
		{name: "nino spaced", id: "NationalInsuranceNumber-GB", in: "AB 12 34 56 C", want: stars("AB 12 34 56 C")},
		{name: "nino banned prefix", id: "NationalInsuranceNumber-GB", in: "BG123456C", want: "BG123456C"},
		{name: "ca postal", id: "PostalCode-CA", in: "to K1A 0B1 now", want: "to " + stars("K1A 0B1") + " now"},
		{name: "ca postal bad letter", id: "PostalCode-CA", in: "D1A 0B1", want: "D1A 0B1"},
		{
			name: "dea valid",
			id:   "DrugEnforcementAgencyNumber-US",
			in:   "dea AB1234563",
			want: "dea " + stars("AB1234563"),
		},
		{name: "dea bad checksum", id: "DrugEnforcementAgencyNumber-US", in: "AB1234564", want: "AB1234564"},
		{name: "itin", id: "IndividualTaxIdentificationNumber-US", in: "912-70-1234", want: stars("912-70-1234")},
		{name: "itin bad group", id: "IndividualTaxIdentificationNumber-US", in: "912-93-1234", want: "912-93-1234"},
		{name: "nif", id: "NifNumber-ES", in: "nif 12345678Z", want: "nif " + stars("12345678Z")},
		{name: "nif bad letter", id: "NifNumber-ES", in: "12345678A", want: "12345678A"},
		{name: "nie", id: "NieNumber-ES", in: "nie X1234567L", want: "nie " + stars("X1234567L")},
		{
			name: "documented openssh casing",
			id:   "OpenSshPrivateKey",
			in:   "-----BEGIN OPENSSH PRIVATE KEY-----\nx\n-----END OPENSSH PRIVATE KEY-----",
			want: stars("-----BEGIN OPENSSH PRIVATE KEY-----\nx\n-----END OPENSSH PRIVATE KEY-----"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
			ctx := t.Context()

			_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("/f")})
			require.NoError(t, err)
			_, err = client.CreateLogStream(ctx, &cwlsdk.CreateLogStreamInput{
				LogGroupName: aws.String("/f"), LogStreamName: aws.String("s"),
			})
			require.NoError(t, err)

			doc := `{"Name":"p","Version":"2021-06-01","Statement":[{"Sid":"r","DataIdentifier":` +
				`["arn:aws:dataprotection::aws:data-identifier/` + tt.id + `"],` + redactOp + `}]}`
			_, err = client.PutDataProtectionPolicy(ctx, &cwlsdk.PutDataProtectionPolicyInput{
				LogGroupIdentifier: aws.String("/f"), PolicyDocument: aws.String(doc),
			})
			require.NoError(t, err)

			_, err = client.PutLogEvents(ctx, &cwlsdk.PutLogEventsInput{
				LogGroupName: aws.String("/f"), LogStreamName: aws.String("s"),
				LogEvents: []cwltypes.InputLogEvent{{
					Message: aws.String(tt.in), Timestamp: aws.Int64(time.Now().UnixMilli()),
				}},
			})
			require.NoError(t, err)

			out, err := client.GetLogEvents(ctx, &cwlsdk.GetLogEventsInput{
				LogGroupName: aws.String("/f"), LogStreamName: aws.String("s"), StartFromHead: aws.Bool(true),
			})
			require.NoError(t, err)
			require.Len(t, out.Events, 1)
			assert.Equal(t, tt.want, aws.ToString(out.Events[0].Message))
		})
	}
}
