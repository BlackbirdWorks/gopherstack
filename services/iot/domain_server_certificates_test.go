package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeCertChecker struct{}

func (fakeCertChecker) ServerCertificateStatus(arn string) (string, string) {
	if arn == "arn:aws:acm:us-east-1:000000000000:certificate/bad" {
		return "INVALID", "certificate not found"
	}

	return "VALID", ""
}

func TestRealClient_DomainConfigurationServerCertificates(t *testing.T) {
	t.Parallel()

	const good = "arn:aws:acm:us-east-1:000000000000:certificate/good"

	cases := []struct {
		name       string
		wantStatus types.ServerCertificateStatus
		arns       []string
		wantCount  int
		checker    bool
		wantErr    bool
	}{
		{name: "echoed_without_checker", arns: []string{good}, wantCount: 1},
		{
			name:       "valid",
			arns:       []string{good},
			checker:    true,
			wantStatus: types.ServerCertificateStatusValid,
			wantCount:  1,
		},
		{
			name: "invalid", arns: []string{"arn:aws:acm:us-east-1:000000000000:certificate/bad"}, checker: true,
			wantStatus: types.ServerCertificateStatusInvalid, wantCount: 1,
		},
		{name: "two_rejected", arns: []string{good, good + "2"}, wantErr: true},
		{name: "none"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, backend := newIoTTestClientWithBackend(t)
			if tt.checker {
				backend.SetServerCertificateChecker(fakeCertChecker{})
			}

			_, err := client.CreateDomainConfiguration(t.Context(), &iotsdk.CreateDomainConfigurationInput{
				DomainConfigurationName:  aws.String("dc"),
				DomainName:               aws.String("example.com"),
				ServerCertificateArns:    tt.arns,
				ValidationCertificateArn: aws.String(good),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			out, err := client.DescribeDomainConfiguration(t.Context(), &iotsdk.DescribeDomainConfigurationInput{
				DomainConfigurationName: aws.String("dc"),
			})
			require.NoError(t, err)
			require.Len(t, out.ServerCertificates, tt.wantCount)

			if tt.wantCount > 0 {
				assert.Equal(t, tt.arns[0], aws.ToString(out.ServerCertificates[0].ServerCertificateArn))
				assert.Equal(t, tt.wantStatus, out.ServerCertificates[0].ServerCertificateStatus)
			}
		})
	}
}
