package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/acm"
	"github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIoTDomainConfigurationServerCertificateStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantStatus iottypes.ServerCertificateStatus
		name       string
		issued     bool
	}{
		{name: "issued_is_valid", issued: true, wantStatus: iottypes.ServerCertificateStatusValid},
		{name: "unknown_is_invalid", wantStatus: iottypes.ServerCertificateStatusInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIoTRuleFixture(t, false, "*")
			certARN := "arn:aws:acm:us-east-1:000000000000:certificate/00000000-0000-0000-0000-000000000000"

			if tt.issued {
				out, err := acm.NewFromConfig(f.fx.cfg).RequestCertificate(t.Context(), &acm.RequestCertificateInput{
					DomainName: aws.String("iot.example.com"),
				})
				require.NoError(t, err)

				certARN = aws.ToString(out.CertificateArn)
			}

			c := iot.NewFromConfig(f.fx.cfg)
			_, err := c.CreateDomainConfiguration(t.Context(), &iot.CreateDomainConfigurationInput{
				DomainConfigurationName: aws.String("dc"),
				DomainName:              aws.String("iot.example.com"),
				ServerCertificateArns:   []string{certARN},
			})
			require.NoError(t, err)

			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				out, derr := c.DescribeDomainConfiguration(t.Context(), &iot.DescribeDomainConfigurationInput{
					DomainConfigurationName: aws.String("dc"),
				})
				if !assert.NoError(ct, derr) || !assert.Len(ct, out.ServerCertificates, 1) {
					return
				}

				got := out.ServerCertificates[0]
				assert.Equal(ct, tt.wantStatus, got.ServerCertificateStatus)
				assert.Equal(ct, certARN, aws.ToString(got.ServerCertificateArn))
				assert.Equal(ct, tt.wantStatus == iottypes.ServerCertificateStatusInvalid,
					aws.ToString(got.ServerCertificateStatusDetail) != "")
			}, authzDeadline, authzTick)
		})
	}
}
