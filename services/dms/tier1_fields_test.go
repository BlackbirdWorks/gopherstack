package dms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportCertificate_KmsKeyIDEchoed(t *testing.T) {
	t.Parallel()

	client := newTestDMSClient(t, newTestDMSHandler())
	const key = "arn:aws:kms:us-east-1:000000000000:key/abcd"

	out, err := client.ImportCertificate(t.Context(), &dmssdk.ImportCertificateInput{
		CertificateIdentifier: aws.String("c1"),
		CertificatePem:        aws.String("-----BEGIN CERTIFICATE-----"),
		KmsKeyId:              aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, key, aws.ToString(out.Certificate.KmsKeyId))

	desc, err := client.DescribeCertificates(t.Context(), &dmssdk.DescribeCertificatesInput{})
	require.NoError(t, err)
	require.Len(t, desc.Certificates, 1)
	assert.Equal(t, key, aws.ToString(desc.Certificates[0].KmsKeyId))
}

func TestModifyInstanceProfile_NewFields(t *testing.T) {
	t.Parallel()

	client := newTestDMSClient(t, newTestDMSHandler())

	_, err := client.CreateInstanceProfile(t.Context(), &dmssdk.CreateInstanceProfileInput{
		InstanceProfileName: aws.String("ip"), PubliclyAccessible: aws.Bool(true),
	})
	require.NoError(t, err)

	out, err := client.ModifyInstanceProfile(t.Context(), &dmssdk.ModifyInstanceProfileInput{
		InstanceProfileIdentifier: aws.String("ip"),
		PubliclyAccessible:        aws.Bool(false),
		KmsKeyArn:                 aws.String("arn:aws:kms:us-east-1:000000000000:key/k"),
		SubnetGroupIdentifier:     aws.String("sg-1"),
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(out.InstanceProfile.PubliclyAccessible))
	assert.Equal(t, "sg-1", aws.ToString(out.InstanceProfile.SubnetGroupIdentifier))
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/k", aws.ToString(out.InstanceProfile.KmsKeyArn))
}

func TestDescribeApplicableIndividualAssessments_UnknownBase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input dmssdk.DescribeApplicableIndividualAssessmentsInput
	}{
		{name: "task", input: dmssdk.DescribeApplicableIndividualAssessmentsInput{
			ReplicationTaskArn: aws.String("arn:aws:dms:us-east-1:000000000000:task:nope"),
		}},
		{name: "instance", input: dmssdk.DescribeApplicableIndividualAssessmentsInput{
			ReplicationInstanceArn: aws.String("arn:aws:dms:us-east-1:000000000000:rep:nope"),
		}},
		{name: "config", input: dmssdk.DescribeApplicableIndividualAssessmentsInput{
			ReplicationConfigArn: aws.String("arn:aws:dms:us-east-1:000000000000:replication-config:nope"),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDMSClient(t, newTestDMSHandler())
			_, err := client.DescribeApplicableIndividualAssessments(t.Context(), &tt.input)
			require.Error(t, err)

			var nf *types.ResourceNotFoundFault
			require.ErrorAs(t, err, &nf)
		})
	}

	t.Run("existing-instance", func(t *testing.T) {
		t.Parallel()

		client := newTestDMSClient(t, newTestDMSHandler())
		ri, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
			ReplicationInstanceIdentifier: aws.String("ri"), ReplicationInstanceClass: aws.String("dms.t3.micro"),
		})
		require.NoError(t, err)

		out, err := client.DescribeApplicableIndividualAssessments(t.Context(),
			&dmssdk.DescribeApplicableIndividualAssessmentsInput{
				ReplicationInstanceArn: ri.ReplicationInstance.ReplicationInstanceArn,
			})
		require.NoError(t, err)
		assert.NotEmpty(t, out.IndividualAssessmentNames)
	})
}
