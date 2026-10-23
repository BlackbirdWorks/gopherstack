package app

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimestreamInfluxDBAuthSecretWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		user, org    string
		wantUsername string
	}{
		{name: "explicit credentials", user: "alice", org: "acme", wantUsername: "alice"},
		{name: "default credentials", wantUsername: "admin"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			ts := timestreaminfluxdb.NewFromConfig(fx.cfg)

			subnets, err := ec2.NewFromConfig(fx.cfg).DescribeSubnets(t.Context(), &ec2.DescribeSubnetsInput{})
			require.NoError(t, err)
			require.NotEmpty(t, subnets.Subnets)

			in := &timestreaminfluxdb.CreateDbInstanceInput{
				Name: aws.String("wired-db"), Password: aws.String("Passw0rdPassw0rd"),
				DbInstanceType: types.DbInstanceTypeDbInfluxMedium, AllocatedStorage: aws.Int32(100),
				VpcSubnetIds:        []string{aws.ToString(subnets.Subnets[0].SubnetId)},
				VpcSecurityGroupIds: []string{"sg-bbb222"},
			}
			if tt.user != "" {
				in.Username, in.Organization = aws.String(tt.user), aws.String(tt.org)
			}

			out, err := ts.CreateDbInstance(t.Context(), in)
			require.NoError(t, err)
			require.NotNil(t, out.InfluxAuthParametersSecretArn)

			val, err := secretsmanager.NewFromConfig(fx.cfg).GetSecretValue(t.Context(),
				&secretsmanager.GetSecretValueInput{SecretId: out.InfluxAuthParametersSecretArn})
			require.NoError(t, err)

			var secret map[string]string
			require.NoError(t, json.Unmarshal([]byte(aws.ToString(val.SecretString)), &secret))
			assert.Equal(t, tt.wantUsername, secret["username"])
			assert.Equal(t, "Passw0rdPassw0rd", secret["password"])
			assert.Len(t, secret["token"], 64)

			_, err = ts.DeleteDbInstance(t.Context(), &timestreaminfluxdb.DeleteDbInstanceInput{Identifier: out.Id})
			assert.Error(t, err, "a CREATING instance cannot be deleted yet")
		})
	}
}
