package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestInputValidation_Errors(t *testing.T) {
	t.Parallel()

	inst := func(mutate func(*rdssdk.CreateDBInstanceInput)) *rdssdk.CreateDBInstanceInput {
		in := &rdssdk.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String("iv-inst"), Engine: aws.String("mysql"),
			DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"),
			MasterUserPassword: aws.String("password123"), AllocatedStorage: aws.Int32(20),
		}
		mutate(in)

		return in
	}

	tests := []struct {
		call     func(t *testing.T, c *rdssdk.Client) error
		name     string
		wantCode string
	}{
		{
			name: "bad instance class", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *rdssdk.Client) error {
				t.Helper()

				_, err := c.CreateDBInstance(t.Context(), inst(func(in *rdssdk.CreateDBInstanceInput) {
					in.DBInstanceClass = aws.String("bogus")
				}))

				return err
			},
		},
		{
			name: "short password", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *rdssdk.Client) error {
				t.Helper()

				_, err := c.CreateDBInstance(t.Context(), inst(func(in *rdssdk.CreateDBInstanceInput) {
					in.MasterUserPassword = aws.String("short")
				}))

				return err
			},
		},
		{
			name: "password with at sign", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *rdssdk.Client) error {
				t.Helper()

				_, err := c.CreateDBInstance(t.Context(), inst(func(in *rdssdk.CreateDBInstanceInput) {
					in.MasterUserPassword = aws.String("pass@word123")
				}))

				return err
			},
		},
		{
			name: "mysql password too long", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *rdssdk.Client) error {
				t.Helper()

				long := make([]byte, 42)
				for i := range long {
					long[i] = 'a'
				}

				_, err := c.CreateDBInstance(t.Context(), inst(func(in *rdssdk.CreateDBInstanceInput) {
					in.MasterUserPassword = aws.String(string(long))
				}))

				return err
			},
		},
		{
			name: "cluster password too short", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *rdssdk.Client) error {
				t.Helper()

				_, err := c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("iv-cluster"), Engine: aws.String("aurora-mysql"),
					MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("abc"),
				})

				return err
			},
		},
		{
			name: "cluster id underscore", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *rdssdk.Client) error {
				t.Helper()

				_, err := c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("bad_cluster"), Engine: aws.String("aurora-mysql"),
				})

				return err
			},
		},
		{
			name: "cluster id double hyphen", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *rdssdk.Client) error {
				t.Helper()

				_, err := c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("bad--cluster"), Engine: aws.String("aurora-mysql"),
				})

				return err
			},
		},
		{
			name: "describe instances bad marker", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *rdssdk.Client) error {
				t.Helper()

				_, err := c.DescribeDBInstances(t.Context(), &rdssdk.DescribeDBInstancesInput{
					Marker: aws.String("garbage"),
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClientBackendAndClient(t)

			err := tc.call(t, client)
			require.Error(t, err)
			assert.Equal(t, tc.wantCode, apiErrCode(t, err))
		})
	}
}

func TestInputValidation_DefaultEngineVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		engine  string
		wantVer string
	}{
		{engine: "mysql", wantVer: "8.0.35"},
		{engine: "postgres", wantVer: "15.5"},
		{engine: "mariadb", wantVer: "10.6.14"},
	}

	for _, tc := range tests {
		t.Run(tc.engine, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			inst, err := b.CreateDBInstance(
				"dv-"+tc.engine, tc.engine, "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantVer, inst.EngineVersion)
		})
	}
}
