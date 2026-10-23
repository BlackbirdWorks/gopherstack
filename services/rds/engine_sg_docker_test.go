package rds_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestEngineDockerSecurityGroupEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		engine string
		kind   string
		port   int
	}{
		{name: "postgres", engine: "postgres", kind: "postgres", port: 5432},
		{name: "mysql", engine: "mysql", kind: "mysql", port: 3306},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newDockerEngineBackend(t)

			ecb := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			sg, err := ecb.CreateSecurityGroup("db-"+tt.name, "db", "vpc-default")
			require.NoError(t, err)
			require.NoError(t, ecb.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{
				{Protocol: "tcp", IPRange: "203.0.113.0/24", FromPort: tt.port, ToPort: tt.port},
			}))

			enf := sgfilter.New()
			enf.SetSource(ec2.NewHandler(ecb).SecurityGroupSource())
			b.SetSecurityGroupEnforcer(enf)

			_, err = b.CreateDBInstance("sg1", tt.engine, "db.t3.micro", "appdb", "master", "", 20,
				rds.DBInstanceOptions{MasterUserPassword: "firstpass1", VpcSecurityGroupIDs: []string{sg.ID}})
			require.NoError(t, err)

			var inst rds.DBInstance
			require.Eventually(t, func() bool {
				got, derr := b.DescribeDBInstances("sg1")
				require.NoError(t, derr)
				inst = got[0]
				require.NotEqual(t, "failed", inst.DBInstanceStatus)

				return inst.DBInstanceStatus == "available"
			}, dockerEngineWait, 500*time.Millisecond)

			refused := openEngineDB(t, tt.kind, inst, "firstpass1")
			require.Error(t, refused.PingContext(dbCtx(t)), "SG admits only a foreign CIDR")

			require.NoError(t, ecb.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{
				{Protocol: "tcp", IPRange: "127.0.0.1/32", FromPort: tt.port, ToPort: tt.port},
			}))

			admitted := openEngineDB(t, tt.kind, inst, "firstpass1")
			require.NoError(t, admitted.PingContext(dbCtx(t)), "rule added live admits loopback")

			_, _, err = ecb.RevokeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{
				{Protocol: "tcp", IPRange: "127.0.0.1/32", FromPort: tt.port, ToPort: tt.port},
			})
			require.NoError(t, err)

			again := openEngineDB(t, tt.kind, inst, "firstpass1")
			require.Error(t, again.PingContext(dbCtx(t)), "revoked rule refuses new connections")

			_, err = b.DeleteDBInstanceWithOptions("sg1", true, "", true)
			require.NoError(t, err)
		})
	}
}
