package backup_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

type fakeInstances struct{ launched []backup.InstanceInfo }

func (f *fakeInstances) DescribeInstance(string) (backup.InstanceInfo, bool) {
	return backup.InstanceInfo{ImageID: "ami-1", InstanceType: "t2.micro", SubnetID: "subnet-1"}, true
}

func (f *fakeInstances) LaunchInstance(info backup.InstanceInfo) (string, error) {
	f.launched = append(f.launched, info)

	return "i-new", nil
}

type fakeDBInstances struct{ created []backup.DBInstanceInfo }

func (f *fakeDBInstances) DescribeDBInstance(string) (backup.DBInstanceInfo, bool) {
	return backup.DBInstanceInfo{Identifier: "db-1", InstanceClass: "db.t3.micro", Engine: "mysql"}, true
}

func (f *fakeDBInstances) CreateDBInstance(info backup.DBInstanceInfo) (string, error) {
	f.created = append(f.created, info)

	return "arn:aws:rds:us-east-1:123456789012:db:" + info.Identifier, nil
}

type fakeStackRecorder struct{ created []backup.StackInfo }

func (f *fakeStackRecorder) DescribeStack(string) (backup.StackInfo, bool) {
	return backup.StackInfo{Name: "orig", Template: "{}"}, true
}

func (f *fakeStackRecorder) CreateStack(_ context.Context, info backup.StackInfo) (string, error) {
	f.created = append(f.created, info)

	return "stack-id", nil
}

type fakeTableRecorder struct{ created []backup.TableInfo }

func (f *fakeTableRecorder) DescribeTable(db, tbl string) (backup.TableInfo, bool) {
	return backup.TableInfo{Database: db, Table: tbl}, true
}

func (f *fakeTableRecorder) CreateTable(info backup.TableInfo) (string, error) {
	f.created = append(f.created, info)

	return "arn:aws:timestream:us-east-1:123456789012:database/" + info.Database + "/table/" + info.Table, nil
}

func TestRestore_DocumentedMetadataKeys(t *testing.T) {
	t.Parallel()

	const (
		ec2Arn = "arn:aws:ec2:us-east-1:123456789012:instance/i-1"
		rdsArn = "arn:aws:rds:us-east-1:123456789012:db:db-1"
		cfnArn = "arn:aws:cloudformation:us-east-1:123456789012:stack/orig/abc"
		tsArn  = "arn:aws:timestream:us-east-1:123456789012:database/srcdb/table/srctbl"
		efsArn = "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-1"
	)

	tests := []struct {
		check        func(t *testing.T, w *wiring)
		md           map[string]string
		name         string
		resourceType string
		resourceArn  string
	}{
		{
			name: "ec2 pascal", resourceType: "EC2", resourceArn: ec2Arn,
			md: map[string]string{"InstanceType": "m5.large", "SubnetId": "subnet-2"},
			check: func(t *testing.T, w *wiring) {
				t.Helper()
				require.Len(t, w.ec2.launched, 1)
				assert.Equal(t, "m5.large", w.ec2.launched[0].InstanceType)
				assert.Equal(t, "subnet-2", w.ec2.launched[0].SubnetID)
			},
		},
		{
			name: "ec2 lowercase", resourceType: "EC2", resourceArn: ec2Arn,
			md: map[string]string{"instancetype": "m5.large"},
			check: func(t *testing.T, w *wiring) {
				t.Helper()
				require.Len(t, w.ec2.launched, 1)
				assert.Equal(t, "m5.large", w.ec2.launched[0].InstanceType)
				assert.Equal(t, "subnet-1", w.ec2.launched[0].SubnetID)
			},
		},
		{
			name: "rds lowercase", resourceType: "RDS", resourceArn: rdsArn,
			md: map[string]string{"dbinstanceidentifier": "db-2", "DBInstanceClass": "db.m5.large"},
			check: func(t *testing.T, w *wiring) {
				t.Helper()
				require.Len(t, w.rds.created, 1)
				assert.Equal(t, "db-2", w.rds.created[0].Identifier)
				assert.Equal(t, "db.m5.large", w.rds.created[0].InstanceClass)
			},
		},
		{
			name: "cloudformation pascal", resourceType: "CloudFormation", resourceArn: cfnArn,
			md: map[string]string{"StackName": "restored", "ChangeSetName": "cs"},
			check: func(t *testing.T, w *wiring) {
				t.Helper()
				require.Len(t, w.cfn.created, 1)
				assert.Equal(t, "restored", w.cfn.created[0].Name)
			},
		},
		{
			name: "timestream pascal", resourceType: "Timestream", resourceArn: tsArn,
			md: map[string]string{"TableName": "newtbl", "DatabaseName": "newdb"},
			check: func(t *testing.T, w *wiring) {
				t.Helper()
				require.Len(t, w.ts.created, 1)
				assert.Equal(t, "newtbl", w.ts.created[0].Table)
				assert.Equal(t, "newdb", w.ts.created[0].Database)
			},
		},
		{
			name: "timestream default", resourceType: "Timestream", resourceArn: tsArn,
			md: map[string]string{"aws:backup:request-id": "r1"},
			check: func(t *testing.T, w *wiring) {
				t.Helper()
				require.Len(t, w.ts.created, 1)
				assert.Equal(t, "srctbl-restored", w.ts.created[0].Table)
				assert.Equal(t, "srcdb", w.ts.created[0].Database)
			},
		},
		{
			name: "efs encrypted", resourceType: "EFS", resourceArn: efsArn,
			md: map[string]string{"Encrypted": "true"},
			check: func(t *testing.T, w *wiring) {
				t.Helper()
				require.Len(t, w.efs.created, 1)
				assert.True(t, w.efs.created[0].Encrypted)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			w := &wiring{
				ec2: &fakeInstances{}, rds: &fakeDBInstances{}, cfn: &fakeStackRecorder{},
				ts: &fakeTableRecorder{}, efs: &fakeFileSystems{},
			}
			b.SetInstanceService(w.ec2, "us-east-1", "123456789012")
			b.SetDBInstanceService(w.rds)
			b.SetStackService(w.cfn)
			b.SetTableService(w.ts)
			b.SetFileSystemService(w.efs, "us-east-1", "123456789012")

			v := mustVault(t, b, "vault")
			rp := completedRecoveryPoint(t, b, v.BackupVaultName, tt.resourceArn, tt.resourceType)

			job, err := b.StartRestoreJob(rp, testRole, tt.resourceType, tt.md)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", job.Status)

			tt.check(t, w)
		})
	}
}

type wiring struct {
	ec2 *fakeInstances
	rds *fakeDBInstances
	cfn *fakeStackRecorder
	ts  *fakeTableRecorder
	efs *fakeFileSystems
}
