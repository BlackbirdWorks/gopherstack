package rds_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRDSHandler_FormActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		body            string
		setupBodies     []string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name: "CreateDBInstance",
			body: "Action=CreateDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=test-db&Engine=postgres&DBInstanceClass=db.t3.micro" +
				"&MasterUsername=admin&DBName=mydb&AllocatedStorage=20",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBInstanceResponse", "test-db", "postgres"},
		},
		{
			name: "CreateDBInstance_MySQL",
			body: "Action=CreateDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=mysql-db&Engine=mysql&DBInstanceClass=db.t3.micro",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBInstanceResponse", "mysql-db", "mysql", "<Port>3306</Port>"},
		},
		{
			name:         "CreateDBInstance_DefaultEngine",
			body:         "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=default-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"postgres", "<Port>5432</Port>"},
		},
		{
			name:         "CreateDBInstance_InvalidAllocatedStorage",
			body:         "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=bad-db&AllocatedStorage=abc",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:        "ModifyDBInstance_InvalidAllocatedStorage",
			setupBodies: []string{"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=mod-bad-db"},
			body: "Action=ModifyDBInstance&Version=2014-10-31&DBInstanceIdentifier=mod-bad-db&" +
				"AllocatedStorage=notanumber",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateDBSnapshot_EmptySnapshotID",
			setupBodies:  []string{"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-empty-db"},
			body:         "Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=&DBInstanceIdentifier=snap-empty-db",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateDBSnapshot_EmptyInstanceID",
			body:         "Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-noinst&DBInstanceIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateDBSubnetGroup_EmptyName",
			body:         "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateDBInstance_EmptyID",
			body:         "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateDBInstance_Duplicate",
			setupBodies:  []string{"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=dup-db"},
			body:         "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=dup-db",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceAlreadyExists"},
		},
		{
			name:        "DeleteDBInstance",
			setupBodies: []string{"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db"},
			body: "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db" +
				"&SkipFinalSnapshot=true",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDBInstanceResponse", "del-db"},
		},
		{
			name:     "DeleteDBInstance_NotFound",
			body:     "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=nonexistent",
			wantCode: http.StatusBadRequest,
			wantContains: []string{
				"DBInstanceNotFound",
			},
		},
		{
			name: "DeleteDBInstance_MissingSkipFinalSnapshotAndFinalSnapshotID",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db-nosnap",
			},
			body:         "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db-nosnap",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterCombination", "FinalDBSnapshotIdentifier"},
		},
		{
			name: "DeleteDBInstance_FinalSnapshotIDWithSkipFinalSnapshot",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db-badcombo",
			},
			body: "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db-badcombo" +
				"&SkipFinalSnapshot=true&FinalDBSnapshotIdentifier=del-db-badcombo-final",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterCombination", "FinalDBSnapshotIdentifier"},
		},
		{
			name: "DeleteDBInstance_WithFinalSnapshot",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db-finalsnap",
			},
			body: "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=del-db-finalsnap" +
				"&FinalDBSnapshotIdentifier=del-db-finalsnap-final",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDBInstanceResponse", "del-db-finalsnap"},
		},
		{
			name:         "DescribeDBInstances",
			setupBodies:  []string{"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=desc-db"},
			body:         "Action=DescribeDBInstances&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBInstancesResponse", "desc-db"},
		},
		{
			name: "DescribeDBInstances_ByID",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db-one",
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db-two",
			},
			body:            "Action=DescribeDBInstances&Version=2014-10-31&DBInstanceIdentifier=db-one",
			wantCode:        http.StatusOK,
			wantContains:    []string{"db-one"},
			wantNotContains: []string{"db-two"},
		},
		{
			name:         "DescribeDBInstances_NotFound",
			body:         "Action=DescribeDBInstances&Version=2014-10-31&DBInstanceIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name: "ModifyDBInstance",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31" +
					"&DBInstanceIdentifier=mod-db&DBInstanceClass=db.t3.micro&AllocatedStorage=20",
			},
			body: "Action=ModifyDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=mod-db&DBInstanceClass=db.r5.large&AllocatedStorage=100",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyDBInstanceResponse", "db.r5.large"},
		},
		{
			name:         "ModifyDBInstance_NotFound",
			body:         "Action=ModifyDBInstance&Version=2014-10-31&DBInstanceIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name:         "CreateDBSnapshot",
			setupBodies:  []string{"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db"},
			body:         "Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-1&DBInstanceIdentifier=snap-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBSnapshotResponse", "snap-1"},
		},
		{
			name: "CreateDBSnapshot_InstanceNotFound",
			body: "Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-1&" +
				"DBInstanceIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name: "CreateDBSnapshot_Duplicate",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db2",
				"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=dup-snap&DBInstanceIdentifier=snap-db2",
			},
			body: "Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=dup-snap&" +
				"DBInstanceIdentifier=snap-db2",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSnapshotAlreadyExists"},
		},
		{
			name: "DescribeDBSnapshots",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db3",
				"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=list-snap&DBInstanceIdentifier=snap-db3",
			},
			body:         "Action=DescribeDBSnapshots&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBSnapshotsResponse", "list-snap"},
		},
		{
			name:         "DescribeDBSnapshots_NotFound",
			body:         "Action=DescribeDBSnapshots&Version=2014-10-31&DBSnapshotIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSnapshotNotFound"},
		},
		{
			name: "DeleteDBSnapshot",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db4",
				"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=del-snap&DBInstanceIdentifier=snap-db4",
			},
			body:         "Action=DeleteDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=del-snap",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDBSnapshotResponse", "del-snap"},
		},
		{
			name:         "DeleteDBSnapshot_NotFound",
			body:         "Action=DeleteDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSnapshotNotFound"},
		},
		{
			name: "CreateDBSubnetGroup",
			body: "Action=CreateDBSubnetGroup&Version=2014-10-31" +
				"&DBSubnetGroupName=my-subnet-group&DBSubnetGroupDescription=My+group" +
				"&VpcId=vpc-12345" +
				"&SubnetIds.SubnetIdentifier.1=subnet-1&SubnetIds.SubnetIdentifier.2=subnet-2",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBSubnetGroupResponse", "my-subnet-group", "subnet-1"},
		},
		{
			name:         "CreateDBSubnetGroup_Duplicate",
			setupBodies:  []string{"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=dup-sg"},
			body:         "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=dup-sg",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSubnetGroupAlreadyExists"},
		},
		{
			name:         "DescribeDBSubnetGroups",
			setupBodies:  []string{"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=list-sg"},
			body:         "Action=DescribeDBSubnetGroups&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBSubnetGroupsResponse", "list-sg"},
		},
		{
			name: "DescribeDBSubnetGroups_ByName",
			setupBodies: []string{
				"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=find-sg",
				"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=other-sg",
			},
			body:            "Action=DescribeDBSubnetGroups&Version=2014-10-31&DBSubnetGroupName=find-sg",
			wantCode:        http.StatusOK,
			wantContains:    []string{"find-sg"},
			wantNotContains: []string{"other-sg"},
		},
		{
			name:         "DescribeDBSubnetGroups_NotFound",
			body:         "Action=DescribeDBSubnetGroups&Version=2014-10-31&DBSubnetGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSubnetGroupNotFoundFault"},
		},
		{
			name:         "DeleteDBSubnetGroup",
			setupBodies:  []string{"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=del-sg"},
			body:         "Action=DeleteDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=del-sg",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDBSubnetGroupResponse"},
		},
		{
			name:         "DeleteDBSubnetGroup_NotFound",
			body:         "Action=DeleteDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBSubnetGroupNotFoundFault"},
		},
		{
			name: "ListTagsForResource",
			body: "Action=ListTagsForResource&Version=2014-10-31&" +
				"ResourceName=arn:aws:rds:us-east-1:000000000000:db:test-db",
			wantCode:        http.StatusOK,
			wantContains:    []string{"ListTagsForResourceResponse"},
			wantNotContains: []string{"<Tag>"},
		},
		{
			name: "AddTagsToResource_Overwrite",
			setupBodies: []string{
				"Action=AddTagsToResource&Version=2014-10-31" +
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:db:tag-db" +
					"&Tags.Tag.1.Key=Env&Tags.Tag.1.Value=staging",
				"Action=AddTagsToResource&Version=2014-10-31" +
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:db:tag-db" +
					"&Tags.Tag.1.Key=Env&Tags.Tag.1.Value=prod",
			},
			body: "Action=ListTagsForResource&Version=2014-10-31&" +
				"ResourceName=arn:aws:rds:us-east-1:000000000000:db:tag-db",
			wantCode:        http.StatusOK,
			wantContains:    []string{"<Value>prod</Value>"},
			wantNotContains: []string{"<Value>staging</Value>"},
		},
		{
			name:         "InvalidAction",
			body:         "Action=InvalidAction&Version=2014-10-31",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidAction"},
		},
		{
			name:         "MissingAction",
			body:         "Version=2014-10-31",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"MissingAction"},
		},
		// Parameter Group tests
		{
			name: "CreateDBParameterGroup",
			body: "Action=CreateDBParameterGroup&Version=2014-10-31" +
				"&DBParameterGroupName=my-pg&DBParameterGroupFamily=postgres14&Description=My+param+group",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBParameterGroupResponse", "my-pg", "postgres14"},
		},
		{
			name:         "CreateDBParameterGroup_EmptyName",
			body:         "Action=CreateDBParameterGroup&Version=2014-10-31&DBParameterGroupName=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "CreateDBParameterGroup_Duplicate",
			setupBodies: []string{
				"Action=CreateDBParameterGroup&Version=2014-10-31" +
					"&DBParameterGroupName=dup-pg&DBParameterGroupFamily=mysql8.0",
			},
			body: "Action=CreateDBParameterGroup&Version=2014-10-31" +
				"&DBParameterGroupName=dup-pg&DBParameterGroupFamily=mysql8.0",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBParameterGroupAlreadyExists"},
		},
		{
			name: "DescribeDBParameterGroups",
			setupBodies: []string{
				"Action=CreateDBParameterGroup&Version=2014-10-31&DBParameterGroupName=list-pg&DBParameterGroupFamily=postgres14",
			},
			body:         "Action=DescribeDBParameterGroups&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBParameterGroupsResponse", "list-pg"},
		},
		{
			name:         "DescribeDBParameterGroups_NotFound",
			body:         "Action=DescribeDBParameterGroups&Version=2014-10-31&DBParameterGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBParameterGroupNotFound"},
		},
		{
			name: "DeleteDBParameterGroup",
			setupBodies: []string{
				"Action=CreateDBParameterGroup&Version=2014-10-31&DBParameterGroupName=del-pg&DBParameterGroupFamily=postgres14",
			},
			body:         "Action=DeleteDBParameterGroup&Version=2014-10-31&DBParameterGroupName=del-pg",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDBParameterGroupResponse"},
		},
		{
			name:         "DeleteDBParameterGroup_NotFound",
			body:         "Action=DeleteDBParameterGroup&Version=2014-10-31&DBParameterGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBParameterGroupNotFound"},
		},
		{
			name: "ModifyDBParameterGroup",
			setupBodies: []string{
				"Action=CreateDBParameterGroup&Version=2014-10-31&DBParameterGroupName=mod-pg&DBParameterGroupFamily=postgres14",
			},
			body: "Action=ModifyDBParameterGroup&Version=2014-10-31&DBParameterGroupName=mod-pg" +
				"&Parameters.Parameter.1.ParameterName=max_connections&Parameters.Parameter.1.ParameterValue=200",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyDBParameterGroupResponse", "mod-pg"},
		},
		{
			name: "DescribeDBParameters",
			setupBodies: []string{
				"Action=CreateDBParameterGroup&Version=2014-10-31" +
					"&DBParameterGroupName=desc-param-pg&DBParameterGroupFamily=postgres14",
				"Action=ModifyDBParameterGroup&Version=2014-10-31&DBParameterGroupName=desc-param-pg" +
					"&Parameters.Parameter.1.ParameterName=max_connections&Parameters.Parameter.1.ParameterValue=100",
			},
			body:         "Action=DescribeDBParameters&Version=2014-10-31&DBParameterGroupName=desc-param-pg",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBParametersResponse", "max_connections"},
		},
		{
			name: "ResetDBParameterGroup",
			setupBodies: []string{
				"Action=CreateDBParameterGroup&Version=2014-10-31&DBParameterGroupName=reset-pg&DBParameterGroupFamily=postgres14",
				"Action=ModifyDBParameterGroup&Version=2014-10-31&DBParameterGroupName=reset-pg" +
					"&Parameters.Parameter.1.ParameterName=max_connections&Parameters.Parameter.1.ParameterValue=200",
			},
			body: "Action=ResetDBParameterGroup&Version=2014-10-31" +
				"&DBParameterGroupName=reset-pg&ResetAllParameters=true",
			wantCode:     http.StatusOK,
			wantContains: []string{"ResetDBParameterGroupResponse", "reset-pg"},
		},
		// Option Group tests
		{
			name: "CreateOptionGroup",
			body: "Action=CreateOptionGroup&Version=2014-10-31" +
				"&OptionGroupName=my-og&EngineName=mysql&MajorEngineVersion=8.0&OptionGroupDescription=My+option+group",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateOptionGroupResponse", "my-og", "mysql"},
		},
		{
			name:         "CreateOptionGroup_EmptyName",
			body:         "Action=CreateOptionGroup&Version=2014-10-31&OptionGroupName=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "CreateOptionGroup_Duplicate",
			setupBodies: []string{
				"Action=CreateOptionGroup&Version=2014-10-31" +
					"&OptionGroupName=dup-og&EngineName=mysql&MajorEngineVersion=8.0",
			},
			body: "Action=CreateOptionGroup&Version=2014-10-31" +
				"&OptionGroupName=dup-og&EngineName=mysql&MajorEngineVersion=8.0",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"OptionGroupAlreadyExists"},
		},
		{
			name: "DescribeOptionGroups",
			setupBodies: []string{
				"Action=CreateOptionGroup&Version=2014-10-31&OptionGroupName=list-og&EngineName=mysql&MajorEngineVersion=8.0",
			},
			body:         "Action=DescribeOptionGroups&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeOptionGroupsResponse", "list-og"},
		},
		{
			name:         "DescribeOptionGroups_NotFound",
			body:         "Action=DescribeOptionGroups&Version=2014-10-31&OptionGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"OptionGroupNotFound"},
		},
		{
			name: "DeleteOptionGroup",
			setupBodies: []string{
				"Action=CreateOptionGroup&Version=2014-10-31&OptionGroupName=del-og&EngineName=mysql&MajorEngineVersion=8.0",
			},
			body:         "Action=DeleteOptionGroup&Version=2014-10-31&OptionGroupName=del-og",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteOptionGroupResponse"},
		},
		{
			name:         "DeleteOptionGroup_NotFound",
			body:         "Action=DeleteOptionGroup&Version=2014-10-31&OptionGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"OptionGroupNotFound"},
		},
		{
			name: "ModifyOptionGroup_Add",
			setupBodies: []string{
				"Action=CreateOptionGroup&Version=2014-10-31&OptionGroupName=mod-og&EngineName=mysql&MajorEngineVersion=8.0",
			},
			body: "Action=ModifyOptionGroup&Version=2014-10-31&OptionGroupName=mod-og" +
				"&OptionsToInclude.OptionConfiguration.1.OptionName=MEMCACHED",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyOptionGroupResponse", "MEMCACHED"},
		},
		// Cluster tests
		{
			name: "CreateDBCluster",
			body: "Action=CreateDBCluster&Version=2014-10-31" +
				"&DBClusterIdentifier=my-cluster&Engine=aurora-postgresql&MasterUsername=admin&DatabaseName=mydb",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBClusterResponse", "my-cluster", "aurora-postgresql"},
		},
		{
			name:         "CreateDBCluster_EmptyID",
			body:         "Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "CreateDBCluster_Duplicate",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=dup-cluster&Engine=aurora-postgresql",
			},
			body:         "Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=dup-cluster&Engine=aurora-postgresql",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterAlreadyExists"},
		},
		{
			name: "DescribeDBClusters",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=list-cluster&Engine=aurora-postgresql",
			},
			body:         "Action=DescribeDBClusters&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBClustersResponse", "list-cluster"},
		},
		{
			name:         "DescribeDBClusters_NotFound",
			body:         "Action=DescribeDBClusters&Version=2014-10-31&DBClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterNotFound"},
		},
		{
			name: "DeleteDBCluster",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=del-cluster&Engine=aurora-postgresql",
			},
			body: "Action=DeleteDBCluster&Version=2014-10-31&DBClusterIdentifier=del-cluster" +
				"&SkipFinalSnapshot=true",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDBClusterResponse", "del-cluster"},
		},
		{
			name:         "DeleteDBCluster_NotFound",
			body:         "Action=DeleteDBCluster&Version=2014-10-31&DBClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterNotFound"},
		},
		{
			name: "DeleteDBCluster_MissingSkipFinalSnapshotAndFinalSnapshotID",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=del-cluster-nosnap" +
					"&Engine=aurora-postgresql",
			},
			body:         "Action=DeleteDBCluster&Version=2014-10-31&DBClusterIdentifier=del-cluster-nosnap",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterCombination", "FinalDBSnapshotIdentifier"},
		},
		{
			name: "DeleteDBCluster_WithFinalSnapshot",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=del-cluster-finalsnap" +
					"&Engine=aurora-postgresql",
			},
			body: "Action=DeleteDBCluster&Version=2014-10-31&DBClusterIdentifier=del-cluster-finalsnap" +
				"&FinalDBSnapshotIdentifier=del-cluster-finalsnap-final",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDBClusterResponse", "del-cluster-finalsnap"},
		},
		{
			name: "ModifyDBCluster",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=mod-cluster&Engine=aurora-postgresql",
			},
			body: "Action=ModifyDBCluster&Version=2014-10-31&DBClusterIdentifier=mod-cluster" +
				"&DBClusterParameterGroupName=my-cluster-pg",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyDBClusterResponse", "my-cluster-pg"},
		},
		// Cluster Parameter Group tests
		{
			name: "CreateDBClusterParameterGroup",
			body: "Action=CreateDBClusterParameterGroup&Version=2014-10-31" +
				"&DBClusterParameterGroupName=my-cpg&DBParameterGroupFamily=aurora-postgresql14&Description=My+cluster+pg",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBClusterParameterGroupResponse", "my-cpg"},
		},
		{
			name: "DescribeDBClusterParameterGroups",
			setupBodies: []string{
				"Action=CreateDBClusterParameterGroup&Version=2014-10-31" +
					"&DBClusterParameterGroupName=list-cpg&DBParameterGroupFamily=aurora-postgresql14",
			},
			body:         "Action=DescribeDBClusterParameterGroups&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBClusterParameterGroupsResponse", "list-cpg"},
		},
		// Cluster Snapshot tests
		{
			name: "CreateDBClusterSnapshot",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=snap-cluster&Engine=aurora-postgresql",
			},
			body: "Action=CreateDBClusterSnapshot&Version=2014-10-31" +
				"&DBClusterSnapshotIdentifier=cluster-snap-1&DBClusterIdentifier=snap-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBClusterSnapshotResponse", "cluster-snap-1"},
		},
		{
			name: "CreateDBClusterSnapshot_ClusterNotFound",
			body: "Action=CreateDBClusterSnapshot&Version=2014-10-31" +
				"&DBClusterSnapshotIdentifier=orphan-snap&DBClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterNotFound"},
		},
		{
			name: "DescribeDBClusterSnapshots",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=snap-cluster2&Engine=aurora-postgresql",
				"Action=CreateDBClusterSnapshot&Version=2014-10-31" +
					"&DBClusterSnapshotIdentifier=list-csnap&DBClusterIdentifier=snap-cluster2",
			},
			body:         "Action=DescribeDBClusterSnapshots&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBClusterSnapshotsResponse", "list-csnap"},
		},
		// StartDBCluster / StopDBCluster tests
		{
			name: "StartDBCluster",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=start-cluster&Engine=aurora-postgresql",
				"Action=StopDBCluster&Version=2014-10-31&DBClusterIdentifier=start-cluster",
			},
			body:         "Action=StartDBCluster&Version=2014-10-31&DBClusterIdentifier=start-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"StartDBClusterResponse", "start-cluster", "available"},
		},
		{
			name:         "StartDBCluster_NotFound",
			body:         "Action=StartDBCluster&Version=2014-10-31&DBClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterNotFound"},
		},
		{
			name:         "StartDBCluster_EmptyID",
			body:         "Action=StartDBCluster&Version=2014-10-31&DBClusterIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "StopDBCluster",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=stop-cluster&Engine=aurora-postgresql",
			},
			body:         "Action=StopDBCluster&Version=2014-10-31&DBClusterIdentifier=stop-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"StopDBClusterResponse", "stop-cluster", "stopped"},
		},
		{
			name:         "StopDBCluster_NotFound",
			body:         "Action=StopDBCluster&Version=2014-10-31&DBClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterNotFound"},
		},
		{
			name:         "StopDBCluster_EmptyID",
			body:         "Action=StopDBCluster&Version=2014-10-31&DBClusterIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		// DeleteDBClusterSnapshot tests
		{
			name: "DeleteDBClusterSnapshot",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=delsnap-cluster&Engine=aurora-postgresql",
				"Action=CreateDBClusterSnapshot&Version=2014-10-31" +
					"&DBClusterSnapshotIdentifier=del-csnap&DBClusterIdentifier=delsnap-cluster",
			},
			body:         "Action=DeleteDBClusterSnapshot&Version=2014-10-31&DBClusterSnapshotIdentifier=del-csnap",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDBClusterSnapshotResponse", "del-csnap"},
		},
		{
			name:         "DeleteDBClusterSnapshot_NotFound",
			body:         "Action=DeleteDBClusterSnapshot&Version=2014-10-31&DBClusterSnapshotIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterSnapshotNotFound"},
		},
		{
			name:         "DeleteDBClusterSnapshot_EmptyID",
			body:         "Action=DeleteDBClusterSnapshot&Version=2014-10-31&DBClusterSnapshotIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		// RestoreDBClusterFromSnapshot tests
		{
			name: "RestoreDBClusterFromSnapshot",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=restore-src&Engine=aurora-postgresql",
				"Action=CreateDBClusterSnapshot&Version=2014-10-31" +
					"&DBClusterSnapshotIdentifier=restore-snap&DBClusterIdentifier=restore-src",
			},
			body: "Action=RestoreDBClusterFromSnapshot&Version=2014-10-31" +
				"&DBClusterIdentifier=restored-cluster&SnapshotIdentifier=restore-snap&Engine=aurora-postgresql",
			wantCode:     http.StatusOK,
			wantContains: []string{"RestoreDBClusterFromSnapshotResponse", "restored-cluster"},
		},
		{
			name: "RestoreDBClusterFromSnapshot_SnapshotNotFound",
			body: "Action=RestoreDBClusterFromSnapshot&Version=2014-10-31" +
				"&DBClusterIdentifier=new-cluster&SnapshotIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterSnapshotNotFound"},
		},
		{
			name: "RestoreDBClusterFromSnapshot_EmptyID",
			body: "Action=RestoreDBClusterFromSnapshot&Version=2014-10-31" +
				"&DBClusterIdentifier=&SnapshotIdentifier=some-snap",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "RestoreDBClusterFromSnapshot_EmptySnapshotID",
			body: "Action=RestoreDBClusterFromSnapshot&Version=2014-10-31" +
				"&DBClusterIdentifier=some-cluster&SnapshotIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		// RestoreDBClusterToPointInTime tests
		{
			name: "RestoreDBClusterToPointInTime",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31" +
					"&DBClusterIdentifier=pitr-src&Engine=aurora-postgresql&MasterUsername=admin",
			},
			body: "Action=RestoreDBClusterToPointInTime&Version=2014-10-31" +
				"&DBClusterIdentifier=pitr-restored&SourceDBClusterIdentifier=pitr-src",
			wantCode:     http.StatusOK,
			wantContains: []string{"RestoreDBClusterToPointInTimeResponse", "pitr-restored"},
		},
		{
			name: "RestoreDBClusterToPointInTime_SourceNotFound",
			body: "Action=RestoreDBClusterToPointInTime&Version=2014-10-31" +
				"&DBClusterIdentifier=pitr-new&SourceDBClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterNotFound"},
		},
		{
			name: "RestoreDBClusterToPointInTime_EmptySourceID",
			body: "Action=RestoreDBClusterToPointInTime&Version=2014-10-31" +
				"&DBClusterIdentifier=pitr-new&SourceDBClusterIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		// CopyDBClusterSnapshot tests
		{
			name: "CopyDBClusterSnapshot",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=copy-src-cluster&Engine=aurora-postgresql",
				"Action=CreateDBClusterSnapshot&Version=2014-10-31" +
					"&DBClusterSnapshotIdentifier=copy-src-snap&DBClusterIdentifier=copy-src-cluster",
			},
			body: "Action=CopyDBClusterSnapshot&Version=2014-10-31" +
				"&SourceDBClusterSnapshotIdentifier=copy-src-snap&TargetDBClusterSnapshotIdentifier=copy-dst-snap",
			wantCode:     http.StatusOK,
			wantContains: []string{"CopyDBClusterSnapshotResponse", "copy-dst-snap"},
		},
		{
			name: "CopyDBClusterSnapshot_SourceNotFound",
			body: "Action=CopyDBClusterSnapshot&Version=2014-10-31" +
				"&SourceDBClusterSnapshotIdentifier=nonexistent&TargetDBClusterSnapshotIdentifier=dst-snap",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterSnapshotNotFound"},
		},
		{
			name: "CopyDBClusterSnapshot_EmptySourceID",
			body: "Action=CopyDBClusterSnapshot&Version=2014-10-31" +
				"&SourceDBClusterSnapshotIdentifier=&TargetDBClusterSnapshotIdentifier=dst-snap",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		// CreateDBClusterEndpoint tests
		{
			name: "CreateDBClusterEndpoint",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=ep-cluster&Engine=aurora-postgresql",
			},
			body: "Action=CreateDBClusterEndpoint&Version=2014-10-31" +
				"&DBClusterEndpointIdentifier=my-endpoint&DBClusterIdentifier=ep-cluster&EndpointType=READER",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBClusterEndpointResponse", "my-endpoint", "READER"},
		},
		{
			name: "CreateDBClusterEndpoint_ClusterNotFound",
			body: "Action=CreateDBClusterEndpoint&Version=2014-10-31" +
				"&DBClusterEndpointIdentifier=ep&DBClusterIdentifier=nonexistent&EndpointType=READER",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterNotFound"},
		},
		{
			name: "CreateDBClusterEndpoint_EmptyID",
			body: "Action=CreateDBClusterEndpoint&Version=2014-10-31" +
				"&DBClusterEndpointIdentifier=&DBClusterIdentifier=some-cluster&EndpointType=READER",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "CreateDBClusterEndpoint_EmptyClusterID",
			body: "Action=CreateDBClusterEndpoint&Version=2014-10-31" +
				"&DBClusterEndpointIdentifier=my-ep&DBClusterIdentifier=&EndpointType=READER",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		// DescribeDBClusterEndpoints tests
		{
			name: "DescribeDBClusterEndpoints",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=eplist-cluster&Engine=aurora-postgresql",
				"Action=CreateDBClusterEndpoint&Version=2014-10-31" +
					"&DBClusterEndpointIdentifier=list-ep&DBClusterIdentifier=eplist-cluster&EndpointType=READER",
			},
			body:         "Action=DescribeDBClusterEndpoints&Version=2014-10-31&DBClusterIdentifier=eplist-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBClusterEndpointsResponse", "list-ep"},
		},
		{
			name:         "DescribeDBClusterEndpoints_NotFound",
			body:         "Action=DescribeDBClusterEndpoints&Version=2014-10-31&DBClusterEndpointIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterEndpointNotFound"},
		},
		// DeleteDBClusterEndpoint tests
		{
			name: "DeleteDBClusterEndpoint",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=dep-cluster&Engine=aurora-postgresql",
				"Action=CreateDBClusterEndpoint&Version=2014-10-31" +
					"&DBClusterEndpointIdentifier=dep-ep&DBClusterIdentifier=dep-cluster&EndpointType=READER",
			},
			body:         "Action=DeleteDBClusterEndpoint&Version=2014-10-31&DBClusterEndpointIdentifier=dep-ep",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteDBClusterEndpointResponse", "dep-ep"},
		},
		{
			name:         "DeleteDBClusterEndpoint_NotFound",
			body:         "Action=DeleteDBClusterEndpoint&Version=2014-10-31&DBClusterEndpointIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBClusterEndpointNotFound"},
		},
		// DescribeValidDBInstanceModifications tests
		{
			name: "DescribeValidDBInstanceModifications",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=mod-valid-db&Engine=postgres",
			},
			body:         "Action=DescribeValidDBInstanceModifications&Version=2014-10-31&DBInstanceIdentifier=mod-valid-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeValidDBInstanceModificationsResponse", "coreCount", "threadsPerCore"},
		},
		{
			name:         "DescribeValidDBInstanceModifications_NotFound",
			body:         "Action=DescribeValidDBInstanceModifications&Version=2014-10-31&DBInstanceIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		// StartExportTask tests
		{
			name: "StartExportTask",
			body: "Action=StartExportTask&Version=2014-10-31" +
				"&ExportTaskIdentifier=my-export&SourceArn=arn:aws:rds:us-east-1:000000000000:snapshot:my-snap" +
				"&S3BucketName=my-bucket",
			wantCode:     http.StatusOK,
			wantContains: []string{"StartExportTaskResponse", "my-export", "complete"},
		},
		{
			name: "StartExportTask_EmptyID",
			body: "Action=StartExportTask&Version=2014-10-31" +
				"&ExportTaskIdentifier=&SourceArn=arn:aws:rds:us-east-1:000000000000:snapshot:my-snap",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "StartExportTask_Duplicate",
			setupBodies: []string{
				"Action=StartExportTask&Version=2014-10-31" +
					"&ExportTaskIdentifier=dup-export&SourceArn=arn:aws:rds:us-east-1:000000000000:snapshot:s1",
			},
			body: "Action=StartExportTask&Version=2014-10-31" +
				"&ExportTaskIdentifier=dup-export&SourceArn=arn:aws:rds:us-east-1:000000000000:snapshot:s1",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ExportTaskAlreadyExists"},
		},
		// DescribeExportTasks tests
		{
			name: "DescribeExportTasks",
			setupBodies: []string{
				"Action=StartExportTask&Version=2014-10-31" +
					"&ExportTaskIdentifier=list-export&SourceArn=arn:aws:rds:us-east-1:000000000000:snapshot:s2",
			},
			body:         "Action=DescribeExportTasks&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeExportTasksResponse", "list-export"},
		},
		{
			name:         "DescribeExportTasks_NotFound",
			body:         "Action=DescribeExportTasks&Version=2014-10-31&ExportTaskIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ExportTaskNotFound"},
		},
		// CancelExportTask tests
		{
			name: "CancelExportTask",
			setupBodies: []string{
				"Action=StartExportTask&Version=2014-10-31" +
					"&ExportTaskIdentifier=cancel-export&SourceArn=arn:aws:rds:us-east-1:000000000000:snapshot:s3",
			},
			body:         "Action=CancelExportTask&Version=2014-10-31&ExportTaskIdentifier=cancel-export",
			wantCode:     http.StatusOK,
			wantContains: []string{"CancelExportTaskResponse", "cancel-export", "canceled"},
		},
		{
			name:         "CancelExportTask_EmptyID",
			body:         "Action=CancelExportTask&Version=2014-10-31&ExportTaskIdentifier=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CancelExportTask_NotFound",
			body:         "Action=CancelExportTask&Version=2014-10-31&ExportTaskIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ExportTaskNotFound"},
		},
		{
			name: "CreateDBInstanceReadReplica",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=source-db&Engine=postgres",
			},
			body: "Action=CreateDBInstanceReadReplica&Version=2014-10-31" +
				"&DBInstanceIdentifier=replica-db&SourceDBInstanceIdentifier=source-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBInstanceReadReplicaResponse", "replica-db", "source-db"},
		},
		{
			name: "CreateDBInstanceReadReplica_SourceNotFound",
			body: "Action=CreateDBInstanceReadReplica&Version=2014-10-31" +
				"&DBInstanceIdentifier=replica-db&SourceDBInstanceIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name: "PromoteReadReplica",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=promo-source&Engine=postgres",
				"Action=CreateDBInstanceReadReplica&Version=2014-10-31" +
					"&DBInstanceIdentifier=promo-replica&SourceDBInstanceIdentifier=promo-source",
			},
			body:         "Action=PromoteReadReplica&Version=2014-10-31&DBInstanceIdentifier=promo-replica",
			wantCode:     http.StatusOK,
			wantContains: []string{"PromoteReadReplicaResponse", "promo-replica"},
		},
		// Misc tests
		{
			name: "RebootDBInstance",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=reboot-db&Engine=postgres",
			},
			body:         "Action=RebootDBInstance&Version=2014-10-31&DBInstanceIdentifier=reboot-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"RebootDBInstanceResponse", "reboot-db"},
		},
		{
			name:         "RebootDBInstance_NotFound",
			body:         "Action=RebootDBInstance&Version=2014-10-31&DBInstanceIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name:         "DescribeDBEngineVersions",
			body:         "Action=DescribeDBEngineVersions&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBEngineVersionsResponse", "postgres"},
		},
		{
			name:            "DescribeDBEngineVersions_ByEngine",
			body:            "Action=DescribeDBEngineVersions&Version=2014-10-31&Engine=mysql",
			wantCode:        http.StatusOK,
			wantContains:    []string{"mysql"},
			wantNotContains: []string{"aurora-postgresql"},
		},
		{
			name:         "DescribeOrderableDBInstanceOptions",
			body:         "Action=DescribeOrderableDBInstanceOptions&Version=2014-10-31&Engine=postgres",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeOrderableDBInstanceOptionsResponse", "db.t3.micro"},
		},
		{
			name: "DescribeDBLogFiles",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=log-db&Engine=postgres",
			},
			body:         "Action=DescribeDBLogFiles&Version=2014-10-31&DBInstanceIdentifier=log-db",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDBLogFilesResponse"},
		},
		{
			name:         "DescribeDBLogFiles_NotFound",
			body:         "Action=DescribeDBLogFiles&Version=2014-10-31&DBInstanceIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name: "DownloadDBLogFilePortion",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=logportion-db&Engine=postgres",
			},
			body: "Action=DownloadDBLogFilePortion&Version=2014-10-31" +
				"&DBInstanceIdentifier=logportion-db&LogFileName=error/postgresql.log",
			wantCode:     http.StatusOK,
			wantContains: []string{"DownloadDBLogFilePortionResponse"},
		},
		{
			name: "DownloadDBLogFilePortion_NotFound",
			body: "Action=DownloadDBLogFilePortion&Version=2014-10-31" +
				"&DBInstanceIdentifier=nonexistent&LogFileName=error.log",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DBInstanceNotFound"},
		},
		{
			name:         "DescribeGlobalClusters",
			body:         "Action=DescribeGlobalClusters&Version=2014-10-31",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeGlobalClustersResponse"},
		},
		{
			name:         "DescribeOptionGroupOptions",
			body:         "Action=DescribeOptionGroupOptions&Version=2014-10-31&EngineName=mysql",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeOptionGroupOptionsResponse"},
		},
		{
			name: "CreateDBInstance_WithParameterGroup",
			body: "Action=CreateDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=pg-db&Engine=postgres&DBParameterGroupName=my-custom-pg",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateDBInstanceResponse", "pg-db", "my-custom-pg"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()

			for _, setup := range tt.setupBodies {
				postRDSForm(t, h, setup)
			}

			rec := postRDSForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			body := rec.Body.String()
			for _, s := range tt.wantContains {
				assert.Contains(t, body, s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, body, s)
			}
		})
	}
}
