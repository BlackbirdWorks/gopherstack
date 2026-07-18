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
