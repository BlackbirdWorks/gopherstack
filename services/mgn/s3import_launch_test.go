package mgn_test

import (
	"bytes"
	"encoding/csv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/aws/aws-sdk-go-v2/service/mgn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mgn"
)

func TestStartImport_LaunchTemplateColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *mgn.LaunchConfiguration, *mgnsdk.GetLaunchConfigurationOutput)
		name       string
		header     []string
		row        []string
		wantErrors int
	}{
		{
			name: "applies_template_contents",
			header: []string{
				"mgn:server:hostname", "mgn:launch:instance-type", "mgn:launch:iam-instance-profile:name",
				"mgn:launch:placement:tenancy", "mgn:launch:placement:host-id", "mgn:launch:map-tag-key",
				"mgn:launch:nic:0:subnet-id", "mgn:launch:nic:0:private-ip:0", "mgn:launch:nic:0:security-group-id:0",
				"mgn:launch:nic:1:network-interface-id", "mgn:launch:volume:/dev/sda1:type",
				"mgn:launch:tag:instance:Env", "mgn:launch:post-actions:enabled",
			},
			row: []string{
				"l1.example.com", "m4.large", "prof", "host", "h-1", "aws-apn-id", "subnet-1", "10.0.0.5", "sg-1",
				"eni-2", "gp3", "Prod", "true",
			},
			check: func(t *testing.T, lc *mgn.LaunchConfiguration, _ *mgnsdk.GetLaunchConfigurationOutput) {
				t.Helper()

				c := lc.TemplateContents
				require.NotNil(t, c)
				assert.Equal(t, "m4.large", c.InstanceType)
				assert.Equal(t, "prof", c.IamProfileName)
				assert.Equal(t, "host", c.Tenancy)
				assert.Equal(t, "h-1", c.HostID)
				assert.Equal(t, "aws-apn-id", c.MapTagKey)
				assert.Equal(t, map[string]string{"/dev/sda1": "gp3"}, c.Volumes)
				assert.Equal(t, map[string]string{"Env": "Prod"}, c.InstanceTags)
				assert.True(t, aws.ToBool(c.PostActionsEnabled))
				require.Len(t, c.NetworkInterfaces, 2)
				assert.Equal(t, "subnet-1", c.NetworkInterfaces[0].SubnetID)
				assert.Equal(t, []string{"10.0.0.5"}, c.NetworkInterfaces[0].PrivateIPs)
				assert.Equal(t, []string{"sg-1"}, c.NetworkInterfaces[0].SecurityGroupIDs)
				assert.Equal(t, "eni-2", c.NetworkInterfaces[1].NetworkInterfaceID)
			},
		},
		{
			name: "post_actions_ordered",
			header: []string{
				"mgn:server:hostname",
				"mgn:launch:post-actions:second:ssmDocumentName", "mgn:launch:post-actions:second:order",
				"mgn:launch:post-actions:second:timeoutSeconds", "mgn:launch:post-actions:second:mustSucceedForCutover",
				"mgn:launch:post-actions:second:parameters",
				"mgn:launch:post-actions:first:ssmDocumentName", "mgn:launch:post-actions:first:order",
			},
			row: []string{
				"l2.example.com", "DocB", "2", "90", "true",
				`{"parameters":{"Region":[{"value":"us-east-1"}]},"externalParameters":{"InstanceId":"$.instanceId"}}`,
				"DocA", "1",
			},
			check: func(t *testing.T, _ *mgn.LaunchConfiguration, out *mgnsdk.GetLaunchConfigurationOutput) {
				t.Helper()

				require.NotNil(t, out.PostLaunchActions)
				docs := out.PostLaunchActions.SsmDocuments
				require.Len(t, docs, 2)
				assert.Equal(t, "first", aws.ToString(docs[0].ActionName))
				assert.Equal(t, "DocA", aws.ToString(docs[0].SsmDocumentName))
				assert.Equal(t, "second", aws.ToString(docs[1].ActionName))
				assert.EqualValues(t, 90, aws.ToInt32(docs[1].TimeoutSeconds))
				assert.True(t, aws.ToBool(docs[1].MustSucceedForCutover))
				require.Contains(t, docs[1].ExternalParameters, "InstanceId")
			},
		},
		{
			name:       "bad_tenancy",
			header:     []string{"mgn:server:hostname", "mgn:launch:placement:tenancy"},
			row:        []string{"l3.example.com", "Dedicated"},
			wantErrors: 1,
		},
		{
			name:       "bad_volume_type",
			header:     []string{"mgn:server:hostname", "mgn:launch:volume:/dev/sda1:type"},
			row:        []string{"l4.example.com", "fast"},
			wantErrors: 1,
		},
		{
			name:       "bad_map_tag_key",
			header:     []string{"mgn:server:hostname", "mgn:launch:map-tag-key"},
			row:        []string{"l5.example.com", "Map-Migrated"},
			wantErrors: 1,
		},
		{
			name:       "action_without_document",
			header:     []string{"mgn:server:hostname", "mgn:launch:post-actions:a:order"},
			row:        []string{"l6.example.com", "1"},
			wantErrors: 1,
		},
		{
			name: "bad_action_order",
			header: []string{
				"mgn:server:hostname", "mgn:launch:post-actions:a:ssmDocumentName", "mgn:launch:post-actions:a:order",
			},
			row:        []string{"l7.example.com", "Doc", "0"},
			wantErrors: 1,
		},
		{
			name: "bad_parameters_json",
			header: []string{
				"mgn:server:hostname", "mgn:launch:post-actions:a:ssmDocumentName",
				"mgn:launch:post-actions:a:parameters",
			},
			row:        []string{"l8.example.com", "Doc", `{"other":1}`},
			wantErrors: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, client := newTestHandlerAndClient(t)
			s3 := newMockS3()
			h.Backend.SetS3Backend(s3)

			var buf bytes.Buffer

			w := csv.NewWriter(&buf)
			require.NoError(t, w.Write(tt.header))
			require.NoError(t, w.Write(tt.row))
			w.Flush()
			require.NoError(t, w.Error())

			final := runImportAndWait(t, client, "launch-bucket", buf.String(), s3)
			require.Equal(t, types.ImportStatusSucceeded, final.Status)

			errs, err := client.ListImportErrors(t.Context(), &mgnsdk.ListImportErrorsInput{ImportID: final.ImportID})
			require.NoError(t, err)
			require.Len(t, errs.Items, tt.wantErrors)

			if tt.check == nil {
				return
			}

			servers, err := client.DescribeSourceServers(t.Context(), &mgnsdk.DescribeSourceServersInput{})
			require.NoError(t, err)
			require.Len(t, servers.Items, 1)

			id := servers.Items[0].SourceServerID
			out, err := client.GetLaunchConfiguration(
				t.Context(),
				&mgnsdk.GetLaunchConfigurationInput{SourceServerID: id},
			)
			require.NoError(t, err)

			lc, err := h.Backend.GetLaunchConfiguration(aws.ToString(id))
			require.NoError(t, err)

			tt.check(t, lc, out)
		})
	}
}
