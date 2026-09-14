package appstream_test

import (
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFleetApplicationStackAppBlock_NoInventedTagsKey decodes the raw
// rpc-v2-cbor response body (not the typed SDK client, which would silently
// swallow an unrecognized key) for Create* on Fleet, Application, Stack, and
// AppBlock and asserts the top-level key set contains no "Tags" key. Real
// deserializeCBOR_Fleet/_Application/_Stack/_AppBlock (appstream@v1.64.5
// deserializers.go) declare no such member -- gopherstack previously emitted
// one anyway on all four (gopherstack-gv10n).
func TestFleetApplicationStackAppBlock_NoInventedTagsKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        cbor.Map
		op          string
		respKey     string
		name        string
		allowedKeys []string
	}{
		{
			name:    "fleet",
			op:      "CreateFleet",
			respKey: "Fleet",
			body: cbor.Map{
				"Name":         cbor.String("tagkey-fleet"),
				"InstanceType": cbor.String("stream.standard.medium"),
				"Tags":         cbor.Map{"env": cbor.String("test")},
			},
			allowedKeys: []string{
				"Arn", "Name", "DisplayName", "Description", "ImageName", "ImageArn",
				"InstanceType", "FleetType", "ComputeCapacityStatus", "MaxUserDurationInSeconds",
				"DisconnectTimeoutInSeconds", "State", "VpcConfig", "CreatedTime", "FleetErrors",
				"EnableDefaultInternetAccess", "DomainJoinInfo", "IdleDisconnectTimeoutInSeconds",
				"IamRoleArn", "StreamView", "Platform", "MaxConcurrentSessions",
				"UsbDeviceFilterStrings", "SessionScriptS3Location", "MaxSessionsPerInstance",
				"RootVolumeConfig", "DisableIMDSV1",
			},
		},
		{
			name:    "application",
			op:      "CreateApplication",
			respKey: "Application",
			body: cbor.Map{
				"Name":             cbor.String("tagkey-app"),
				"LaunchPath":       cbor.String("/app"),
				"Platforms":        cbor.List{cbor.String("WINDOWS")},
				"InstanceFamilies": cbor.List{cbor.String("GENERAL_PURPOSE")},
				"IconS3Location": cbor.Map{
					"S3Bucket": cbor.String("icon-bucket"),
					"S3Key":    cbor.String("icon.png"),
				},
				"Tags": cbor.Map{"env": cbor.String("test")},
			},
			allowedKeys: []string{
				"Name", "DisplayName", "IconURL", "LaunchPath", "LaunchParameters", "Enabled",
				"Metadata", "WorkingDirectory", "Description", "Arn", "AppBlockArn",
				"IconS3Location", "Platforms", "InstanceFamilies", "CreatedTime",
			},
		},
		{
			name:    "stack",
			op:      "CreateStack",
			respKey: "Stack",
			body: cbor.Map{
				"Name": cbor.String("tagkey-stack"),
				"Tags": cbor.Map{"env": cbor.String("test")},
			},
			allowedKeys: []string{
				"Arn", "Name", "Description", "DisplayName", "CreatedTime", "StorageConnectors",
				"RedirectURL", "FeedbackURL", "StackErrors", "UserSettings", "ApplicationSettings",
				"AccessEndpoints", "EmbedHostDomains", "StreamingExperienceSettings",
				"ContentRedirection", "AgentAccessConfig",
			},
		},
		{
			name:    "appblock",
			op:      "CreateAppBlock",
			respKey: "AppBlock",
			body: cbor.Map{
				"Name": cbor.String("tagkey-appblock"),
				"SourceS3Location": cbor.Map{
					"S3Bucket": cbor.String("appblock-bucket"),
					"S3Key":    cbor.String("tagkey-appblock.zip"),
				},
				"Tags": cbor.Map{"env": cbor.String("test")},
			},
			allowedKeys: []string{
				"Name", "Arn", "Description", "DisplayName", "SourceS3Location",
				"SetupScriptDetails", "CreatedTime", "PostSetupScriptDetails", "PackagingType",
				"State", "AppBlockErrors",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := postCBOR(t, h, tc.op, tc.body)
			require.Equal(t, 200, rec.Code, rec.Body.String())

			resp := decodeCBORResponse(t, rec)
			resource, ok := resp[tc.respKey].(cbor.Map)
			require.True(t, ok, "expected %q key in response", tc.respKey)

			_, hasTags := resource["Tags"]
			assert.False(t, hasTags, "%s response must not emit an invented Tags key", tc.name)

			allowed := make(map[string]bool, len(tc.allowedKeys))
			for _, k := range tc.allowedKeys {
				allowed[k] = true
			}

			for k := range resource {
				assert.True(t, allowed[k],
					"%s response emitted key %q not in the real deserializer's member set", tc.name, k)
			}
		})
	}
}
