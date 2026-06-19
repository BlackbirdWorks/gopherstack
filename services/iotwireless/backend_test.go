package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func TestInMemoryBackend_WirelessDeviceCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		deviceName  string
		devType     string
		destination string
		description string
		wantErr     bool
	}{
		{
			name:        "create_and_get",
			deviceName:  "device-1",
			devType:     "LoRaWAN",
			destination: "dest-1",
			description: "test device",
		},
		{
			name:    "get_nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			if tt.wantErr {
				_, err := bk.GetWirelessDevice(testAccountID, testRegion, "no-such-id")
				require.Error(t, err)

				return
			}

			d, err := bk.CreateWirelessDevice(
				testAccountID, testRegion,
				tt.deviceName, tt.devType, tt.destination, tt.description,
				map[string]string{"env": "test"},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.deviceName, d.Name)
			assert.Equal(t, tt.devType, d.Type)
			assert.NotEmpty(t, d.ID)
			assert.NotEmpty(t, d.ARN)
			assert.Equal(t, "test", d.Tags["env"])

			got, err := bk.GetWirelessDevice(testAccountID, testRegion, d.ID)
			require.NoError(t, err)
			assert.Equal(t, d.ID, got.ID)
			assert.Equal(t, tt.deviceName, got.Name)

			err = bk.DeleteWirelessDevice(testAccountID, testRegion, d.ID)
			require.NoError(t, err)

			_, err = bk.GetWirelessDevice(testAccountID, testRegion, d.ID)
			require.Error(t, err)
		})
	}
}

func TestInMemoryBackend_WirelessDevice_DeleteNotFound(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()
	err := bk.DeleteWirelessDevice(testAccountID, testRegion, "no-such-id")
	require.Error(t, err)
	assert.ErrorIs(t, err, iotwireless.ErrDeviceNotFound)
}

func TestInMemoryBackend_ListWirelessDevices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		deviceNames []string
		wantCount   int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name:        "multiple",
			deviceNames: []string{"d1", "d2", "d3"},
			wantCount:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			for _, name := range tt.deviceNames {
				_, err := bk.CreateWirelessDevice(testAccountID, testRegion, name, "LoRaWAN", "", "", nil)
				require.NoError(t, err)
			}

			devices := bk.ListWirelessDevices(testAccountID, testRegion)
			assert.Len(t, devices, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_WirelessGatewayCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		gwName      string
		description string
		wantErr     bool
	}{
		{
			name:        "create_and_get",
			gwName:      "gw-1",
			description: "test gateway",
		},
		{
			name:    "get_nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			if tt.wantErr {
				_, err := bk.GetWirelessGateway(testAccountID, testRegion, "no-such-id")
				require.Error(t, err)

				return
			}

			gw, err := bk.CreateWirelessGateway(
				testAccountID, testRegion,
				tt.gwName, tt.description,
				map[string]string{"team": "infra"},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.gwName, gw.Name)
			assert.NotEmpty(t, gw.ID)
			assert.NotEmpty(t, gw.ARN)
			assert.Equal(t, "infra", gw.Tags["team"])

			got, err := bk.GetWirelessGateway(testAccountID, testRegion, gw.ID)
			require.NoError(t, err)
			assert.Equal(t, gw.ID, got.ID)

			err = bk.DeleteWirelessGateway(testAccountID, testRegion, gw.ID)
			require.NoError(t, err)

			_, err = bk.GetWirelessGateway(testAccountID, testRegion, gw.ID)
			require.Error(t, err)
		})
	}
}

func TestInMemoryBackend_WirelessGateway_DeleteNotFound(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()
	err := bk.DeleteWirelessGateway(testAccountID, testRegion, "no-such-id")
	require.Error(t, err)
	assert.ErrorIs(t, err, iotwireless.ErrGatewayNotFound)
}

func TestInMemoryBackend_ListWirelessGateways(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		gwNames   []string
		wantCount int
	}{
		{name: "empty", wantCount: 0},
		{name: "two", gwNames: []string{"gw-a", "gw-b"}, wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			for _, name := range tt.gwNames {
				_, err := bk.CreateWirelessGateway(testAccountID, testRegion, name, "", nil)
				require.NoError(t, err)
			}

			gws := bk.ListWirelessGateways(testAccountID, testRegion)
			assert.Len(t, gws, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_ServiceProfileCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileName string
		wantErr     bool
	}{
		{
			name:        "create_and_get",
			profileName: "profile-1",
		},
		{
			name:    "get_nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			if tt.wantErr {
				_, err := bk.GetServiceProfile(testAccountID, testRegion, "no-such-id")
				require.Error(t, err)

				return
			}

			sp, err := bk.CreateServiceProfile(
				testAccountID,
				testRegion,
				tt.profileName,
				map[string]string{"tier": "standard"},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.profileName, sp.Name)
			assert.NotEmpty(t, sp.ID)
			assert.NotEmpty(t, sp.ARN)
			assert.Equal(t, "standard", sp.Tags["tier"])

			got, err := bk.GetServiceProfile(testAccountID, testRegion, sp.ID)
			require.NoError(t, err)
			assert.Equal(t, sp.ID, got.ID)

			err = bk.DeleteServiceProfile(testAccountID, testRegion, sp.ID)
			require.NoError(t, err)

			_, err = bk.GetServiceProfile(testAccountID, testRegion, sp.ID)
			require.Error(t, err)
		})
	}
}

func TestInMemoryBackend_ServiceProfile_DeleteNotFound(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()
	err := bk.DeleteServiceProfile(testAccountID, testRegion, "no-such-id")
	require.Error(t, err)
	assert.ErrorIs(t, err, iotwireless.ErrServiceProfileNotFound)
}

func TestInMemoryBackend_ListServiceProfiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		profileNames []string
		wantCount    int
	}{
		{name: "empty", wantCount: 0},
		{name: "two", profileNames: []string{"sp-1", "sp-2"}, wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			for _, name := range tt.profileNames {
				_, err := bk.CreateServiceProfile(testAccountID, testRegion, name, nil)
				require.NoError(t, err)
			}

			profiles := bk.ListServiceProfiles(testAccountID, testRegion)
			assert.Len(t, profiles, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_DestinationCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		destName    string
		expression  string
		exprType    string
		roleArn     string
		description string
		wantErr     bool
	}{
		{
			name:        "create_and_get",
			destName:    "dest-1",
			expression:  "arn:aws:iot:us-east-1:000000000000:rule/my-rule",
			exprType:    "RuleName",
			roleArn:     "arn:aws:iam::000000000000:role/my-role",
			description: "test destination",
		},
		{
			name:    "get_nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			if tt.wantErr {
				_, err := bk.GetDestination(testAccountID, testRegion, "no-such-name")
				require.Error(t, err)

				return
			}

			dest, err := bk.CreateDestination(
				testAccountID, testRegion,
				tt.destName, tt.expression, tt.exprType, tt.roleArn, tt.description,
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.destName, dest.Name)
			assert.NotEmpty(t, dest.ARN)
			assert.Equal(t, tt.expression, dest.Expression)

			got, err := bk.GetDestination(testAccountID, testRegion, tt.destName)
			require.NoError(t, err)
			assert.Equal(t, dest.Name, got.Name)

			err = bk.DeleteDestination(testAccountID, testRegion, tt.destName)
			require.NoError(t, err)

			_, err = bk.GetDestination(testAccountID, testRegion, tt.destName)
			require.Error(t, err)
		})
	}
}

func TestInMemoryBackend_Destination_DeleteNotFound(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()
	err := bk.DeleteDestination(testAccountID, testRegion, "no-such-name")
	require.Error(t, err)
	assert.ErrorIs(t, err, iotwireless.ErrDestinationNotFound)
}

func TestInMemoryBackend_ListDestinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		destNames []string
		wantCount int
	}{
		{name: "empty", wantCount: 0},
		{name: "two", destNames: []string{"dest-a", "dest-b"}, wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			for _, name := range tt.destNames {
				_, err := bk.CreateDestination(testAccountID, testRegion, name, "", "", "", "", nil)
				require.NoError(t, err)
			}

			dests := bk.ListDestinations(testAccountID, testRegion)
			assert.Len(t, dests, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupTags  map[string]string
		addTags    map[string]string
		wantTags   map[string]string
		name       string
		removeTags []string
	}{
		{
			name:      "add_tags",
			setupTags: nil,
			addTags:   map[string]string{"env": "prod", "team": "platform"},
			wantTags:  map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name:       "remove_tags",
			setupTags:  map[string]string{"env": "prod", "team": "platform"},
			removeTags: []string{"team"},
			wantTags:   map[string]string{"env": "prod"},
		},
		{
			name:      "list_tags_empty_arn",
			setupTags: nil,
			addTags:   nil,
			wantTags:  map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			sp, err := bk.CreateServiceProfile(testAccountID, testRegion, "sp-tag-test", tt.setupTags)
			require.NoError(t, err)

			if tt.addTags != nil {
				err = bk.TagResource(sp.ARN, tt.addTags)
				require.NoError(t, err)
			}

			if tt.removeTags != nil {
				err = bk.UntagResource(sp.ARN, tt.removeTags)
				require.NoError(t, err)
			}

			tags, err := bk.ListTagsForResource(sp.ARN)
			require.NoError(t, err)

			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestInMemoryBackend_ListTagsForResource_UnknownARN(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()

	tags, err := bk.ListTagsForResource("arn:aws:iotwireless:us-east-1:000000000000:ServiceProfile/unknown")
	require.NoError(t, err)
	assert.Empty(t, tags)
}

func TestInMemoryBackend_UntagResource_CleansEmptyMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupTags    map[string]string
		removeTags   []string
		wantTagsLeft int
	}{
		{
			name:         "removing_all_tags_cleans_map",
			setupTags:    map[string]string{"env": "prod", "team": "platform"},
			removeTags:   []string{"env", "team"},
			wantTagsLeft: 0,
		},
		{
			name:         "removing_some_tags_leaves_rest",
			setupTags:    map[string]string{"env": "prod", "team": "platform"},
			removeTags:   []string{"team"},
			wantTagsLeft: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			sp, err := bk.CreateServiceProfile(testAccountID, testRegion, "sp-cleanup", tt.setupTags)
			require.NoError(t, err)

			err = bk.UntagResource(sp.ARN, tt.removeTags)
			require.NoError(t, err)

			tags, err := bk.ListTagsForResource(sp.ARN)
			require.NoError(t, err)
			assert.Len(t, tags, tt.wantTagsLeft)
		})
	}
}

func TestInMemoryBackend_GetReturnsIsolatedCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "wireless_device"},
		{name: "wireless_gateway"},
		{name: "service_profile"},
		{name: "destination"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			switch tt.name {
			case "wireless_device":
				d, err := bk.CreateWirelessDevice(
					testAccountID,
					testRegion,
					"dev",
					"LoRaWAN",
					"",
					"",
					map[string]string{"k": "v"},
				)
				require.NoError(t, err)

				d.Tags["injected"] = "yes"

				got, err := bk.GetWirelessDevice(testAccountID, testRegion, d.ID)
				require.NoError(t, err)
				assert.NotContains(t, got.Tags, "injected", "mutation of returned pointer must not affect backend")

			case "wireless_gateway":
				gw, err := bk.CreateWirelessGateway(testAccountID, testRegion, "gw", "", map[string]string{"k": "v"})
				require.NoError(t, err)

				gw.Tags["injected"] = "yes"

				got, err := bk.GetWirelessGateway(testAccountID, testRegion, gw.ID)
				require.NoError(t, err)
				assert.NotContains(t, got.Tags, "injected", "mutation of returned pointer must not affect backend")

			case "service_profile":
				sp, err := bk.CreateServiceProfile(testAccountID, testRegion, "sp", map[string]string{"k": "v"})
				require.NoError(t, err)

				sp.Tags["injected"] = "yes"

				got, err := bk.GetServiceProfile(testAccountID, testRegion, sp.ID)
				require.NoError(t, err)
				assert.NotContains(t, got.Tags, "injected", "mutation of returned pointer must not affect backend")

			case "destination":
				dest, err := bk.CreateDestination(
					testAccountID,
					testRegion,
					"dest",
					"",
					"",
					"",
					"",
					map[string]string{"k": "v"},
				)
				require.NoError(t, err)

				dest.Tags["injected"] = "yes"

				got, err := bk.GetDestination(testAccountID, testRegion, dest.Name)
				require.NoError(t, err)
				assert.NotContains(t, got.Tags, "injected", "mutation of returned pointer must not affect backend")
			}
		})
	}
}

func TestInMemoryBackend_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()

	// Create one of each resource type.
	dev, err := bk.CreateWirelessDevice(
		testAccountID,
		testRegion,
		"dev-snap",
		"LoRaWAN",
		"dest-snap",
		"desc",
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	gw, err := bk.CreateWirelessGateway(
		testAccountID,
		testRegion,
		"gw-snap",
		"gateway",
		map[string]string{"tier": "free"},
	)
	require.NoError(t, err)

	sp, err := bk.CreateServiceProfile(testAccountID, testRegion, "sp-snap", map[string]string{"role": "iot"})
	require.NoError(t, err)

	dest, err := bk.CreateDestination(
		testAccountID,
		testRegion,
		"dest-snap",
		"rule",
		"RuleName",
		"arn:role",
		"desc",
		nil,
	)
	require.NoError(t, err)

	// Snapshot.
	snap := bk.Snapshot()
	require.NotNil(t, snap)

	// Restore into a fresh backend.
	bk2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, bk2.Restore(snap))

	// Verify all resources are present with correct fields.
	gotDev, err := bk2.GetWirelessDevice(testAccountID, testRegion, dev.ID)
	require.NoError(t, err)
	assert.Equal(t, dev.Name, gotDev.Name)
	assert.Equal(t, "test", gotDev.Tags["env"])

	gotGW, err := bk2.GetWirelessGateway(testAccountID, testRegion, gw.ID)
	require.NoError(t, err)
	assert.Equal(t, gw.Name, gotGW.Name)

	gotSP, err := bk2.GetServiceProfile(testAccountID, testRegion, sp.ID)
	require.NoError(t, err)
	assert.Equal(t, sp.Name, gotSP.Name)

	gotDest, err := bk2.GetDestination(testAccountID, testRegion, dest.Name)
	require.NoError(t, err)
	assert.Equal(t, dest.Expression, gotDest.Expression)

	// Resource tags are preserved.
	tags, err := bk2.ListTagsForResource(dev.ARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

func TestCreateWirelessGatewayTaskDefinition_ARNUsesRegionAndAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
		wantARN   string
	}{
		{
			name:      "default region and account",
			accountID: "000000000000",
			region:    "us-east-1",
			wantARN:   "arn:aws:iotwireless:us-east-1:000000000000:WirelessGatewayTaskDefinition/",
		},
		{
			name:      "eu-west-1 cross-region",
			accountID: "111122223333",
			region:    "eu-west-1",
			wantARN:   "arn:aws:iotwireless:eu-west-1:111122223333:WirelessGatewayTaskDefinition/",
		},
		{
			name:      "ap-southeast-2 cross-region",
			accountID: "999988887777",
			region:    "ap-southeast-2",
			wantARN:   "arn:aws:iotwireless:ap-southeast-2:999988887777:WirelessGatewayTaskDefinition/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotwireless.NewInMemoryBackend()
			def, err := b.CreateWirelessGatewayTaskDefinition(tt.accountID, tt.region, "taskdef", false)
			require.NoError(t, err)
			assert.Greater(t, len(def.ARN), len(tt.wantARN), "ARN too short")
			assert.Equal(t, tt.wantARN, def.ARN[:len(tt.wantARN)],
				"ARN %q does not start with %q", def.ARN, tt.wantARN)
		})
	}
}
