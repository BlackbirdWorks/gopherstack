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
