package dms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
)

func seedMigrationProject(t *testing.T, b *dms.InMemoryBackend, name string) {
	t.Helper()

	ctx := t.Context()

	ip, err := b.CreateInstanceProfile(ctx, name+"-ip", "", "", "", "", "", false, nil, nil)
	require.NoError(t, err)

	dp, err := b.CreateDataProvider(ctx, dms.CreateDataProviderParams{Name: name + "-dp", Engine: "mysql"})
	require.NoError(t, err)

	descs := []dms.DataProviderDescriptorInput{{DataProviderIdentifier: dp.DataProviderName}}
	_, err = b.CreateMigrationProject(ctx, dms.CreateMigrationProjectParams{
		Name:                      name,
		InstanceProfileIdentifier: ip.InstanceProfileName,
		SourceDescriptors:         descs,
		TargetDescriptors:         descs,
	})
	require.NoError(t, err)
}

func seedMigrationProjectViaClient(t *testing.T, client *dmssdk.Client, name string) {
	t.Helper()

	_, err := client.CreateInstanceProfile(t.Context(), &dmssdk.CreateInstanceProfileInput{
		InstanceProfileName: aws.String(name + "-ip"),
	})
	require.NoError(t, err)

	_, err = client.CreateDataProvider(t.Context(), &dmssdk.CreateDataProviderInput{
		DataProviderName: aws.String(name + "-dp"),
		Engine:           aws.String("mysql"),
		Settings: &types.DataProviderSettingsMemberMySqlSettings{
			Value: types.MySqlDataProviderSettings{Port: aws.Int32(3306)},
		},
	})
	require.NoError(t, err)

	descs := []types.DataProviderDescriptorDefinition{{DataProviderIdentifier: aws.String(name + "-dp")}}
	_, err = client.CreateMigrationProject(t.Context(), &dmssdk.CreateMigrationProjectInput{
		MigrationProjectName:          aws.String(name),
		InstanceProfileIdentifier:     aws.String(name + "-ip"),
		SourceDataProviderDescriptors: descs,
		TargetDataProviderDescriptors: descs,
	})
	require.NoError(t, err)
}
