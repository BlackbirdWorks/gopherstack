package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/mysql/armmysqlflexibleservers/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLFlexBackups(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.myCreate(t, "bk-my")

	f := s.myFactory(t)
	bc := f.NewBackupsClient()

	var names []string

	for p := bc.NewListByServerPager(flexRG, "bk-my", nil); p.More(); {
		page, err := p.NextPage(t.Context())
		require.NoError(t, err)

		for _, b := range page.Value {
			names = append(names, *b.Name)
			assert.Equal(t, "Automatic", *b.Properties.Source)
			assert.Equal(t, "FULL", *b.Properties.BackupType)
		}
	}

	require.Len(t, names, 1, "the first automatic backup exists after create")

	put, err := bc.Put(t.Context(), flexRG, "bk-my", "manual1", nil)
	require.NoError(t, err)
	assert.Equal(t, "manual1", *put.Name)
	assert.Equal(t, "Full", *put.Properties.Source)

	got, err := bc.Get(t.Context(), flexRG, "bk-my", "manual1", nil)
	require.NoError(t, err)
	assert.Equal(t, *put.ID, *got.ID)

	_, err = bc.Put(t.Context(), flexRG, "bk-my", "manual1", nil)
	require.Error(t, err, "duplicate backup name")

	_, err = bc.Put(t.Context(), flexRG, "bk-my", "bad name!", nil)
	require.Error(t, err)

	v2 := 0

	for p := f.NewLongRunningBackupsClient().NewListPager(flexRG, "bk-my", nil); p.More(); {
		page, perr := p.NextPage(t.Context())
		require.NoError(t, perr)

		for _, b := range page.Value {
			v2++
			assert.Equal(t, armmysqlflexibleservers.BackupTypeFULL, *b.Properties.BackupType)
			assert.Equal(t, *b.Name, *b.Properties.BackupNameV2)
		}
	}

	assert.Equal(t, 2, v2)

	_, err = bc.Get(t.Context(), flexRG, "bk-my", "missing", nil)
	require.Error(t, err)
}

func TestPGFlexBackups(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.pgCreate(t, "bk-pg")

	pf, err := armpostgresqlflexibleservers.NewClientFactory(aciSub, aciCred{}, s.opts)
	require.NoError(t, err)

	n := 0

	for p := pf.NewBackupsClient().NewListByServerPager(flexRG, "bk-pg", nil); p.More(); {
		page, perr := p.NextPage(t.Context())
		require.NoError(t, perr)

		n += len(page.Value)
	}

	assert.Equal(t, 1, n)
}

func TestMySQLFlexBatchConfigurations(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.myCreate(t, "bc-my")

	cc := s.myFactory(t).NewConfigurationsClient()

	batch := func(vals map[string]string, reset bool) error {
		req := armmysqlflexibleservers.ConfigurationListForBatchUpdate{}

		for name, v := range vals {
			req.Value = append(req.Value, &armmysqlflexibleservers.ConfigurationForBatchUpdate{
				Name:       new(name),
				Properties: &armmysqlflexibleservers.ConfigurationForBatchUpdateProperties{Value: new(v)},
			})
		}

		if reset {
			req.ResetAllToDefault = new(armmysqlflexibleservers.ResetAllToDefaultTrue)
		}

		p, err := cc.BeginBatchUpdate(t.Context(), flexRG, "bc-my", req, nil)
		if err != nil {
			return err
		}

		_, err = p.PollUntilDone(t.Context(), pollFast())

		return err
	}

	require.NoError(t, batch(map[string]string{"wait_timeout": "100", "max_allowed_packet": "2097152"}, false))

	got, err := cc.Get(t.Context(), flexRG, "bc-my", "wait_timeout", nil)
	require.NoError(t, err)
	assert.Equal(t, "100", *got.Properties.Value)

	require.Error(t, batch(map[string]string{"wait_timeout": "200", "no_such_param": "1"}, false))

	got, err = cc.Get(t.Context(), flexRG, "bc-my", "wait_timeout", nil)
	require.NoError(t, err)
	assert.Equal(t, "100", *got.Properties.Value, "a failed batch changes nothing")

	require.NoError(t, batch(nil, true))

	got, err = cc.Get(t.Context(), flexRG, "bc-my", "wait_timeout", nil)
	require.NoError(t, err)
	assert.NotEqual(t, "100", *got.Properties.Value, "reset all returns the default")
}
