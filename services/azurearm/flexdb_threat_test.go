package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/mysql/armmysqlflexibleservers/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLAdvancedThreatProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		set    armmysqlflexibleservers.AdvancedThreatProtectionState
		viaPut bool
	}{
		{name: "patch enable", set: armmysqlflexibleservers.AdvancedThreatProtectionStateEnabled},
		{name: "put enable", set: armmysqlflexibleservers.AdvancedThreatProtectionStateEnabled, viaPut: true},
		{name: "patch disable", set: armmysqlflexibleservers.AdvancedThreatProtectionStateDisabled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newFlexStack(t, nil)
			s.myCreate(t, "my-atp")

			c := s.myFactory(t).NewAdvancedThreatProtectionSettingsClient()
			def := armmysqlflexibleservers.AdvancedThreatProtectionNameDefault

			got, err := c.Get(t.Context(), flexRG, "my-atp", def, nil)
			require.NoError(t, err)
			assert.Equal(t, armmysqlflexibleservers.AdvancedThreatProtectionStateDisabled, *got.Properties.State)

			if tt.viaPut {
				poller, perr := c.BeginUpdatePut(t.Context(), flexRG, "my-atp", def,
					armmysqlflexibleservers.AdvancedThreatProtection{
						Properties: &armmysqlflexibleservers.AdvancedThreatProtectionProperties{State: &tt.set},
					}, nil)
				require.NoError(t, perr)

				_, err = poller.PollUntilDone(t.Context(), pollFast())
			} else {
				poller, perr := c.BeginUpdate(t.Context(), flexRG, "my-atp", def,
					armmysqlflexibleservers.AdvancedThreatProtectionForUpdate{
						Properties: &armmysqlflexibleservers.AdvancedThreatProtectionUpdateProperties{State: &tt.set},
					}, nil)
				require.NoError(t, perr)

				_, err = poller.PollUntilDone(t.Context(), pollFast())
			}

			require.NoError(t, err)

			got, err = c.Get(t.Context(), flexRG, "my-atp", def, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.set, *got.Properties.State)

			page, err := c.NewListPager(flexRG, "my-atp", nil).NextPage(t.Context())
			require.NoError(t, err)
			require.Len(t, page.Value, 1)
			assert.Equal(t, tt.set, *page.Value[0].Properties.State)
		})
	}
}

func TestMySQLAdvancedThreatProtectionRefusals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, method, suffix, body string
		want                       int
	}{
		{name: "bad state", method: http.MethodPatch, suffix: "/advancedThreatProtectionSettings/Default",
			body: `{"properties":{"state":"Maybe"}}`, want: http.StatusBadRequest},
		{name: "bad name", method: http.MethodPatch, suffix: "/advancedThreatProtectionSettings/Other",
			body: `{"properties":{"state":"Enabled"}}`, want: http.StatusBadRequest},
		{name: "delete", method: http.MethodDelete, suffix: "/advancedThreatProtectionSettings/Default",
			want: http.StatusBadRequest},
		{name: "unknown server", method: http.MethodGet, suffix: "x/advancedThreatProtectionSettings/Default",
			want: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newFlexStack(t, nil)
			s.myCreate(t, "my-atp2")

			status, resp := s.raw(t, tt.method, flexServerPath(myNS, "my-atp2")+tt.suffix, tt.body, flexAPIMy)
			assert.Equal(t, tt.want, status, resp)
		})
	}
}

func TestMySQLDetachVNet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		access  armmysqlflexibleservers.EnableStatusEnum
		private bool
		wantErr bool
	}{
		{name: "private to public", private: true, access: armmysqlflexibleservers.EnableStatusEnumEnabled},
		{name: "private to disabled", private: true, access: armmysqlflexibleservers.EnableStatusEnumDisabled},
		{
			name:    "not integrated",
			private: false,
			access:  armmysqlflexibleservers.EnableStatusEnumEnabled,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newFlexStack(t, nil)
			path := flexServerPath(myNS, "my-detach")

			if tt.private {
				vnet := flexVNetFixture(t, s)
				status, resp := s.raw(t, http.MethodPut, path,
					flexPrivateBody(vnet, "mysub", "privateDnsZoneResourceId", flexZone("mysql"), ""), flexAPIMy)
				require.Equal(t, http.StatusAccepted, status, resp)
			} else {
				s.myCreate(t, "my-detach")
			}

			poller, err := s.myFactory(t).NewServersClient().BeginDetachVNet(t.Context(), flexRG, "my-detach",
				armmysqlflexibleservers.ServerDetachVNetParameter{PublicNetworkAccess: &tt.access}, nil)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			_, err = poller.PollUntilDone(t.Context(), pollFast())
			require.NoError(t, err)

			status, resp := s.raw(t, http.MethodGet, path, "", flexAPIMy)
			require.Equal(t, http.StatusOK, status, resp)

			props, _ := resp["properties"].(map[string]any)
			net, _ := props["network"].(map[string]any)

			assert.Equal(t, string(tt.access), net["publicNetworkAccess"])
			assert.Empty(t, net["delegatedSubnetResourceId"])
		})
	}
}

func TestMySQLAdvancedThreatProtectionPersistence(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.myCreate(t, "my-atp3")

	path := flexServerPath(myNS, "my-atp3") + "/advancedThreatProtectionSettings/Default"
	status, resp := s.raw(t, http.MethodPatch, path, `{"properties":{"state":"Enabled"}}`, flexAPIMy)
	require.Equal(t, http.StatusOK, status, resp)

	s2 := newFlexStack(t, nil)
	require.NoError(t, s2.handler.Restore(t.Context(), s.handler.Snapshot(t.Context())))

	status, resp = s2.raw(t, http.MethodGet, path, "", flexAPIMy)
	require.Equal(t, http.StatusOK, status, resp)

	props, _ := resp["properties"].(map[string]any)
	assert.Equal(t, "Enabled", props["state"])
}
