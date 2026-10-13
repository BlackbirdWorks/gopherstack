package servicediscovery_test

import (
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

type fakeRecordSets struct {
	sets map[string][]string
	mu   sync.Mutex
}

func (f *fakeRecordSets) PutRecordSet(zoneID, name, typ string, ttl int64, values []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.sets[zoneID+"|"+name+"|"+typ] = append([]string{"ttl=" + strconv.FormatInt(ttl, 10)}, values...)

	return nil
}

func (f *fakeRecordSets) DeleteRecordSet(zoneID, name, typ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.sets, zoneID+"|"+name+"|"+typ)

	return nil
}

func TestRecordSetWriter_FollowsInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		records []servicediscovery.DNSRecord
		wantKey bool
	}{
		{name: "a record configured", records: []servicediscovery.DNSRecord{{Type: "A", TTL: 30}}, wantKey: true},
		{name: "no records configured"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := servicediscovery.NewInMemoryBackend("123456789012", "us-east-1")
			w := &fakeRecordSets{sets: map[string][]string{}}
			b.SetRecordSetWriter(w)

			_, err := b.CreatePrivateDNSNamespace("ns.local", "", "vpc-1", 0, nil)
			require.NoError(t, err)

			nss := b.ListNamespaces(servicediscovery.ListNamespacesFilter{})
			require.Len(t, nss, 1)

			nsID := nss[0].ID
			zone := nss[0].Properties.DNSProperties.HostedZoneID

			dnsCfg := &servicediscovery.DNSConfig{DNSRecords: tt.records}
			svc, err := b.CreateService("web", nsID, "", "", dnsCfg, nil, nil, nil)
			require.NoError(t, err)

			_, err = b.RegisterInstance(svc.ID, "i-1", map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.2"})
			require.NoError(t, err)
			_, err = b.RegisterInstance(svc.ID, "i-2", map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"})
			require.NoError(t, err)

			key := zone + "|web.ns.local|A"

			if !tt.wantKey {
				assert.Empty(t, w.sets)

				return
			}

			got := append([]string(nil), w.sets[key]...)
			sort.Strings(got[1:])
			assert.Equal(t, []string{"ttl=30", "10.0.0.1", "10.0.0.2"}, got)

			_, err = b.DeregisterInstance(svc.ID, "i-1")
			require.NoError(t, err)
			assert.Equal(t, []string{"ttl=30", "10.0.0.1"}, w.sets[key])

			_, err = b.DeregisterInstance(svc.ID, "i-2")
			require.NoError(t, err)
			assert.NotContains(t, w.sets, key)
		})
	}
}
