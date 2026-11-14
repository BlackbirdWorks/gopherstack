package ocidns_test

import (
	"sync"
	"testing"

	ocidnssdk "github.com/oracle/oci-go-sdk/v65/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentCreateZoneRetryToken(t *testing.T) {
	t.Parallel()

	const workers = 16

	e := newEnv(t, envOpts{})
	ids := make([]string, workers)
	errs := make([]error, workers)
	start := make(chan struct{})

	var wg sync.WaitGroup

	for i := range workers {
		wg.Go(func() {
			<-start

			r, err := e.client.CreateZone(t.Context(), ocidnssdk.CreateZoneRequest{
				OpcRetryToken: new("race"),
				CreateZoneDetails: ocidnssdk.CreateZoneDetails{
					Name: new("race.example.com"), CompartmentId: new(testCompartment),
				},
			})
			if err == nil {
				ids[i] = *r.Id
			}

			errs[i] = err
		})
	}

	close(start)
	wg.Wait()

	for i := range workers {
		require.NoError(t, errs[i])
		assert.Equal(t, ids[0], ids[i])
	}

	r, err := e.client.ListZones(t.Context(), ocidnssdk.ListZonesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.Len(t, r.Items, 1)
}

func TestZoneFromFileRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	file := "$ORIGIN tok.example.\n" +
		"tok.example.\t300\tIN\tSOA\tns1.p68.dns.oraclecloud.net. hostmaster.tok.example. 1 3600 600 604800 1800\n"

	var ids [2]string

	for i := range ids {
		r, err := e.client.CreateZoneFromZoneFile(t.Context(), ocidnssdk.CreateZoneFromZoneFileRequest{
			CompartmentId:                 new(testCompartment),
			CreateZoneFromZoneFileDetails: readCloser(file),
			OpcRetryToken:                 new("tok"),
		})
		require.NoError(t, err)

		ids[i] = *r.Id
	}

	assert.Equal(t, ids[0], ids[1])
}
