package ocimonitoring_test

import (
	"context"
	"encoding/json/v2"
	"sync"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const streamOCID = "ocid1.stream.oc1..stream"

type streamSink struct {
	msgs map[string][][]byte
	mu   sync.Mutex
}

func (s *streamSink) put(_ context.Context, id string, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.msgs[id] = append(s.msgs[id], v)

	return nil
}

func TestAlarmDeliversToStream(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	sink := &streamSink{msgs: map[string][][]byte{}}
	e.backend.SetStreamPublisher(sink.put, func(id string) bool { return id == streamOCID })

	_, err := e.mon.CreateAlarm(t.Context(), monitoring.CreateAlarmRequest{
		CreateAlarmDetails: e.alarmDetails("ocid1.stream.oc1..gone", nil),
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	a := e.mkAlarm(t, streamOCID, nil)

	e.post(t, ns, metric, map[string]string{"host": "a"}, pt{0, 90, 0})
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusFiring, e.status(t))

	e.backend.Sweep(t.Context())

	sink.mu.Lock()
	defer sink.mu.Unlock()

	require.Len(t, sink.msgs[streamOCID], 1)

	var got struct {
		Type      string `json:"type"`
		DedupeKey string `json:"dedupeKey"`
	}

	require.NoError(t, json.Unmarshal(sink.msgs[streamOCID][0], &got, jsonx.V1()))
	assert.Equal(t, "OK_TO_FIRING", got.Type)
	assert.Equal(t, *a.Id, got.DedupeKey)
}
