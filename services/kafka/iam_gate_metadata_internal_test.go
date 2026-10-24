package kafka

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func metadataUpstreamResponse(version int16, corr uint32, names ...string) []byte {
	resp := kmsg.NewPtrMetadataResponse()
	resp.SetVersion(version)

	b := kmsg.NewMetadataResponseBroker()
	b.NodeID, b.Host, b.Port = 1, "broker-1", 9098
	resp.Brokers = append(resp.Brokers, b)

	if version >= 2 {
		cid := "cluster-x"
		resp.ClusterID = &cid
	}

	for _, n := range names {
		t := kmsg.NewMetadataResponseTopic()
		t.Topic = &n
		resp.Topics = append(resp.Topics, t)
	}

	buf := []byte{byte(corr >> 24), byte(corr >> 16), byte(corr >> 8), byte(corr)}
	if resp.IsFlexible() {
		buf = append(buf, 0)
	}

	return resp.AppendTo(buf)
}

func (h *gateHarness) metadataUpstream(version int16, names ...string) {
	go func() {
		for {
			frame, err := readFrame(h.upstream)
			if err != nil {
				return
			}

			resp := metadataUpstreamResponse(version, corrOf(frame[4:]), names...)
			if writeFrame(h.upstream, resp) != nil {
				return
			}
		}
	}()
}

func decodeMetadata(t *testing.T, frame []byte, version int16) *kmsg.MetadataResponse {
	t.Helper()

	resp := kmsg.NewPtrMetadataResponse()
	resp.SetVersion(version)

	off := 4
	if resp.IsFlexible() {
		off++
	}

	require.NoError(t, resp.ReadFrom(frame[off:]))

	return resp
}

func TestGate_MetadataAllTopicsFiltered(t *testing.T) {
	t.Parallel()

	describe := func(topic string) string { return "kafka-cluster:DescribeTopic|" + gateTopicARN(topic) }

	tests := []struct {
		name     string
		explicit []string
		grants   []string
		want     []string
		versions []int16
	}{
		{
			name: "subset", grants: []string{describe("a"), describe("c")}, want: []string{"a", "c"},
			versions: []int16{0, 1, 4, 8, 9, 11, 12},
		},
		{name: "none", want: nil, versions: []int16{0, 1, 9, 12}},
		{
			name: "all", grants: []string{describe("a"), describe("b"), describe("c")}, want: []string{"a", "b", "c"},
			versions: []int16{1, 9},
		},
		{
			name: "explicit_passthrough", explicit: []string{"a", "b"},
			grants: []string{describe("a"), describe("b")}, want: []string{"a", "b", "c"}, versions: []int16{1, 9},
		},
	}

	for _, tt := range tests {
		for _, v := range tt.versions {
			t.Run(fmt.Sprintf("%s_v%d", tt.name, v), func(t *testing.T) {
				t.Parallel()

				h := startGate(t, allowOnly(tt.grants...))
				h.metadataUpstream(v, "a", "b", "c")

				req := kmsg.NewPtrMetadataRequest()
				req.SetVersion(v)

				if tt.explicit != nil {
					for _, n := range tt.explicit {
						rt := kmsg.NewMetadataRequestTopic()
						rt.Topic = &n
						req.Topics = append(req.Topics, rt)
					}
				} else if v == 0 {
					req.Topics = []kmsg.MetadataRequestTopic{}
				}

				h.send(t, 11, req)

				frame := h.recv(t)
				assert.Equal(t, uint32(11), corrOf(frame))

				resp := decodeMetadata(t, frame, v)

				got := make([]string, 0, len(resp.Topics))
				for _, rt := range resp.Topics {
					got = append(got, *rt.Topic)
				}

				assert.ElementsMatch(t, tt.want, got)
				require.Len(t, resp.Brokers, 1)
				assert.Equal(t, "broker-1", resp.Brokers[0].Host)
			})
		}
	}
}
