package kafka

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestGate_AdminRequestsGated(t *testing.T) {
	t.Parallel()

	deleteRecords := kmsg.NewPtrDeleteRecordsRequest()
	dt := kmsg.NewDeleteRecordsRequestTopic()
	dt.Topic = "t1"
	deleteRecords.Topics = append(deleteRecords.Topics, dt)

	txnCommit := kmsg.NewPtrTxnOffsetCommitRequest()
	txnCommit.Group = "g1"

	clusterAlter := []string{"kafka-cluster:AlterCluster|" + gateClusterArn}

	tests := []struct {
		req    kmsg.Request
		name   string
		grants []string
	}{
		{
			name: "delete_records", req: deleteRecords,
			grants: []string{"kafka-cluster:DeleteTopic|" + gateTopicARN("t1")},
		},
		{name: "txn_offset_commit", req: txnCommit, grants: []string{"kafka-cluster:AlterGroup|" + gateGroupARN("g1")}},
		{name: "create_acls", req: kmsg.NewPtrCreateACLsRequest(), grants: clusterAlter},
		{name: "delete_acls", req: kmsg.NewPtrDeleteACLsRequest(), grants: clusterAlter},
		{
			name: "scram", req: kmsg.NewPtrAlterUserSCRAMCredentialsRequest(),
			grants: clusterAlter,
		},
		{
			name: "reassign", req: kmsg.NewPtrAlterPartitionAssignmentsRequest(),
			grants: clusterAlter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_denied", func(t *testing.T) {
			t.Parallel()

			h := startGate(t, allowOnly())
			h.send(t, 1, tt.req)

			_, err := readFrame(h.client)
			require.ErrorIs(t, err, io.EOF)
		})

		t.Run(tt.name+"_allowed", func(t *testing.T) {
			t.Parallel()

			h := startGate(t, allowOnly(tt.grants...))
			h.echoUpstream()
			h.send(t, 5, tt.req)

			assert.Equal(t, uint32(5), corrOf(h.recv(t)))
		})
	}
}
