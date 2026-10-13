package docdb

import (
	"fmt"
	"strings"
)

const syncStatusSynced = "synced"

func (b *InMemoryBackend) storeNewCluster(c *DBCluster, opts *CreateDBClusterOptions) error {
	if opts == nil || opts.GlobalClusterIdentifier == "" {
		b.clusterPut(c)

		return nil
	}
	gc, ok := b.globalClusters.Get(opts.GlobalClusterIdentifier)
	if !ok {
		return fmt.Errorf(
			"%w: global cluster %s not found", ErrGlobalClusterNotFound, opts.GlobalClusterIdentifier,
		)
	}
	b.clusterPut(c)
	gc.GlobalClusterMembers = append(gc.GlobalClusterMembers, GlobalClusterMember{
		DBClusterArn:          c.DBClusterArn,
		IsWriter:              len(gc.GlobalClusterMembers) == 0,
		SynchronizationStatus: syncStatusSynced,
	})
	b.syncGlobalLinkage(gc)

	return nil
}

func (b *InMemoryBackend) detachFromGlobalClusters(clusterARN string) {
	for _, gc := range b.globalClusters.All() {
		kept := gc.GlobalClusterMembers[:0:0]
		for _, m := range gc.GlobalClusterMembers {
			if m.DBClusterArn != clusterARN {
				kept = append(kept, m)
			}
		}
		if len(kept) == len(gc.GlobalClusterMembers) {
			continue
		}
		gc.GlobalClusterMembers = kept
		b.syncGlobalLinkage(gc)
	}
}

func (b *InMemoryBackend) localClusterByARN(clusterARN string) (*DBCluster, bool) {
	_, id, found := strings.Cut(clusterARN, ":cluster:")
	if !found {
		return nil, false
	}

	return b.clusterGet(regionFromARN(clusterARN, b.region), id)
}

func (b *InMemoryBackend) clearClusterLinkage(clusterARN string) {
	if c, ok := b.localClusterByARN(clusterARN); ok {
		c.ReplicationSourceIdentifier = ""
		c.ReadReplicaIdentifiers = nil
	}
}

// syncGlobalLinkage derives member Readers and each local member cluster's
// ReplicationSourceIdentifier/ReadReplicaIdentifiers from which member is the writer.
func (b *InMemoryBackend) syncGlobalLinkage(gc *GlobalCluster) {
	var writerARN string
	var readers []string
	for _, m := range gc.GlobalClusterMembers {
		if m.IsWriter {
			writerARN = m.DBClusterArn
		} else {
			readers = append(readers, m.DBClusterArn)
		}
	}
	for i := range gc.GlobalClusterMembers {
		m := &gc.GlobalClusterMembers[i]
		var memberReaders []string
		source := writerARN
		if m.IsWriter {
			memberReaders = append(memberReaders, readers...)
			source = ""
		}
		m.Readers = memberReaders
		if c, ok := b.localClusterByARN(m.DBClusterArn); ok {
			c.ReplicationSourceIdentifier = source
			c.ReadReplicaIdentifiers = append([]string(nil), memberReaders...)
		}
	}
}
