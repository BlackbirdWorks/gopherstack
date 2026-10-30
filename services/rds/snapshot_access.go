package rds

import (
	"net/url"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	callerAccountParam = "X-Gopherstack-Caller-Account"
	restoreAttribute   = "restore"
	shareWithAll       = "all"
	snapshotTypeShared = "shared"
	snapshotTypePublic = "public"
)

// SnapshotQuery selects snapshots for DescribeDBSnapshots and
// DescribeDBClusterSnapshots on behalf of CallerAccount ("" = no identity,
// everything visible).
type SnapshotQuery struct {
	CallerAccount string
	SnapshotID    string
	ParentID      string
	SnapshotType  string
	IncludeShared bool
	IncludePublic bool
}

// snapshotRef splits a snapshot reference into its identifier and whether it
// was given as an ARN (the only way to address another account's snapshot).
func snapshotRef(ref string) (string, bool) {
	id := rdsIDFromARN(ref)

	return id, id != ref
}

func ownsSnapshot(caller, owner string) bool {
	return caller == "" || owner == "" || owner == caller
}

func sharedWithAccount(restore []string, account string) bool {
	return account != "" && slices.Contains(restore, account)
}

func (b *InMemoryBackend) restoreValuesLocked(kind string, id string) []string {
	var attrs []DBSnapshotAttribute

	if kind == snapKindCluster {
		if r, ok := b.clusterSnapshotAttributes.Get(normalizeID(id)); ok {
			attrs = r.DBClusterSnapshotAttributes
		}
	} else if r, ok := b.snapshotAttributes.Get(normalizeID(id)); ok {
		attrs = r.DBSnapshotAttributes
	}

	for _, a := range attrs {
		if a.AttributeName == restoreAttribute {
			return a.AttributeValues
		}
	}

	return nil
}

// snapshotReachableLocked reports whether caller may address the snapshot:
// its owner always, anyone else only by ARN and only when the owner shared it
// with the caller's account or with "all".
func (b *InMemoryBackend) snapshotReachableLocked(caller, owner string, kind string, id string, byARN bool) bool {
	if ownsSnapshot(caller, owner) {
		return true
	}

	if !byARN {
		return false
	}

	restore := b.restoreValuesLocked(kind, id)

	return sharedWithAccount(restore, caller) || slices.Contains(restore, shareWithAll)
}

// snapshotListedLocked applies DescribeDB(Cluster)Snapshots' SnapshotType,
// IncludeShared and IncludePublic semantics (api_op_DescribeDBSnapshots.go).
func (b *InMemoryBackend) snapshotListedLocked(
	q SnapshotQuery, kind string, id, owner, storedType string,
) bool {
	own := ownsSnapshot(q.CallerAccount, owner)
	restore := b.restoreValuesLocked(kind, id)
	public := slices.Contains(restore, shareWithAll)

	switch q.SnapshotType {
	case snapshotTypeShared:
		return !own && sharedWithAccount(restore, q.CallerAccount)
	case snapshotTypePublic:
		return public
	case "":
		switch {
		case own:
			return true
		case q.IncludeShared && sharedWithAccount(restore, q.CallerAccount):
			return true
		default:
			return q.IncludePublic && public
		}
	default:
		return own && storedType == q.SnapshotType
	}
}

func (b *InMemoryBackend) ownedSnapshotARN(owner, resource, id string) string {
	if owner == "" {
		return b.rdsARN(resource, id)
	}

	return arn.Build("rds", b.region, owner, resource+":"+id)
}

func parseSnapshotQuery(vals url.Values, snapshotID, parentID string) SnapshotQuery {
	return SnapshotQuery{
		CallerAccount: vals.Get(callerAccountParam),
		SnapshotID:    snapshotID,
		ParentID:      parentID,
		SnapshotType:  vals.Get("SnapshotType"),
		IncludeShared: vals.Get("IncludeShared") == formTrue,
		IncludePublic: vals.Get("IncludePublic") == formTrue,
	}
}
