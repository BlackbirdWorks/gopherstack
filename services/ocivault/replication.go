package ocivault

import (
	"net/http"
	"regexp"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	replCreating = "CREATING"
	replCreated  = "CREATED"
	replDeleting = "DELETING"
	replDeleted  = "DELETED"

	statusReplicating = "REPLICATING"
	statusReplicated  = "REPLICATED"

	replOpRetention = 24 * time.Hour
	replIDLen       = 32
)

var regionRe = regexp.MustCompile(`^[a-z]+-[a-z]+-[0-9]+$`)

// VaultReplica is a vault's copy in another region.
type VaultReplica struct {
	transition
	TimeCreated   time.Time `json:"timeCreated"`
	Region        string    `json:"region"`
	Status        string    `json:"status"`
	ReplicationID string    `json:"replicationId"`
}

// replOp is one replicated change; it counts as REPLICATED in every region once ReadyAt passes.
type replOp struct {
	ReadyAt time.Time `json:"readyAt"`
	Regions []string  `json:"regions"`
}

// CreateReplicaIn is the CreateVaultReplica body.
type CreateReplicaIn struct {
	ReplicaRegion string `json:"replicaRegion"`
}

// DeleteReplicaIn is the DeleteVaultReplica body.
type DeleteReplicaIn struct {
	ReplicaRegion string `json:"replicaRegion"`
}

type replicaSummaryWire struct {
	CryptoEndpoint     string `json:"cryptoEndpoint"`
	ManagementEndpoint string `json:"managementEndpoint"`
	Region             string `json:"region"`
	Status             string `json:"status"`
}

type replicaDetailWire struct {
	Region string `json:"region"`
	Status string `json:"status"`
}

type replicationStatusWire struct {
	ReplicaDetails []replicaDetailWire `json:"replicaDetails"`
}

type replicationRef struct {
	ReplicationID string `json:"replicationId"`
}

func (v *Vault) liveReplica(region string) *VaultReplica {
	for _, r := range v.Replicas {
		if r.Region == region && r.Status != replDeleted {
			return r
		}
	}

	return nil
}

func (v *Vault) replicaRegions() []string {
	var out []string

	for _, r := range v.Replicas {
		if r.Status == replCreating || r.Status == replCreated {
			out = append(out, r.Region)
		}
	}

	return out
}

// hasReplicas reports whether any replica still exists in some form.
func (v *Vault) hasReplicas() bool {
	return slices.ContainsFunc(v.Replicas, func(r *VaultReplica) bool { return r.Status != replDeleted })
}

func (b *InMemoryBackend) newReplOp(regions []string) string {
	id := awsid.NewN("", replIDLen)
	b.repl[id] = &replOp{ReadyAt: b.nowUTC().Add(b.delay), Regions: slices.Clone(regions)}

	return id
}

// stampKey records a replication operation for a key change when its vault has replicas.
func (b *InMemoryBackend) stampKey(k *Key) {
	v, ok := b.vaults[k.VaultID]
	if !ok {
		return
	}

	if regions := v.replicaRegions(); len(regions) > 0 {
		k.ReplicationID = b.newReplOp(regions)
	}
}

func (b *InMemoryBackend) touchKey(k *Key) {
	k.Etag = newEtag()
	b.stampKey(k)
}

func replicaEndpoints(v *Vault, region, hostPort string) (crypto, mgmt string) {
	return "http://" + v.Prefix + "-" + region + "-crypto.localhost" + hostPort,
		"http://" + v.Prefix + "-" + region + "-management.localhost" + hostPort
}

// CreateVaultReplica adds a replica of a virtual private vault in another region.
func (b *InMemoryBackend) CreateVaultReplica(id, ifMatch string, in CreateReplicaIn) error {
	if !regionRe.MatchString(in.ReplicaRegion) {
		return errParam("replicaRegion must be a region identifier such as us-phoenix-1")
	}

	if in.ReplicaRegion == Region {
		return errParam("replicaRegion must differ from the vault's own region")
	}

	b.mu.Lock("CreateVaultReplica")
	defer b.mu.Unlock()

	v, aerr := b.activeVault(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(v.Etag, ifMatch); aerr != nil {
		return aerr
	}

	if v.VaultType != "VIRTUAL_PRIVATE" {
		return errParam("Only virtual private vaults can be replicated")
	}

	if v.liveReplica(in.ReplicaRegion) != nil {
		return nil
	}

	if v.hasReplicas() {
		return errExists("Vault " + id + " is already replicated to another region; delete that replica first")
	}

	r := &VaultReplica{Region: in.ReplicaRegion, TimeCreated: b.nowUTC(), ReplicationID: b.newReplOp([]string{in.ReplicaRegion})}
	b.begin(&r.Status, &r.transition, replCreating, replCreated)
	v.Replicas = append(v.Replicas, r)
	v.ReplicationID, v.Etag = r.ReplicationID, newEtag()

	for _, k := range b.keys {
		if k.VaultID == id && k.State != stateDeleted {
			b.stampKey(k)
		}
	}

	return nil
}

// DeleteVaultReplica removes the replica in the given region.
func (b *InMemoryBackend) DeleteVaultReplica(id, ifMatch string, in DeleteReplicaIn) error {
	b.mu.Lock("DeleteVaultReplica")
	defer b.mu.Unlock()

	v, aerr := b.activeVault(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(v.Etag, ifMatch); aerr != nil {
		return aerr
	}

	r := v.liveReplica(in.ReplicaRegion)
	if r == nil {
		return errNotFound("replica " + in.ReplicaRegion)
	}

	if r.Status == replDeleting {
		return nil
	}

	r.ReplicationID = b.newReplOp([]string{r.Region})
	b.begin(&r.Status, &r.transition, replDeleting, replDeleted)
	v.ReplicationID, v.Etag = r.ReplicationID, newEtag()

	return nil
}

// ListVaultReplicas lists the replicas of a vault.
func (b *InMemoryBackend) ListVaultReplicas(id, hostPort string, o ListOpts) (page.Page[replicaSummaryWire], error) {
	b.mu.RLock("ListVaultReplicas")
	defer b.mu.RUnlock()

	v, aerr := b.vaultByID(id)
	if aerr != nil {
		return page.Page[replicaSummaryWire]{}, aerr
	}

	var all []*VaultReplica

	for _, r := range v.Replicas {
		if r.Status != replDeleted {
			all = append(all, r)
		}
	}

	pg := paginate(all, o, func(r *VaultReplica) string { return r.Region },
		func(r *VaultReplica) (time.Time, int64) { return r.TimeCreated, 0 })

	out := make([]replicaSummaryWire, 0, len(pg.Data))

	for _, r := range pg.Data {
		c, m := replicaEndpoints(v, r.Region, hostPort)
		out = append(out, replicaSummaryWire{CryptoEndpoint: c, ManagementEndpoint: m, Region: r.Region, Status: r.Status})
	}

	return page.Page[replicaSummaryWire]{Data: out, Next: pg.Next}, nil
}

// GetReplicationStatus reports whether a replicated operation has been applied in every replica region.
func (b *InMemoryBackend) GetReplicationStatus(replicationID string) (replicationStatusWire, error) {
	b.mu.RLock("GetReplicationStatus")
	defer b.mu.RUnlock()

	op, ok := b.repl[replicationID]
	if !ok {
		return replicationStatusWire{}, errNotFound(replicationID)
	}

	status := statusReplicating
	if !b.nowUTC().Before(op.ReadyAt) {
		status = statusReplicated
	}

	out := replicationStatusWire{ReplicaDetails: make([]replicaDetailWire, 0, len(op.Regions))}
	for _, r := range op.Regions {
		out.ReplicaDetails = append(out.ReplicaDetails, replicaDetailWire{Region: r, Status: status})
	}

	return out, nil
}

func (b *InMemoryBackend) pruneRepl(now time.Time) {
	for id, op := range b.repl {
		if now.Sub(op.ReadyAt) > replOpRetention {
			delete(b.repl, id)
		}
	}
}

// replicaHint reports whether an endpoint hint ("<prefix>-<region>") names a replica; it returns the prefix.
func replicaHint(hint string) (prefix, region string, ok bool) {
	if len(hint) <= prefixLen+1 || hint[prefixLen] != '-' {
		return "", "", false
	}

	return hint[:prefixLen], hint[prefixLen+1:], regionRe.MatchString(hint[prefixLen+1:])
}

var errReplicaReadOnly = newErr(http.StatusBadRequest, "InvalidParameter",
	"Replica vaults are read-only; make this change in the primary region")

func replicaWrites(op string) bool {
	switch op {
	case opCreateKey, opUpdateKey, opEnableKey, opDisableKey, opScheduleKeyDel, opCancelKeyDel, opChangeKeyComp,
		opCreateKeyVersion, opScheduleKeyVerDel, opCancelKeyVerDel, opImportKey, opImportKeyVersion, opBackupKey,
		opRestoreKeyFile, opRestoreKeyStore:
		return true
	}

	return false
}
