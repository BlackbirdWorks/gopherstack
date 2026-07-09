package transfer

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/sqs pilot (commit 0f09d77c) / services/ec2 sweep (commit
// 12e611a4) for the pattern this follows.
//
// Two fields remain plain maps, not [store.Table]s, because their values
// are not *T (store.Table's shape requirement):
//   - sshKeyBodies: values are map[string]struct{} (an SSH-key-body dedup
//     index derived from sshPublicKeys), not a pointer to a resource struct.
//   - tagsStore: values are map[string]string, not a pointer to a resource
//     struct.
//
// webAppCustomizations IS registered as a store.Table below (so Reset still
// clears it via registry.ResetAll), but persistTables deliberately excludes
// it from the persistence-scoped registry used by Snapshot/Restore: it was
// never part of backendSnapshot before this conversion, and this refactor
// preserves that pre-existing gap rather than silently starting to persist
// it (see persistence.go).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func serversKeyFn(v *Server) string                           { return v.ServerID }
func connectorsKeyFn(v *Connector) string                     { return v.ConnectorID }
func profilesKeyFn(v *Profile) string                         { return v.ProfileID }
func webAppsKeyFn(v *WebApp) string                           { return v.WebAppID }
func webAppCustomizationsKeyFn(v *WebAppCustomization) string { return v.WebAppID }
func workflowsKeyFn(v *Workflow) string                       { return v.WorkflowID }
func certificatesKeyFn(v *Certificate) string                 { return v.CertificateID }
func transferRecordsKeyFn(v *FileTransferResult) string       { return v.TransferID }
func asyncOperationsKeyFn(v *AsyncOperationRecord) string     { return v.ID }

// userKey builds the composite primary key for the users table: a User's
// identity is the (ServerID, UserName) pair -- the same username may exist
// on different servers -- not UserName alone. The NUL separator matches the
// composite-key convention already used elsewhere in this codebase (e.g.
// services/eks/store_setup.go) since AWS resource identifiers never contain
// a NUL byte.
func userKey(serverID, userName string) string { return serverID + "\x00" + userName }
func usersKeyFn(v *User) string                { return userKey(v.ServerID, v.UserName) }

func accessKey(serverID, externalID string) string { return serverID + "\x00" + externalID }
func accessesKeyFn(v *Access) string               { return accessKey(v.ServerID, v.ExternalID) }

func agreementKey(serverID, agreementID string) string { return serverID + "\x00" + agreementID }
func agreementsKeyFn(v *Agreement) string              { return agreementKey(v.ServerID, v.AgreementID) }

func hostKeyKey(serverID, hostKeyID string) string { return serverID + "\x00" + hostKeyID }
func hostKeysKeyFn(v *HostKey) string              { return hostKeyKey(v.ServerID, v.HostKeyID) }

func executionKey(workflowID, executionID string) string { return workflowID + "\x00" + executionID }
func executionsKeyFn(v *Execution) string                { return executionKey(v.WorkflowID, v.ExecutionID) }

// serverUserKey builds the composite key used by the sshPublicKeys table's
// "serverUser" index: SSH public keys are further scoped per (ServerID,
// UserName) beneath the server.
func serverUserKey(serverID, userName string) string { return serverID + "\x00" + userName }

func sshPublicKeyKey(serverID, userName, keyID string) string {
	return serverID + "\x00" + userName + "\x00" + keyID
}

func sshPublicKeysKeyFn(v *SSHPublicKey) string {
	return sshPublicKeyKey(v.ServerID, v.UserName, v.SSHPublicKeyID)
}

// registerAllTables registers every converted resource map on b.registry
// exactly once, and wires up every secondary index a call site needs (e.g.
// "all users of server X", "all SSH keys for user Y on server X"). It must
// be called during construction only, immediately after b.registry is
// created -- store.Register panics on a duplicate name, so runtime resets go
// through registry.ResetAll() instead (see InMemoryBackend.Reset in
// backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.servers = store.Register(b.registry, "servers", store.New(serversKeyFn))

	b.users = store.Register(b.registry, "users", store.New(usersKeyFn))
	b.usersByServer = b.users.AddIndex("server", func(v *User) string { return v.ServerID })

	b.accesses = store.Register(b.registry, "accesses", store.New(accessesKeyFn))
	b.accessesByServer = b.accesses.AddIndex("server", func(v *Access) string { return v.ServerID })

	b.agreements = store.Register(b.registry, "agreements", store.New(agreementsKeyFn))
	b.agreementsByServer = b.agreements.AddIndex("server", func(v *Agreement) string { return v.ServerID })

	b.connectors = store.Register(b.registry, "connectors", store.New(connectorsKeyFn))
	b.profiles = store.Register(b.registry, "profiles", store.New(profilesKeyFn))
	b.webApps = store.Register(b.registry, "webApps", store.New(webAppsKeyFn))
	b.webAppCustomizations = store.Register(
		b.registry, "webAppCustomizations", store.New(webAppCustomizationsKeyFn),
	)
	b.workflows = store.Register(b.registry, "workflows", store.New(workflowsKeyFn))
	b.certificates = store.Register(b.registry, "certificates", store.New(certificatesKeyFn))

	b.hostKeys = store.Register(b.registry, "hostKeys", store.New(hostKeysKeyFn))
	b.hostKeysByServer = b.hostKeys.AddIndex("server", func(v *HostKey) string { return v.ServerID })

	b.sshPublicKeys = store.Register(b.registry, "sshPublicKeys", store.New(sshPublicKeysKeyFn))
	b.sshKeysByServer = b.sshPublicKeys.AddIndex("server", func(v *SSHPublicKey) string { return v.ServerID })
	b.sshKeysByServerUser = b.sshPublicKeys.AddIndex(
		"serverUser",
		func(v *SSHPublicKey) string { return serverUserKey(v.ServerID, v.UserName) },
	)

	b.executions = store.Register(b.registry, "executions", store.New(executionsKeyFn))
	b.executionsByWorkflow = b.executions.AddIndex(
		"workflow",
		func(v *Execution) string { return v.WorkflowID },
	)

	b.transferRecords = store.Register(b.registry, "transferRecords", store.New(transferRecordsKeyFn))
	b.asyncOperations = store.Register(b.registry, "asyncOperations", store.New(asyncOperationsKeyFn))
}

// persistTables builds an ephemeral registry scoped to exactly the tables
// that participate in Snapshot/Restore, so persistence.go can reuse
// store.Registry's deterministic SnapshotAll/RestoreAll JSON encoding
// instead of hand-rolling it. It intentionally omits webAppCustomizations --
// see the file doc comment above. Registering the same live *Table under a
// second, ephemeral Registry alongside b.registry is safe: Register has no
// side effect on the Table itself, only on the Registry's own bookkeeping.
func persistTables(b *InMemoryBackend) *store.Registry {
	r := store.NewRegistry()

	store.Register(r, "servers", b.servers)
	store.Register(r, "users", b.users)
	store.Register(r, "accesses", b.accesses)
	store.Register(r, "agreements", b.agreements)
	store.Register(r, "connectors", b.connectors)
	store.Register(r, "profiles", b.profiles)
	store.Register(r, "webApps", b.webApps)
	store.Register(r, "workflows", b.workflows)
	store.Register(r, "certificates", b.certificates)
	store.Register(r, "hostKeys", b.hostKeys)
	store.Register(r, "sshPublicKeys", b.sshPublicKeys)
	store.Register(r, "executions", b.executions)
	store.Register(r, "transferRecords", b.transferRecords)
	store.Register(r, "asyncOperations", b.asyncOperations)

	return r
}
