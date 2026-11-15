package ocicore

import (
	"net/http"

	"golang.org/x/crypto/ssh"
)

const (
	kindConsoleConn    = "instanceconsoleconnection"
	kindConsoleHistory = "instanceconsolehistory"
	statActive         = "ACTIVE"
	statDeleting       = "DELETING"
	statSucceeded      = "SUCCEEDED"
	statRequested      = "REQUESTED"
	consoleKeyBits     = 2048
)

// InstanceConsoleConnection is a console connection of an instance; no console is served behind it.
type InstanceConsoleConnection struct {
	Base
	InstanceID       string `json:"instanceId"`
	Fingerprint      string `json:"fingerprint"`
	ConnectionString string `json:"connectionString"`
}

// ConsoleHistory is a serial console capture of an instance.
type ConsoleHistory struct {
	Base
	InstanceID         string `json:"instanceId"`
	AvailabilityDomain string `json:"availabilityDomain"`
}

func (v *InstanceConsoleConnection) vcnRef() string { return "" }
func (v *ConsoleHistory) vcnRef() string            { return "" }

func (v *InstanceConsoleConnection) view() Resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *ConsoleHistory) view() Resource {
	c := *v
	c.Internal = nil

	return &c
}

// CreateConsoleConnectionIn is the CreateInstanceConsoleConnection request body.
type CreateConsoleConnectionIn struct {
	TagsIn
	InstanceID string `json:"instanceId"`
	PublicKey  string `json:"publicKey"`
}

// consoleFingerprint is the MD5 fingerprint of an RSA public key in authorized_keys format.
func consoleFingerprint(publicKey string) (string, *APIError) {
	key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(publicKey))
	if err != nil {
		return "", errParam("publicKey must be an SSH public key in authorized_keys format")
	}

	if key.Type() != ssh.KeyAlgoRSA {
		return "", errParam("publicKey must be an RSA key")
	}

	if cp, isCrypto := key.(ssh.CryptoPublicKey); isCrypto {
		if rk, isRSA := cp.CryptoPublicKey().(interface{ Size() int }); isRSA && rk.Size()*8 < consoleKeyBits {
			return "", errParam("publicKey must be at least a 2048 bit RSA key")
		}
	}

	return ssh.FingerprintLegacyMD5(key), nil
}

// CreateInstanceConsoleConnection creates a console connection for a running or stopped instance.
func (b *InMemoryBackend) CreateInstanceConsoleConnection(in CreateConsoleConnectionIn) (Resource, string, *APIError) {
	if in.InstanceID == "" || in.PublicKey == "" {
		return nil, "", errMissing("instanceId and publicKey are required")
	}

	inst, aerr := getLive[*Instance](b, in.InstanceID)
	if aerr != nil {
		return nil, "", aerr
	}

	if inst.LifecycleState != statRunning && inst.LifecycleState != statStopped {
		return nil, "", errState("The instance " + inst.ID + " is " + inst.LifecycleState)
	}

	fp, aerr := consoleFingerprint(in.PublicKey)
	if aerr != nil {
		return nil, "", aerr
	}

	c := &InstanceConsoleConnection{
		Base:       b.newBase(kindConsoleConn, inst.CompartmentID, "", in.TagsIn),
		InstanceID: inst.ID, Fingerprint: fp,
	}
	c.DisplayName = ""
	c.ConnectionString = "ssh -o ProxyCommand='ssh -W %h:%p -p 443 " + c.ID + "@instance-console." + regionName +
		".oci.oraclecloud.com' " + inst.ID
	b.moveTo(c, statCreating, statActive)
	b.add(c)

	return c.view(), c.Internal.Etag, nil
}

// UpdateConsoleIn is the UpdateInstanceConsoleConnection and UpdateConsoleHistory request body.
type UpdateConsoleIn = UpdateIn

// UpdateInstanceConsoleConnection changes the tags of a console connection.
func (b *InMemoryBackend) UpdateInstanceConsoleConnection(
	id, ifMatch string, in UpdateConsoleIn,
) (Resource, string, *APIError) {
	return updateWith(
		b,
		id,
		ifMatch,
		UpdateIn{TagsIn: in.TagsIn},
		func(*InstanceConsoleConnection) *APIError { return nil },
	)
}

// DeleteInstanceConsoleConnection ends a console connection.
func (b *InMemoryBackend) DeleteInstanceConsoleConnection(id, ifMatch string) *APIError {
	b.mu.Lock("DeleteInstanceConsoleConnection")
	defer b.mu.Unlock()

	c, aerr := get[*InstanceConsoleConnection](b, id)
	if aerr != nil {
		return aerr
	}

	if !live(c) || c.LifecycleState == statDeleted {
		return nil
	}

	if aerr = checkEtag(c, ifMatch); aerr != nil {
		return aerr
	}

	b.moveTo(c, statDeleting, statDeleted)

	return nil
}

// CaptureConsoleHistoryIn is the CaptureConsoleHistory request body.
type CaptureConsoleHistoryIn struct {
	TagsIn
	InstanceID  string `json:"instanceId"`
	DisplayName string `json:"displayName"`
}

// CaptureConsoleHistory records a console history request; no VM executes, so the captured content is empty.
func (b *InMemoryBackend) CaptureConsoleHistory(in CaptureConsoleHistoryIn) (Resource, string, *APIError) {
	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if in.InstanceID == "" {
		return nil, "", errMissing("instanceId is required")
	}

	inst, aerr := getLive[*Instance](b, in.InstanceID)
	if aerr != nil {
		return nil, "", aerr
	}

	if inst.LifecycleState != statRunning && inst.LifecycleState != statStopped {
		return nil, "", errState("The instance " + inst.ID + " is " + inst.LifecycleState)
	}

	name := b.stamped(in.DisplayName, "history")
	h := &ConsoleHistory{
		Base:       b.newBase(kindConsoleHistory, inst.CompartmentID, name, in.TagsIn),
		InstanceID: inst.ID, AvailabilityDomain: inst.AvailabilityDomain,
	}
	b.moveTo(h, statRequested, statSucceeded)
	b.add(h)

	return h.view(), h.Internal.Etag, nil
}

// UpdateConsoleHistory renames a console history or changes its tags.
func (b *InMemoryBackend) UpdateConsoleHistory(id, ifMatch string, in UpdateIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in, func(*ConsoleHistory) *APIError { return nil })
}

// DeleteConsoleHistory removes a console history.
func (b *InMemoryBackend) DeleteConsoleHistory(id, ifMatch string) *APIError {
	b.mu.Lock("DeleteConsoleHistory")
	defer b.mu.Unlock()

	h, aerr := get[*ConsoleHistory](b, id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(h, ifMatch); aerr != nil {
		return aerr
	}

	delete(b.res, id)

	return nil
}

// consoleHistoryContent is the captured console text, which is always empty because no instance runs an OS.
func (b *InMemoryBackend) consoleHistoryContent(id string) *APIError {
	b.mu.RLock("GetConsoleHistoryContent")
	defer b.mu.RUnlock()

	h, aerr := get[*ConsoleHistory](b, id)
	if aerr != nil {
		return aerr
	}

	if h.LifecycleState != statSucceeded {
		return errState("The console history " + id + " is " + h.LifecycleState)
	}

	return nil
}

func consoleFilter(q *req) func(Resource) bool {
	return and(
		queryEq(q, "instanceId", func(r Resource) string {
			if c, ok := r.(*InstanceConsoleConnection); ok {
				return c.InstanceID
			}

			x, _ := r.(*ConsoleHistory)

			return x.InstanceID
		}),
		queryEq(q, "availabilityDomain", func(r Resource) string {
			if x, ok := r.(*ConsoleHistory); ok {
				return x.AvailabilityDomain
			}

			return ""
		}),
	)
}

func consoleEntries() []entry {
	conn, hist := idPath("/instanceConsoleConnections", "instanceConsoleConnectionId"),
		idPath("/instanceConsoleHistories", "instanceConsoleHistoryId")

	return []entry{
		{
			createOp(
				(*InMemoryBackend).CreateInstanceConsoleConnection,
			),
			http.MethodPost,
			"/instanceConsoleConnections",
			"CreateInstanceConsoleConnection",
		},
		{
			listOp[*InstanceConsoleConnection](consoleConnFilter), http.MethodGet, "/instanceConsoleConnections",
			"ListInstanceConsoleConnections",
		},
		{
			getOp[*InstanceConsoleConnection]("instanceConsoleConnectionId"), http.MethodGet, conn,
			"GetInstanceConsoleConnection",
		},
		{
			updateOp("instanceConsoleConnectionId", (*InMemoryBackend).UpdateInstanceConsoleConnection), http.MethodPut,
			conn, "UpdateInstanceConsoleConnection",
		},
		{func(h *Handler, q *req) error {
			return noBody(
				q,
				h.Backend.DeleteInstanceConsoleConnection(q.param("instanceConsoleConnectionId"), q.ifMatch()),
			)
		}, http.MethodDelete, conn, "DeleteInstanceConsoleConnection"},
		{
			createOp((*InMemoryBackend).CaptureConsoleHistory), http.MethodPost, "/instanceConsoleHistories",
			"CaptureConsoleHistory",
		},
		{listOp[*ConsoleHistory](consoleFilter), http.MethodGet, "/instanceConsoleHistories", "ListConsoleHistories"},
		{getOp[*ConsoleHistory]("instanceConsoleHistoryId"), http.MethodGet, hist, "GetConsoleHistory"},
		{
			updateOp("instanceConsoleHistoryId", (*InMemoryBackend).UpdateConsoleHistory), http.MethodPut, hist,
			"UpdateConsoleHistory",
		},
		{func(h *Handler, q *req) error {
			return noBody(q, h.Backend.DeleteConsoleHistory(q.param("instanceConsoleHistoryId"), q.ifMatch()))
		}, http.MethodDelete, hist, "DeleteConsoleHistory"},
		{consoleContentOp, http.MethodGet, hist + "/data", "GetConsoleHistoryContent"},
	}
}

func consoleConnFilter(q *req) func(Resource) bool {
	return queryEq(q, "instanceId", func(r Resource) string {
		c, _ := r.(*InstanceConsoleConnection)

		return c.InstanceID
	})
}

func consoleContentOp(h *Handler, q *req) error {
	if aerr := h.Backend.consoleHistoryContent(q.param("instanceConsoleHistoryId")); aerr != nil {
		return aerr
	}

	q.header("opc-bytes-remaining", "0")
	q.header("Content-Type", "text/plain")
	q.w.WriteHeader(http.StatusOK)

	return nil
}
