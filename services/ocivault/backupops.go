package ocivault

import (
	"crypto/md5" //nolint:gosec // Content-MD5 is the integrity header the OCI API defines
	"encoding/base64"
	"net/http"
)

func backupOps() map[string]opFunc {
	return map[string]opFunc{
		opBackupVault: (*Handler).backupVault, opBackupKey: (*Handler).backupKey,
		opRestoreVaultFile: (*Handler).restoreVaultFromFile, opRestoreVaultStore: (*Handler).restoreVaultFromStore,
		opRestoreKeyFile: (*Handler).restoreKeyFromFile, opRestoreKeyStore: (*Handler).restoreKeyFromStore,
	}
}

func (h *Handler) backupVault(q *req) error {
	var in BackupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.BackupVault(q.param("vaultId"), q.ifMatch(), q.hostPort(), in)

	return reply(q, w, etag, err)
}

func (h *Handler) backupKey(q *req) error {
	var in BackupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.BackupKey(vid, q.param("keyId"), q.ifMatch(), in)

	return reply(q, w, etag, err)
}

// fileBody returns the uploaded backup after checking the optional Content-MD5 header, and echoes opc-content-md5.
func fileBody(q *req) ([]byte, error) {
	if len(q.body) == 0 {
		return nil, errMissing("A backup file is required")
	}

	sum := md5.Sum(q.body) //nolint:gosec // integrity header, not security
	enc := base64.StdEncoding.EncodeToString(sum[:])

	if want := q.r.Header.Get("Content-MD5"); want != "" && want != enc {
		return nil, newErr(http.StatusBadRequest, "InvalidDigest", "The Content-MD5 you specified did not match what we received")
	}

	q.header("opc-content-md5", enc)

	return q.body, nil
}

func (h *Handler) storedBackup(q *req) ([]byte, error) {
	var in BackupIn
	if err := q.decode(&in); err != nil {
		return nil, err
	}

	loc, aerr := in.location()
	if aerr != nil {
		return nil, aerr
	}

	store, aerr := h.Backend.store()
	if aerr != nil {
		return nil, aerr
	}

	data, aerr := readBlob(store, loc)
	if aerr != nil {
		return nil, aerr
	}

	return data, nil
}

func (h *Handler) restoreVaultFromFile(q *req) error {
	blob, err := fileBody(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.RestoreVault(q.query("compartmentId"), q.hostPort(), blob)

	return reply(q, w, etag, err)
}

func (h *Handler) restoreVaultFromStore(q *req) error {
	blob, err := h.storedBackup(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.RestoreVault(q.query("compartmentId"), q.hostPort(), blob)

	return reply(q, w, etag, err)
}

func (h *Handler) restoreKeyFromFile(q *req) error {
	blob, err := fileBody(q)
	if err != nil {
		return err
	}

	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.RestoreKey(vid, blob)

	return reply(q, w, etag, err)
}

func (h *Handler) restoreKeyFromStore(q *req) error {
	blob, err := h.storedBackup(q)
	if err != nil {
		return err
	}

	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.RestoreKey(vid, blob)

	return reply(q, w, etag, err)
}
