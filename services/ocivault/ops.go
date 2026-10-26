package ocivault

import (
	"maps"
	"net/http"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func buildOps() map[string]opFunc {
	ops := map[string]opFunc{}
	for _, m := range []map[string]opFunc{vaultOps(), keyOps(), cryptoOps(), secretOps(), bundleOps()} {
		maps.Copy(ops, m)
	}

	return ops
}

func vaultOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateVault: (*Handler).createVault, opGetVault: (*Handler).getVault, opListVaults: (*Handler).listVaults,
		opUpdateVault: (*Handler).updateVault, opScheduleVaultDel: (*Handler).scheduleVaultDeletion,
		opCancelVaultDel: (*Handler).cancelVaultDeletion, opChangeVaultComp: (*Handler).changeVaultCompartment,
		opGetVaultUsage: (*Handler).getVaultUsage,
	}
}

func keyOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateKey:         (*Handler).createKey,
		opGetKey:            (*Handler).getKey,
		opListKeys:          (*Handler).listKeys,
		opUpdateKey:         (*Handler).updateKey,
		opEnableKey:         (*Handler).enableKey,
		opDisableKey:        (*Handler).disableKey,
		opScheduleKeyDel:    (*Handler).scheduleKeyDeletion,
		opCancelKeyDel:      (*Handler).cancelKeyDeletion,
		opChangeKeyComp:     (*Handler).changeKeyCompartment,
		opCreateKeyVersion:  (*Handler).createKeyVersion,
		opGetKeyVersion:     (*Handler).getKeyVersion,
		opListKeyVersions:   (*Handler).listKeyVersions,
		opScheduleKeyVerDel: (*Handler).scheduleKeyVersionDeletion,
		opCancelKeyVerDel:   (*Handler).cancelKeyVersionDeletion,
		opImportKey:         (*Handler).importKey,
		opImportKeyVersion:  (*Handler).importKeyVersion,
		opGetWrappingKey:    (*Handler).getWrappingKey,
	}
}

func cryptoOps() map[string]opFunc {
	return map[string]opFunc{
		opEncrypt: (*Handler).encrypt, opDecrypt: (*Handler).decrypt, opGenerateDEK: (*Handler).generateDEK,
		opSign: (*Handler).sign, opVerify: (*Handler).verify, opExportKey: (*Handler).exportKey,
	}
}

func secretOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateSecret:       (*Handler).createSecret,
		opGetSecret:          (*Handler).getSecret,
		opListSecrets:        (*Handler).listSecrets,
		opUpdateSecret:       (*Handler).updateSecret,
		opScheduleSecretDel:  (*Handler).scheduleSecretDeletion,
		opCancelSecretDel:    (*Handler).cancelSecretDeletion,
		opChangeSecretComp:   (*Handler).changeSecretCompartment,
		opListSecretVersions: (*Handler).listSecretVersions,
		opGetSecretVersion:   (*Handler).getSecretVersion,
		opScheduleSecretVDel: (*Handler).scheduleSecretVersionDeletion,
		opCancelSecretVDel:   (*Handler).cancelSecretVersionDeletion,
	}
}

func bundleOps() map[string]opFunc {
	return map[string]opFunc{
		opGetSecretBundle: (*Handler).getSecretBundle, opGetBundleByName: (*Handler).getBundleByName,
		opListBundleVersions: (*Handler).listBundleVersions,
	}
}

func reply[T any](q *req, w T, etag string, err error) error {
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, etag)
}

func listReply[T any](q *req, pg page.Page[T], err error) error {
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func done(q *req, err error) error {
	if err != nil {
		return err
	}

	return q.noContent("")
}

func okEmpty(q *req, err error) error {
	if err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusOK)

	return nil
}

// scope resolves the vault the request host addresses, or "" when it names none.
func (h *Handler) scope(q *req) (string, error) {
	if q.rt.hint == "" {
		return "", nil
	}

	id, aerr := h.Backend.VaultIDForHost(q.rt.hint)
	if aerr != nil {
		return "", aerr
	}

	return id, nil
}

// vault resolves the addressed vault, falling back to the only existing vault.
func (h *Handler) vault(q *req) (string, error) {
	id, aerr := h.Backend.VaultIDForHost(q.rt.hint)
	if aerr != nil {
		return "", aerr
	}

	return id, nil
}

func deletionAt(q *req) (*time.Time, error) {
	var in struct {
		TimeOfDeletion *time.Time `json:"timeOfDeletion"`
	}

	if len(q.body) == 0 {
		return nil, nil //nolint:nilnil // an absent body means the default deletion window
	}

	if err := q.decode(&in); err != nil {
		return nil, err
	}

	return in.TimeOfDeletion, nil
}

func compartmentIn(q *req) (string, error) {
	var in struct {
		CompartmentID string `json:"compartmentId"`
	}

	if err := q.decode(&in); err != nil {
		return "", err
	}

	return in.CompartmentID, nil
}

func (h *Handler) createVault(q *req) error {
	var in CreateVaultIn
	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.CreateVault(in, q.hostPort())

	return reply(q, w, etag, err)
}

func (h *Handler) getVault(q *req) error {
	w, etag, err := h.Backend.GetVault(q.param("vaultId"), q.hostPort())

	return reply(q, w, etag, err)
}

func (h *Handler) listVaults(q *req) error {
	pg, err := h.Backend.ListVaults(q.query("compartmentId"), q.hostPort(), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateVault(q *req) error {
	var in UpdateVaultIn
	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.UpdateVault(q.param("vaultId"), q.ifMatch(), q.hostPort(), in)

	return reply(q, w, etag, err)
}

func (h *Handler) scheduleVaultDeletion(q *req) error {
	at, err := deletionAt(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.ScheduleVaultDeletion(q.param("vaultId"), q.ifMatch(), q.hostPort(), at)

	return reply(q, w, etag, err)
}

func (h *Handler) cancelVaultDeletion(q *req) error {
	w, etag, err := h.Backend.CancelVaultDeletion(q.param("vaultId"), q.ifMatch(), q.hostPort())

	return reply(q, w, etag, err)
}

func (h *Handler) changeVaultCompartment(q *req) error {
	c, err := compartmentIn(q)
	if err != nil {
		return err
	}

	return okEmpty(q, h.Backend.ChangeVaultCompartment(q.param("vaultId"), q.ifMatch(), c))
}

func (h *Handler) getVaultUsage(q *req) error {
	w, err := h.Backend.GetVaultUsage(q.param("vaultId"))

	return reply(q, w, "", err)
}

func (h *Handler) createKey(q *req) error {
	var in CreateKeyIn
	if err := q.decode(&in); err != nil {
		return err
	}

	vid, err := h.vault(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.CreateKey(vid, in)

	return reply(q, w, etag, err)
}

func (h *Handler) getKey(q *req) error {
	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.GetKey(vid, q.param("keyId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listKeys(q *req) error {
	vid, err := h.vault(q)
	if err != nil {
		return err
	}

	n, _ := strconv.Atoi(q.query("length"))
	f := KeyFilter{
		ProtectionMode: q.query("protectionMode"), Algorithm: q.query("algorithm"),
		CurveID: q.query("curveId"), Length: n,
	}
	pg, err := h.Backend.ListKeys(vid, q.query("compartmentId"), f, q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateKey(q *req) error {
	var in UpdateKeyIn
	if err := q.decode(&in); err != nil {
		return err
	}

	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.UpdateKey(vid, q.param("keyId"), q.ifMatch(), in)

	return reply(q, w, etag, err)
}

func (h *Handler) keyAction(q *req, fn func(vaultID, keyID, ifMatch string) (keyWire, string, error)) error {
	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	w, etag, err := fn(vid, q.param("keyId"), q.ifMatch())

	return reply(q, w, etag, err)
}

func (h *Handler) enableKey(q *req) error  { return h.keyAction(q, h.Backend.EnableKey) }
func (h *Handler) disableKey(q *req) error { return h.keyAction(q, h.Backend.DisableKey) }

func (h *Handler) cancelKeyDeletion(q *req) error {
	return h.keyAction(q, h.Backend.CancelKeyDeletion)
}

func (h *Handler) scheduleKeyDeletion(q *req) error {
	at, err := deletionAt(q)
	if err != nil {
		return err
	}

	return h.keyAction(q, func(v, k, m string) (keyWire, string, error) {
		return h.Backend.ScheduleKeyDeletion(v, k, m, at)
	})
}

func (h *Handler) changeKeyCompartment(q *req) error {
	c, err := compartmentIn(q)
	if err != nil {
		return err
	}

	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	return okEmpty(q, h.Backend.ChangeKeyCompartment(vid, q.param("keyId"), q.ifMatch(), c))
}

func (h *Handler) createKeyVersion(q *req) error {
	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.CreateKeyVersion(vid, q.param("keyId"))

	return reply(q, w, etag, err)
}

func (h *Handler) getKeyVersion(q *req) error {
	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.GetKeyVersion(vid, q.param("keyId"), q.param("keyVersionId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listKeyVersions(q *req) error {
	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	pg, err := h.Backend.ListKeyVersions(vid, q.param("keyId"), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) scheduleKeyVersionDeletion(q *req) error {
	at, err := deletionAt(q)
	if err != nil {
		return err
	}

	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.ScheduleKeyVersionDeletion(vid, q.param("keyId"), q.param("keyVersionId"), at)

	return reply(q, w, etag, err)
}

func (h *Handler) cancelKeyVersionDeletion(q *req) error {
	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.CancelKeyVersionDeletion(vid, q.param("keyId"), q.param("keyVersionId"))

	return reply(q, w, etag, err)
}

func (h *Handler) importKey(q *req) error {
	var in ImportKeyIn
	if err := q.decode(&in); err != nil {
		return err
	}

	vid, err := h.vault(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.ImportKey(vid, in)

	return reply(q, w, etag, err)
}

func (h *Handler) importKeyVersion(q *req) error {
	var in ImportKeyVersionIn
	if err := q.decode(&in); err != nil {
		return err
	}

	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.ImportKeyVersion(vid, q.param("keyId"), in)

	return reply(q, w, etag, err)
}

func (h *Handler) getWrappingKey(q *req) error {
	vid, err := h.vault(q)
	if err != nil {
		return err
	}

	w, err := h.Backend.GetWrappingKey(vid)

	return reply(q, w, "", err)
}

func cryptoCall[I any, O any](h *Handler, q *req, fn func(vaultID string, in I) (O, error)) error {
	var in I
	if err := q.decode(&in); err != nil {
		return err
	}

	vid, err := h.scope(q)
	if err != nil {
		return err
	}

	w, err := fn(vid, in)

	return reply(q, w, "", err)
}

func (h *Handler) encrypt(q *req) error { return cryptoCall(h, q, h.Backend.Encrypt) }
func (h *Handler) decrypt(q *req) error { return cryptoCall(h, q, h.Backend.Decrypt) }
func (h *Handler) generateDEK(q *req) error {
	return cryptoCall(h, q, h.Backend.GenerateDataEncryptionKey)
}
func (h *Handler) sign(q *req) error      { return cryptoCall(h, q, h.Backend.Sign) }
func (h *Handler) verify(q *req) error    { return cryptoCall(h, q, h.Backend.Verify) }
func (h *Handler) exportKey(q *req) error { return cryptoCall(h, q, h.Backend.ExportKey) }

func (h *Handler) createSecret(q *req) error {
	var in CreateSecretIn
	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.CreateSecret(in)

	return reply(q, w, etag, err)
}

func (h *Handler) getSecret(q *req) error {
	w, etag, err := h.Backend.GetSecret(q.param("secretId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listSecrets(q *req) error {
	f := SecretFilter{
		Compartment: q.query("compartmentId"), Name: q.query("name"), VaultID: q.query("vaultId"),
		State: q.query("lifecycleState"),
	}
	pg, err := h.Backend.ListSecrets(f, q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateSecret(q *req) error {
	var in UpdateSecretIn
	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.UpdateSecret(q.param("secretId"), q.ifMatch(), in)

	return reply(q, w, etag, err)
}

func (h *Handler) scheduleSecretDeletion(q *req) error {
	at, err := deletionAt(q)
	if err != nil {
		return err
	}

	return done(q, h.Backend.ScheduleSecretDeletion(q.param("secretId"), q.ifMatch(), at))
}

func (h *Handler) cancelSecretDeletion(q *req) error {
	return done(q, h.Backend.CancelSecretDeletion(q.param("secretId"), q.ifMatch()))
}

func (h *Handler) changeSecretCompartment(q *req) error {
	c, err := compartmentIn(q)
	if err != nil {
		return err
	}

	return okEmpty(q, h.Backend.ChangeSecretCompartment(q.param("secretId"), q.ifMatch(), c))
}

func (h *Handler) listSecretVersions(q *req) error {
	pg, err := h.Backend.ListSecretVersions(q.param("secretId"), q.listOpts())

	return listReply(q, pg, err)
}

func versionNumber(q *req) (int64, error) {
	n, err := strconv.ParseInt(q.param("versionNumber"), 10, 64)
	if err != nil || n < 1 {
		return 0, errParam("secretVersionNumber must be a positive integer")
	}

	return n, nil
}

func (h *Handler) getSecretVersion(q *req) error {
	n, err := versionNumber(q)
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.GetSecretVersion(q.param("secretId"), n)

	return reply(q, w, etag, err)
}

func (h *Handler) scheduleSecretVersionDeletion(q *req) error {
	n, err := versionNumber(q)
	if err != nil {
		return err
	}

	at, err := deletionAt(q)
	if err != nil {
		return err
	}

	return done(q, h.Backend.ScheduleSecretVersionDeletion(q.param("secretId"), n, q.ifMatch(), at))
}

func (h *Handler) cancelSecretVersionDeletion(q *req) error {
	n, err := versionNumber(q)
	if err != nil {
		return err
	}

	return done(q, h.Backend.CancelSecretVersionDeletion(q.param("secretId"), n, q.ifMatch()))
}

func (q *req) selector() (BundleSelector, error) {
	sel := BundleSelector{VersionName: q.query("secretVersionName"), Stage: q.query("stage")}

	if v := q.query("versionNumber"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			return sel, errParam("versionNumber must be a positive integer")
		}

		sel.VersionNumber = &n
	}

	return sel, nil
}

func (h *Handler) getSecretBundle(q *req) error {
	sel, err := q.selector()
	if err != nil {
		return err
	}

	w, etag, err := h.Backend.GetSecretBundle(q.param("secretId"), sel)

	return reply(q, w, etag, err)
}

func (h *Handler) getBundleByName(q *req) error {
	sel, err := q.selector()
	if err != nil {
		return err
	}

	w, err := h.Backend.GetSecretBundleByName(q.query("vaultId"), q.query("secretName"), sel)

	return reply(q, w, "", err)
}

func (h *Handler) listBundleVersions(q *req) error {
	pg, err := h.Backend.ListSecretBundleVersions(q.param("secretId"), q.listOpts())

	return listReply(q, pg, err)
}
