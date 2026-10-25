package ocivault

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateVault        = "CreateVault"
	opGetVault           = "GetVault"
	opListVaults         = "ListVaults"
	opUpdateVault        = "UpdateVault"
	opScheduleVaultDel   = "ScheduleVaultDeletion"
	opCancelVaultDel     = "CancelVaultDeletion"
	opChangeVaultComp    = "ChangeVaultCompartment"
	opGetVaultUsage      = "GetVaultUsage"
	opCreateKey          = "CreateKey"
	opGetKey             = "GetKey"
	opListKeys           = "ListKeys"
	opUpdateKey          = "UpdateKey"
	opEnableKey          = "EnableKey"
	opDisableKey         = "DisableKey"
	opScheduleKeyDel     = "ScheduleKeyDeletion"
	opCancelKeyDel       = "CancelKeyDeletion"
	opChangeKeyComp      = "ChangeKeyCompartment"
	opCreateKeyVersion   = "CreateKeyVersion"
	opGetKeyVersion      = "GetKeyVersion"
	opListKeyVersions    = "ListKeyVersions"
	opScheduleKeyVerDel  = "ScheduleKeyVersionDeletion"
	opCancelKeyVerDel    = "CancelKeyVersionDeletion"
	opImportKey          = "ImportKey"
	opImportKeyVersion   = "ImportKeyVersion"
	opGetWrappingKey     = "GetWrappingKey"
	opEncrypt            = "Encrypt"
	opDecrypt            = "Decrypt"
	opGenerateDEK        = "GenerateDataEncryptionKey"
	opSign               = "Sign"
	opVerify             = "Verify"
	opExportKey          = "ExportKey"
	opCreateSecret       = "CreateSecret"
	opGetSecret          = "GetSecret"
	opListSecrets        = "ListSecrets"
	opUpdateSecret       = "UpdateSecret"
	opScheduleSecretDel  = "ScheduleSecretDeletion"
	opCancelSecretDel    = "CancelSecretDeletion"
	opChangeSecretComp   = "ChangeSecretCompartment"
	opListSecretVersions = "ListSecretVersions"
	opGetSecretVersion   = "GetSecretVersion"
	opScheduleSecretVDel = "ScheduleSecretVersionDeletion"
	opCancelSecretVDel   = "CancelSecretVersionDeletion"
	opGetSecretBundle    = "GetSecretBundle"
	opGetBundleByName    = "GetSecretBundleByName"
	opListBundleVersions = "ListSecretBundleVersions"
	opAdminKeys          = "ManageAPIKeys"
	opUnknown            = "Unknown"
)

const (
	adminPrefix = "_gopherstack"
	vaultPrefix = "_vault"
	kmsBase     = "/20180608"
	secretsBase = "/20190301"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: kmsBase + "/keys/import", Op: opImportKey},
	{Method: http.MethodPost, Path: kmsBase + "/keys", Op: opCreateKey},
	{Method: http.MethodGet, Path: kmsBase + "/keys", Op: opListKeys},
	{Method: http.MethodGet, Path: kmsBase + "/keys/{keyId}", Op: opGetKey},
	{Method: http.MethodPut, Path: kmsBase + "/keys/{keyId}", Op: opUpdateKey},
	{Method: http.MethodPost, Path: kmsBase + "/keys/{keyId}/actions/enable", Op: opEnableKey},
	{Method: http.MethodPost, Path: kmsBase + "/keys/{keyId}/actions/disable", Op: opDisableKey},
	{Method: http.MethodPost, Path: kmsBase + "/keys/{keyId}/actions/scheduleDeletion", Op: opScheduleKeyDel},
	{Method: http.MethodPost, Path: kmsBase + "/keys/{keyId}/actions/cancelDeletion", Op: opCancelKeyDel},
	{Method: http.MethodPost, Path: kmsBase + "/keys/{keyId}/actions/changeCompartment", Op: opChangeKeyComp},
	{Method: http.MethodPost, Path: kmsBase + "/keys/{keyId}/keyVersions/import", Op: opImportKeyVersion},
	{Method: http.MethodPost, Path: kmsBase + "/keys/{keyId}/keyVersions", Op: opCreateKeyVersion},
	{Method: http.MethodGet, Path: kmsBase + "/keys/{keyId}/keyVersions", Op: opListKeyVersions},
	{Method: http.MethodGet, Path: kmsBase + "/keys/{keyId}/keyVersions/{keyVersionId}", Op: opGetKeyVersion},
	{
		Method: http.MethodPost,
		Path:   kmsBase + "/keys/{keyId}/keyVersions/{keyVersionId}/actions/scheduleDeletion",
		Op:     opScheduleKeyVerDel,
	},
	{
		Method: http.MethodPost,
		Path:   kmsBase + "/keys/{keyId}/keyVersions/{keyVersionId}/actions/cancelDeletion",
		Op:     opCancelKeyVerDel,
	},
	{Method: http.MethodGet, Path: kmsBase + "/wrappingKeys", Op: opGetWrappingKey},
	{Method: http.MethodPost, Path: kmsBase + "/encrypt", Op: opEncrypt},
	{Method: http.MethodPost, Path: kmsBase + "/decrypt", Op: opDecrypt},
	{Method: http.MethodPost, Path: kmsBase + "/generateDataEncryptionKey", Op: opGenerateDEK},
	{Method: http.MethodPost, Path: kmsBase + "/sign", Op: opSign},
	{Method: http.MethodPost, Path: kmsBase + "/verify", Op: opVerify},
	{Method: http.MethodPost, Path: kmsBase + "/exportKey", Op: opExportKey},
	{Method: http.MethodPost, Path: kmsBase + "/vaults", Op: opCreateVault},
	{Method: http.MethodGet, Path: kmsBase + "/vaults", Op: opListVaults},
	{Method: http.MethodGet, Path: kmsBase + "/vaults/{vaultId}", Op: opGetVault},
	{Method: http.MethodPut, Path: kmsBase + "/vaults/{vaultId}", Op: opUpdateVault},
	{Method: http.MethodGet, Path: kmsBase + "/vaults/{vaultId}/usage", Op: opGetVaultUsage},
	{Method: http.MethodPost, Path: kmsBase + "/vaults/{vaultId}/actions/scheduleDeletion", Op: opScheduleVaultDel},
	{Method: http.MethodPost, Path: kmsBase + "/vaults/{vaultId}/actions/cancelDeletion", Op: opCancelVaultDel},
	{Method: http.MethodPost, Path: kmsBase + "/vaults/{vaultId}/actions/changeCompartment", Op: opChangeVaultComp},
	{Method: http.MethodPost, Path: kmsBase + "/secrets", Op: opCreateSecret},
	{Method: http.MethodGet, Path: kmsBase + "/secrets", Op: opListSecrets},
	{Method: http.MethodGet, Path: kmsBase + "/secrets/{secretId}", Op: opGetSecret},
	{Method: http.MethodPut, Path: kmsBase + "/secrets/{secretId}", Op: opUpdateSecret},
	{Method: http.MethodPost, Path: kmsBase + "/secrets/{secretId}/actions/scheduleDeletion", Op: opScheduleSecretDel},
	{Method: http.MethodPost, Path: kmsBase + "/secrets/{secretId}/actions/cancelDeletion", Op: opCancelSecretDel},
	{Method: http.MethodPost, Path: kmsBase + "/secrets/{secretId}/actions/changeCompartment", Op: opChangeSecretComp},
	{Method: http.MethodGet, Path: kmsBase + "/secrets/{secretId}/versions", Op: opListSecretVersions},
	{Method: http.MethodGet, Path: kmsBase + "/secrets/{secretId}/version/{versionNumber}", Op: opGetSecretVersion},
	{
		Method: http.MethodPost,
		Path:   kmsBase + "/secrets/{secretId}/version/{versionNumber}/actions/scheduleDeletion",
		Op:     opScheduleSecretVDel,
	},
	{
		Method: http.MethodPost,
		Path:   kmsBase + "/secrets/{secretId}/version/{versionNumber}/actions/cancelDeletion",
		Op:     opCancelSecretVDel,
	},
	{Method: http.MethodPost, Path: secretsBase + "/secretbundles/actions/getByName", Op: opGetBundleByName},
	{Method: http.MethodGet, Path: secretsBase + "/secretbundles/actions/getByName", Op: opGetBundleByName},
	{Method: http.MethodGet, Path: secretsBase + "/secretbundles/{secretId}", Op: opGetSecretBundle},
	{Method: http.MethodGet, Path: secretsBase + "/secretbundles/{secretId}/versions", Op: opListBundleVersions},
})

type route struct {
	params map[string]string
	op     string
	hint   string
}

// parseRoute maps method, path and Host to an operation and the vault the endpoint addresses.
func parseRoute(r *http.Request) route {
	path := r.URL.EscapedPath()
	rt := route{hint: hostHint(r.Host)}

	switch {
	case strings.HasPrefix(path, "/"+adminPrefix+"/"):
		return route{op: opAdminKeys}
	case strings.HasPrefix(path, "/"+vaultPrefix+"/"):
		rest := strings.TrimPrefix(path, "/"+vaultPrefix+"/")
		id, tail, _ := strings.Cut(rest, "/")
		rt.hint, path = id, "/"+tail
	}

	rt.op, rt.params = router.Match(r.Method, path)
	if rt.op == "" {
		rt.op = opUnknown
	}

	return rt
}

// hostHint extracts "<prefix>" from "<prefix>-management.<domain>[:port]" or "<prefix>-crypto.<domain>".
func hostHint(host string) string {
	label, _, _ := strings.Cut(host, ".")

	for _, suffix := range []string{"-management", "-crypto"} {
		if p, ok := strings.CutSuffix(label, suffix); ok {
			return p
		}
	}

	return ""
}
