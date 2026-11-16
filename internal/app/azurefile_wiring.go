package app

import (
	"errors"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
	"github.com/blackbirdworks/gopherstack/services/azurestoragevhost"
	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

// wireAzureFile gives Azure Files its Blob copy-source reader, routes the
// ".file." vhost label to it and lets ARM drive its share soft-delete policy.
func wireAzureFile(byName map[string]service.Registerable) {
	fileH, ok := byName["AzureFile"].(*azurefile.Handler)
	if !ok {
		return
	}

	if blobH, blobOk := byName["AzureBlob"].(*azureblob.Handler); blobOk {
		fileH.Blobs = azureBlobReader{backend: blobH.Backend}
		fileH.BlobPort = blobH.Port
	}

	if tableH, tableOk := byName["AzureTable"].(*azuretable.Handler); tableOk {
		fileH.Metrics = azureTableMetrics{backend: tableH.Backend}
	}

	if armH, armOk := byName["AzureARM"].(*azurearm.Handler); armOk {
		if p, found := armH.Registry.ProviderNamed("Microsoft.Storage"); found {
			if sp, isStorage := p.(*azurearm.StorageProvider); isStorage {
				fileH.Backend.AccountSKU = sp.AccountSKUName
			}
		}

		if armH.Issuer != nil {
			fileH.ValidateToken = azureFileTokenValidator(armH.Issuer)
		}

		armH.Registry.SetFileServices(azureFileServices{b: fileH.Backend})
		armH.Registry.SetFileShares(azureFileShares{b: fileH.Backend})
		armH.Registry.SetACIFileShares(aciFileShares{backend: fileH.Backend})
	}

	if vhostH, vhostOk := byName["AzureStorageVHost"].(*azurestoragevhost.Handler); vhostOk {
		vhostH.File = fileH
	}
}

// azureBlobReader exposes blob bytes to Azure Files copy operations.
type azureBlobReader struct {
	backend azureblob.StorageBackend
}

var errBlobBackendUnavailable = errors.New("azure blob backend unavailable")

func (r azureBlobReader) ReadBlob(container, blob, snapshot string) ([]byte, string, error) {
	if r.backend == nil {
		return nil, "", errBlobBackendUnavailable
	}

	info, data, err := r.backend.GetBlobAt(container, blob, snapshot)
	if err != nil {
		return nil, "", err
	}

	return data, info.ContentType, nil
}

// azureTableMetrics stores Azure Files Storage Analytics rows in the Table service.
type azureTableMetrics struct {
	backend azuretable.StorageBackend
}

func (m azureTableMetrics) PutMetricRow(table, partitionKey, rowKey string, props map[string]any) error {
	if err := m.backend.CreateTable(table); err != nil && !errors.Is(err, odatatable.ErrTableAlreadyExists) {
		return err
	}

	ep := make(map[string]odatatable.EntityProperty, len(props))

	for k, v := range props {
		switch n := v.(type) {
		case int64:
			ep[k] = odatatable.EntityProperty{Value: n, Type: odatatable.EdmInt64}
		case float64:
			ep[k] = odatatable.EntityProperty{Value: n, Type: odatatable.EdmDouble}
		default:
			ep[k] = odatatable.EntityProperty{Value: n, Type: odatatable.EdmString}
		}
	}

	_, err := m.backend.ReplaceEntity(table, partitionKey, rowKey, ep, "")

	return err
}

// azureFileServices adapts the Azure Files data plane to services/azurearm.FileServices.
type azureFileServices struct {
	b *azurefile.InMemoryBackend
}

var _ azurearm.FileServices = azureFileServices{}

func (a azureFileServices) SetShareRetention(account string, enabled bool, days int32) {
	a.b.SetShareRetention(account, enabled, days)
}

func (a azureFileServices) ShareRetention(account string) (bool, int32) {
	return a.b.ShareRetention(account)
}

func (a azureFileServices) SetCORS(account string, rules []azurearm.FileCORSRule) error {
	in := make([]azurefile.CORSSetting, 0, len(rules))

	for _, r := range rules {
		in = append(in, azurefile.CORSSetting{
			AllowedOrigins: strings.Join(r.AllowedOrigins, ","), AllowedMethods: strings.Join(r.AllowedMethods, ","),
			AllowedHeaders: strings.Join(r.AllowedHeaders, ","), ExposedHeaders: strings.Join(r.ExposedHeaders, ","),
			MaxAgeInSeconds: r.MaxAgeInSeconds,
		})
	}

	return a.b.SetCORS(account, in)
}

func (a azureFileServices) CORS(account string) []azurearm.FileCORSRule {
	rules := a.b.CORS(account)
	out := make([]azurearm.FileCORSRule, 0, len(rules))

	for _, r := range rules {
		out = append(out, azurearm.FileCORSRule{
			AllowedOrigins: splitList(r.AllowedOrigins), AllowedMethods: splitList(r.AllowedMethods),
			AllowedHeaders: splitList(r.AllowedHeaders), ExposedHeaders: splitList(r.ExposedHeaders),
			MaxAgeInSeconds: r.MaxAgeInSeconds,
		})
	}

	return out
}

func splitList(s string) []string {
	if s == "" {
		return []string{}
	}

	return strings.Split(s, ",")
}

func (a azureFileServices) SetSMB(account string, s azurearm.FileSMBSettings) {
	a.b.SetSMB(account, azurefile.SMBSetting(s))
}

func (a azureFileServices) SMB(account string) azurearm.FileSMBSettings {
	return azurearm.FileSMBSettings(a.b.SMB(account))
}

// azureFileTokenValidator verifies Bearer tokens against the emulator's own Entra issuer.
func azureFileTokenValidator(iss *aadauth.Issuer) func(string) (string, string, error) {
	return func(token string) (string, string, error) {
		claims, err := iss.Parse(token)
		if err != nil {
			return "", "", err
		}

		oid, _ := claims["oid"].(string)
		tid, _ := claims["tid"].(string)

		return oid, tid, nil
	}
}
