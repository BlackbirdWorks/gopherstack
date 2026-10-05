package quicksight

import (
	"fmt"
	"maps"

	sdktypes "github.com/aws/aws-sdk-go-v2/service/quicksight/types"
)

func dataSourcePassthroughKeys() []string {
	return []string{"DataSourceParameters", "AlternateDataSourceParameters", "SslProperties", "VpcConnectionProperties"}
}

func dataSetPassthroughKeys() []string {
	return []string{
		"ColumnGroups", "DataPrepConfiguration", "DataSetUsageConfiguration", "DatasetParameters",
		"FieldFolders", "PerformanceConfiguration", "SemanticModelConfiguration",
	}
}

const keySecretArn = "SecretArn"

// DataSourceOptions holds CreateDataSource/UpdateDataSource members stored and echoed verbatim.
type DataSourceOptions struct {
	Config     map[string]any
	FolderArns []string
}

// DataSetOptions holds CreateDataSet/UpdateDataSet members stored and echoed verbatim.
type DataSetOptions struct {
	Config     map[string]any
	FolderArns []string
}

// passthroughFromBody copies the named request members, deep-cloned so the request body stays untouched.
func passthroughFromBody(body map[string]any, keys []string) map[string]any {
	var out map[string]any

	for _, k := range keys {
		v, ok := body[k]
		if !ok {
			continue
		}

		if out == nil {
			out = make(map[string]any, len(keys))
		}

		out[k] = cloneJSONValue(v)
	}

	return out
}

// dataSourceOptionsFromBody reads the verbatim members, the secret ARN of Credentials and FolderArns.
func dataSourceOptionsFromBody(body map[string]any) DataSourceOptions {
	opts := DataSourceOptions{
		Config:     passthroughFromBody(body, dataSourcePassthroughKeys()),
		FolderArns: stringsFromBody(body, "FolderArns"),
	}

	if creds := mapField(body, "Credentials"); creds != nil {
		if secret, _ := creds[keySecretArn].(string); secret != "" {
			if opts.Config == nil {
				opts.Config = make(map[string]any, 1)
			}

			opts.Config[keySecretArn] = secret
		}
	}

	return opts
}

func dataSetOptionsFromBody(body map[string]any) DataSetOptions {
	return DataSetOptions{
		Config:     passthroughFromBody(body, dataSetPassthroughKeys()),
		FolderArns: stringsFromBody(body, "FolderArns"),
	}
}

func validateDataSourceType(t string) error {
	if t == "" {
		return nil
	}

	for _, v := range sdktypes.DataSourceType("").Values() {
		if string(v) == t {
			return nil
		}
	}

	return fmt.Errorf("%w: unknown data source type %q", ErrValidation, t)
}

// addToFoldersLocked records memberID as a member of every folder in folderArns; callers hold b.mu.
func (b *InMemoryBackend) addToFoldersLocked(accountID, memberType, memberID string, folderArns []string) error {
	for _, folderArn := range folderArns {
		folderID := folderIDFromArn(folderArn)
		if folderID == "" {
			return fmt.Errorf("%w: %q is not a folder ARN", ErrValidation, folderArn)
		}

		if !b.folders.Has(folderKey(accountID, folderID)) {
			return ErrFolderNotFound
		}
	}

	for _, folderArn := range folderArns {
		b.folderMembers.Put(&storedFolderMember{
			FolderID: folderIDFromArn(folderArn), MemberID: memberID, MemberType: memberType,
		})
	}

	return nil
}

// mergeConfig overlays update onto base, keeping members the update omits.
func mergeConfig(base, update map[string]any) map[string]any {
	if len(update) == 0 {
		return base
	}

	out := make(map[string]any, len(base)+len(update))
	maps.Copy(out, base)
	maps.Copy(out, update)

	return out
}

// CheckFolderArns reports whether every ARN names an existing folder.
func (b *InMemoryBackend) CheckFolderArns(accountID string, folderArns []string) error {
	b.mu.RLock("CheckFolderArns")
	defer b.mu.RUnlock()

	for _, folderArn := range folderArns {
		folderID := folderIDFromArn(folderArn)
		if folderID == "" {
			return fmt.Errorf("%w: %q is not a folder ARN", ErrValidation, folderArn)
		}

		if !b.folders.Has(folderKey(accountID, folderID)) {
			return ErrFolderNotFound
		}
	}

	return nil
}

// AddToFolders records memberID as a member of every folder in folderArns.
func (b *InMemoryBackend) AddToFolders(accountID, memberType, memberID string, folderArns []string) error {
	b.mu.Lock("AddToFolders")
	defer b.mu.Unlock()

	return b.addToFoldersLocked(accountID, memberType, memberID, folderArns)
}
