package lambda

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// parseLayerARN extracts the layer name and version number from a layer version ARN.
// Expected format: arn:aws:lambda:{region}:{account}:layer:{name}:{version}
// Returns empty name and 0 version if the ARN is not in the expected format.
func parseLayerARN(layerVersionARN string) (string, int64) {
	// Split on ":" — the resource part is "layer:{name}:{version}".
	parts := strings.Split(layerVersionARN, ":")
	// A valid ARN has exactly 8 colon-separated parts.
	const layerARNParts = 8
	if len(parts) != layerARNParts {
		return "", 0
	}

	if parts[5] != "layer" {
		return "", 0
	}

	layerName := parts[6]

	var v int64

	if _, err := fmt.Sscanf(parts[7], "%d", &v); err != nil {
		return "", 0
	}

	return layerName, v
}

// buildLayerARN constructs a Lambda layer ARN.
func (b *InMemoryBackend) buildLayerARN(layerName string) string {
	return arn.Build("lambda", b.region, b.accountID, "layer:"+layerName)
}

// buildLayerVersionARN constructs a Lambda layer version ARN.
func (b *InMemoryBackend) buildLayerVersionARN(layerName string, version int64) string {
	return fmt.Sprintf("%s:%d", b.buildLayerARN(layerName), version)
}

// PublishLayerVersion creates a new immutable version of the named layer.
func (b *InMemoryBackend) PublishLayerVersion(
	input *PublishLayerVersionInput,
) (*PublishLayerVersionOutput, error) {
	if input == nil || input.Content == nil {
		return nil, fmt.Errorf("%w: Content is required", ErrLambdaUnavailable)
	}

	if input.LayerName == "" {
		return nil, fmt.Errorf("%w: LayerName is required", ErrInvalidParameterValue)
	}

	b.mu.Lock("PublishLayerVersion")
	defer b.mu.Unlock()

	b.layerVersionCounters[input.LayerName]++
	version := b.layerVersionCounters[input.LayerName]

	zipData := input.Content.ZipFile
	codeSize := int64(len(zipData))

	lv := &LayerVersion{
		LayerVersionArn:    b.buildLayerVersionARN(input.LayerName, version),
		Description:        input.Description,
		CreatedDate:        time.Now().UTC().Format(time.RFC3339),
		Version:            version,
		CompatibleRuntimes: input.CompatibleRuntimes,
		LicenseInfo:        input.LicenseInfo,
		ZipData:            zipData,
		Content: &LayerVersionContent{
			CodeSize: codeSize,
		},
	}

	b.layers[input.LayerName] = append(b.layers[input.LayerName], lv)

	return &PublishLayerVersionOutput{
		LayerVersionArn:    lv.LayerVersionArn,
		LayerArn:           b.buildLayerARN(input.LayerName),
		Description:        lv.Description,
		CreatedDate:        lv.CreatedDate,
		Content:            lv.Content,
		CompatibleRuntimes: lv.CompatibleRuntimes,
		LicenseInfo:        lv.LicenseInfo,
		Version:            lv.Version,
	}, nil
}

// GetLayerVersion retrieves metadata for a specific layer version.
func (b *InMemoryBackend) GetLayerVersion(
	layerName string,
	version int64,
) (*GetLayerVersionOutput, error) {
	b.mu.RLock("GetLayerVersion")
	defer b.mu.RUnlock()

	versions, ok := b.layers[layerName]
	if !ok || len(versions) == 0 {
		return nil, ErrLayerNotFound
	}

	for _, lv := range versions {
		if lv.Version == version {
			return &GetLayerVersionOutput{
				LayerVersionArn:    lv.LayerVersionArn,
				LayerArn:           b.buildLayerARN(layerName),
				Description:        lv.Description,
				CreatedDate:        lv.CreatedDate,
				Content:            lv.Content,
				CompatibleRuntimes: lv.CompatibleRuntimes,
				LicenseInfo:        lv.LicenseInfo,
				Version:            lv.Version,
			}, nil
		}
	}

	return nil, ErrLayerVersionNotFound
}

// ListLayers returns a paginated summary of all layers with their latest version.
// Marker is an opaque cursor; maxItems uses lambdaDefaultMaxItems when zero.
func (b *InMemoryBackend) ListLayers(compatibleRuntime, marker string, maxItems int) page.Page[*Layer] {
	b.mu.RLock("ListLayers")
	defer b.mu.RUnlock()

	result := make([]*Layer, 0, len(b.layers))

	names := collections.SortedKeys(b.layers)

	for _, name := range names {
		versions := b.layers[name]
		if len(versions) == 0 {
			continue
		}

		latest := versions[len(versions)-1]

		// Filter by CompatibleRuntime when provided.
		if compatibleRuntime != "" && !slices.Contains(latest.CompatibleRuntimes, compatibleRuntime) {
			continue
		}

		result = append(result, &Layer{
			LayerArn:  b.buildLayerARN(name),
			LayerName: name,
			LatestMatchingVersion: &LayerVersion{
				LayerVersionArn:    latest.LayerVersionArn,
				Description:        latest.Description,
				CreatedDate:        latest.CreatedDate,
				CompatibleRuntimes: latest.CompatibleRuntimes,
				LicenseInfo:        latest.LicenseInfo,
				Version:            latest.Version,
			},
		})
	}

	return page.New(result, marker, maxItems, lambdaDefaultMaxItems)
}

// ListLayerVersions returns all versions of a specific layer in descending order.
func (b *InMemoryBackend) ListLayerVersions(layerName, compatibleRuntime string) ([]*LayerVersion, error) {
	b.mu.RLock("ListLayerVersions")
	defer b.mu.RUnlock()

	versions, ok := b.layers[layerName]
	if !ok {
		return nil, ErrLayerNotFound
	}

	// Return a copy in reverse order (newest first), applying optional runtime filter.
	result := make([]*LayerVersion, 0, len(versions))
	for _, lv := range slices.Backward(versions) {
		if compatibleRuntime != "" && !slices.Contains(lv.CompatibleRuntimes, compatibleRuntime) {
			continue
		}
		result = append(result, &LayerVersion{
			LayerVersionArn:    lv.LayerVersionArn,
			Description:        lv.Description,
			CreatedDate:        lv.CreatedDate,
			CompatibleRuntimes: lv.CompatibleRuntimes,
			LicenseInfo:        lv.LicenseInfo,
			Version:            lv.Version,
		})
	}

	return result, nil
}

// DeleteLayerVersion removes an immutable layer version.
func (b *InMemoryBackend) DeleteLayerVersion(layerName string, version int64) error {
	b.mu.Lock("DeleteLayerVersion")
	defer b.mu.Unlock()

	versions, ok := b.layers[layerName]
	if !ok || len(versions) == 0 {
		return ErrLayerNotFound
	}

	for i, lv := range versions {
		if lv.Version == version {
			b.layers[layerName] = append(versions[:i], versions[i+1:]...)

			// Clean up policy entries for deleted version.
			if b.layerPolicies[layerName] != nil {
				delete(b.layerPolicies[layerName], version)
			}

			return nil
		}
	}

	return ErrLayerVersionNotFound
}

// GetLayerVersionPolicy returns the resource policy for a layer version.
func (b *InMemoryBackend) GetLayerVersionPolicy(
	layerName string,
	version int64,
) (*LayerVersionPolicy, error) {
	b.mu.RLock("GetLayerVersionPolicy")
	defer b.mu.RUnlock()

	// Verify the version exists.
	versions, ok := b.layers[layerName]
	if !ok || len(versions) == 0 {
		return nil, ErrLayerNotFound
	}

	found := false

	for _, lv := range versions {
		if lv.Version == version {
			found = true

			break
		}
	}

	if !found {
		return nil, ErrLayerVersionNotFound
	}

	stmts := b.layerPolicies[layerName][version]

	policy, marshalErr := buildLayerPolicy(stmts)
	if marshalErr != nil {
		return nil, marshalErr
	}

	return &LayerVersionPolicy{
		Policy:     policy,
		RevisionID: layerPolicyRevisionID(stmts),
	}, nil
}

// AddLayerVersionPermission adds a permission statement to a layer version's resource policy.
// A duplicate StatementId is rejected with ErrFunctionAlreadyExists
// (ResourceConflictException), matching AddPermission's function-policy behavior.
// When input.RevisionID is non-empty it must match the policy's current
// revision or the call is rejected with ErrPreconditionFailed.
func (b *InMemoryBackend) AddLayerVersionPermission(
	layerName string, version int64, input *AddLayerVersionPermissionInput,
) (*AddLayerVersionPermissionOutput, error) {
	b.mu.Lock("AddLayerVersionPermission")
	defer b.mu.Unlock()

	versions, ok := b.layers[layerName]
	if !ok || len(versions) == 0 {
		return nil, ErrLayerNotFound
	}

	found := false

	for _, lv := range versions {
		if lv.Version == version {
			found = true

			break
		}
	}

	if !found {
		return nil, ErrLayerVersionNotFound
	}

	if b.layerPolicies[layerName] == nil {
		b.layerPolicies[layerName] = make(map[int64]map[string]*LayerVersionStatement)
	}

	if b.layerPolicies[layerName][version] == nil {
		b.layerPolicies[layerName][version] = make(map[string]*LayerVersionStatement)
	}

	stmts := b.layerPolicies[layerName][version]

	if _, exists := stmts[input.StatementID]; exists {
		return nil, ErrFunctionAlreadyExists
	}

	if input.RevisionID != "" && input.RevisionID != layerPolicyRevisionID(stmts) {
		return nil, ErrPreconditionFailed
	}

	stmt := &LayerVersionStatement{
		StatementID: input.StatementID,
		Action:      input.Action,
		Principal:   input.Principal,
	}

	stmts[input.StatementID] = stmt

	stmtJSON, marshalErr := json.Marshal(stmt)
	if marshalErr != nil {
		return nil, marshalErr
	}

	return &AddLayerVersionPermissionOutput{
		Statement:  string(stmtJSON),
		RevisionID: layerPolicyRevisionID(stmts),
	}, nil
}

// RemoveLayerVersionPermission removes a permission statement from a layer version's resource policy.
// When revisionID is non-empty it must match the policy's current revision or
// the call is rejected with ErrPreconditionFailed without mutating the policy.
func (b *InMemoryBackend) RemoveLayerVersionPermission(
	layerName string,
	version int64,
	statementID string,
	revisionID string,
) error {
	b.mu.Lock("RemoveLayerVersionPermission")
	defer b.mu.Unlock()

	versions, ok := b.layers[layerName]
	if !ok || len(versions) == 0 {
		return ErrLayerNotFound
	}

	found := false

	for _, lv := range versions {
		if lv.Version == version {
			found = true

			break
		}
	}

	if !found {
		return ErrLayerVersionNotFound
	}

	if b.layerPolicies[layerName] == nil || b.layerPolicies[layerName][version] == nil {
		return nil
	}

	if revisionID != "" && revisionID != layerPolicyRevisionID(b.layerPolicies[layerName][version]) {
		return ErrPreconditionFailed
	}

	delete(b.layerPolicies[layerName][version], statementID)

	return nil
}

// layerPolicyRevisionID derives a stable opaque revision identifier for a
// layer version's resource policy from its current set of statement IDs, the
// same content-hash approach policyRevisionID (permissions.go) uses for
// function policies: statement content is immutable once added (no
// UpdateLayerVersionPermission op exists — a StatementId can only be added
// once and then removed), so hashing the sorted StatementId set detects every
// real mutation without needing separate persisted revision state.
func layerPolicyRevisionID(stmts map[string]*LayerVersionStatement) string {
	if len(stmts) == 0 {
		return ""
	}

	ids := collections.SortedKeys(stmts)

	// Content digest over sorted policy statement IDs to derive a revision ID.
	// The input is a list of statement identifiers, never a credential, and the
	// algorithm is SHA-256.
	//
	// CodeQL flags this as go/weak-sensitive-data-hashing; alert #248 is
	// dismissed as a false positive. Inline `codeql[...]` comments do NOT
	// suppress Code Scanning alerts (that is legacy LGTM syntax), so do not add
	// one here expecting it to work -- dismiss via the API or UI instead.
	h := sha256.Sum256([]byte(strings.Join(ids, "\x00")))

	return hex.EncodeToString(h[:])
}

// buildLayerPolicy serialises a map of statements to a JSON IAM policy document string.
func buildLayerPolicy(stmts map[string]*LayerVersionStatement) (string, error) {
	type policyDocument struct {
		Version   string              `json:"Version"`
		Statement []map[string]string `json:"Statement"`
	}

	statements := make([]map[string]string, 0, len(stmts))

	stmtIDs := collections.SortedKeys(stmts)

	for _, sid := range stmtIDs {
		s := stmts[sid]
		statements = append(statements, map[string]string{
			"Sid":       s.StatementID,
			"Effect":    "Allow",
			"Principal": s.Principal,
			"Action":    s.Action,
		})
	}

	doc := policyDocument{
		Version:   "2012-10-17",
		Statement: statements,
	}

	data, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// GetLayerVersionByArn retrieves a layer version by its full ARN.
func (b *InMemoryBackend) GetLayerVersionByArn(
	layerVersionARN string,
) (*GetLayerVersionOutput, error) {
	layerName, version := parseLayerARN(layerVersionARN)
	if layerName == "" || version == 0 {
		return nil, ErrLayerVersionNotFound
	}

	return b.GetLayerVersion(layerName, version)
}
