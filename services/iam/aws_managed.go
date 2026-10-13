package iam

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

//go:embed data/aws_managed_policies.json
var awsManagedPoliciesJSON []byte

const awsManagedPolicyIDLen = 17

type awsManagedFile struct {
	Policies []awsManagedEntry `json:"policies"`
}

type awsManagedEntry struct {
	Name             string          `json:"name"`
	Path             string          `json:"path"`
	PolicyID         string          `json:"policyId"`
	DefaultVersionID string          `json:"defaultVersionId"`
	CreateDate       time.Time       `json:"createDate"`
	UpdateDate       time.Time       `json:"updateDate"`
	Document         json.RawMessage `json:"document"`
}

type awsManagedCatalog struct {
	byARN   map[string]Policy
	ordered []Policy
}

//nolint:gochecknoglobals // read-only catalog parsed once from embedded data
var loadAWSManagedCatalog = sync.OnceValue(func() *awsManagedCatalog {
	cat, err := parseAWSManagedCatalog(awsManagedPoliciesJSON)
	if err != nil {
		panic(err)
	}

	return cat
})

func parseAWSManagedCatalog(raw []byte) (*awsManagedCatalog, error) {
	var file awsManagedFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("iam: parse AWS managed policy catalog: %w", err)
	}

	cat := &awsManagedCatalog{
		byARN:   make(map[string]Policy, len(file.Policies)),
		ordered: make([]Policy, 0, len(file.Policies)),
	}

	for _, e := range file.Policies {
		var doc bytes.Buffer
		if err := json.Compact(&doc, e.Document); err != nil {
			return nil, fmt.Errorf("iam: AWS managed policy %q document: %w", e.Name, err)
		}

		arn := awsManagedPolicyPrefix + strings.TrimPrefix(e.Path, "/") + e.Name
		if e.PolicyID == "" {
			e.PolicyID = deriveAWSManagedPolicyID(arn)
		}

		pol := Policy{
			PolicyName:       e.Name,
			PolicyID:         e.PolicyID,
			Arn:              arn,
			Path:             e.Path,
			PolicyDocument:   doc.String(),
			DefaultVersionID: e.DefaultVersionID,
			CreateDate:       e.CreateDate.UTC(),
			UpdateDate:       e.UpdateDate.UTC(),
			IsAttachable:     true,
		}
		cat.byARN[arn] = pol
		cat.ordered = append(cat.ordered, pol)
	}

	sort.Slice(cat.ordered, func(i, j int) bool { return cat.ordered[i].Arn < cat.ordered[j].Arn })

	return cat, nil
}

func deriveAWSManagedPolicyID(arn string) string {
	sum := sha256.Sum256([]byte(arn))

	return "ANPA" + base32.StdEncoding.EncodeToString(sum[:])[:awsManagedPolicyIDLen]
}

func isAWSManagedPolicyARN(policyArn string) bool {
	return strings.HasPrefix(policyArn, awsManagedPolicyPrefix)
}

// awsManagedPolicyLocked returns the catalog policy for policyArn with its live AttachmentCount.
// Caller must hold b.mu.
func (b *InMemoryBackend) awsManagedPolicyLocked(policyArn string) (Policy, bool) {
	pol, ok := loadAWSManagedCatalog().byARN[policyArn]
	if !ok {
		return Policy{}, false
	}

	pol.AttachmentCount = b.attachmentCountLocked(policyArn)

	return pol, true
}

func (b *InMemoryBackend) attachmentCountLocked(policyArn string) int {
	refs := b.policyAttachments[policyArn]

	return len(refs.users) + len(refs.roles) + len(refs.groups)
}

// ListAWSManagedPolicies returns the seeded AWS managed policies sorted by ARN.
func (b *InMemoryBackend) ListAWSManagedPolicies() []Policy {
	b.mu.RLock("ListAWSManagedPolicies")
	defer b.mu.RUnlock()

	cat := loadAWSManagedCatalog()
	out := make([]Policy, len(cat.ordered))

	for i, pol := range cat.ordered {
		pol.AttachmentCount = b.attachmentCountLocked(pol.Arn)
		out[i] = pol
	}

	return out
}

func rejectAWSManagedMutation(policyArn string) error {
	if isAWSManagedPolicyARN(policyArn) {
		return fmt.Errorf("%w: AWS managed policy %q cannot be modified", ErrAWSManagedPolicyImmutable, policyArn)
	}

	return nil
}
