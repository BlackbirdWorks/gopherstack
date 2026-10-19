package replicator

import (
	"regexp"
	"strings"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

var arnPattern = regexp.MustCompile(`arn:aws[a-z-]*:[a-z0-9-]+:[a-z0-9-]*:[0-9]{12}:[^\s"',\\]+`)

// Remapper rewrites source account, region and replicated KMS key ids to target values.
type Remapper struct {
	keys       map[string]string
	srcAccount string
	dstAccount string
	srcRegion  string
	dstRegion  string
}

// NewRemapper builds a Remapper. An empty source account disables account remapping.
func NewRemapper(srcAccount, srcRegion, dstAccount, dstRegion string) *Remapper {
	return &Remapper{
		srcAccount: srcAccount, srcRegion: srcRegion,
		dstAccount: dstAccount, dstRegion: dstRegion,
		keys: map[string]string{},
	}
}

// MapKey records that source key id src was replicated as dst.
func (m *Remapper) MapKey(src, dst string) { m.keys[src] = dst }

// ARN remaps one ARN; non-ARNs and foreign-account ARNs are returned unchanged.
func (m *Remapper) ARN(s string) string {
	p, err := awsarn.Parse(s)
	if err != nil {
		return s
	}

	if m.srcAccount == "" || p.AccountID != m.srcAccount {
		return s
	}

	p.AccountID = m.dstAccount
	p.Partition = arn.PartitionForRegion(m.dstRegion)

	if p.Region != "" && p.Region == m.srcRegion {
		p.Region = m.dstRegion
	}

	if p.Service == "kms" {
		if id, ok := strings.CutPrefix(p.Resource, "key/"); ok {
			if dst, found := m.keys[id]; found {
				p.Resource = "key/" + dst
			}
		}
	}

	return p.String()
}

// Text remaps every ARN embedded in s (policy documents, JSON attributes) and quoted bare account ids.
func (m *Remapper) Text(s string) string {
	if s == "" || m.srcAccount == "" {
		return s
	}

	s = arnPattern.ReplaceAllStringFunc(s, m.ARN)

	return strings.ReplaceAll(s, `"`+m.srcAccount+`"`, `"`+m.dstAccount+`"`)
}

// KeyRef maps a KMS key reference (id, key ARN or alias) to its target form. Aliases pass
// through (the kms type replicates them); AWS-managed aliases and unreplicated keys report false.
func (m *Remapper) KeyRef(ref string) (string, bool) {
	if strings.HasPrefix(ref, "alias/aws/") {
		return "", false
	}

	if strings.HasPrefix(ref, "alias/") {
		return ref, true
	}

	if p, err := awsarn.Parse(ref); err == nil {
		id, ok := strings.CutPrefix(p.Resource, "key/")
		if !ok {
			return "", false
		}

		if _, found := m.keys[id]; !found {
			return "", false
		}

		return m.ARN(ref), true
	}

	dst, found := m.keys[ref]

	return dst, found
}
