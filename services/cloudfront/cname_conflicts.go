package cloudfront

import (
	"encoding/xml"
	"fmt"
	"slices"
	"strings"
)

type configAliasesXML struct {
	CNAMEs []string `xml:"Aliases>Items>CNAME"`
}

// rawConfigAliases returns the lowercased CNAMEs declared in a raw DistributionConfig.
func rawConfigAliases(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}

	var cfg configAliasesXML
	if err := xml.Unmarshal(raw, &cfg); err != nil {
		return nil
	}

	out := make([]string, 0, len(cfg.CNAMEs))
	for _, c := range cfg.CNAMEs {
		if c = strings.ToLower(strings.TrimSpace(c)); c != "" {
			out = append(out, c)
		}
	}

	return out
}

// checkCNAMEConflictsLocked rejects aliases in raw already used by a distribution other than selfID.
func (b *InMemoryBackend) checkCNAMEConflictsLocked(selfID string, raw []byte) error {
	aliases := rawConfigAliases(raw)
	if len(aliases) == 0 {
		return nil
	}

	for _, other := range b.distributions.All() {
		if other.ID == selfID {
			continue
		}

		used := append(rawConfigAliases(other.RawConfig), b.distributionAliases[other.ID]...)

		for _, a := range aliases {
			if slices.Contains(used, a) {
				return fmt.Errorf(
					"%w: One or more of the CNAMEs you provided are already associated with a different resource",
					ErrCNAMEAlreadyExists,
				)
			}
		}
	}

	return nil
}
