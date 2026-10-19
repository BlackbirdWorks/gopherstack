package gcpcloudtasks

import (
	"regexp"
	"slices"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/protobuf/proto"
)

var kmsKeyRe = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/[^/]+$`)

func cloneCmek(c *cloudtaskspb.CmekConfig) *cloudtaskspb.CmekConfig {
	out, _ := proto.Clone(c).(*cloudtaskspb.CmekConfig)

	return out
}

// GetCmekConfig returns a location's CMEK config; an unconfigured location reports an empty kms_key.
func (b *InMemoryBackend) GetCmekConfig(name string) (*cloudtaskspb.CmekConfig, error) {
	if err := parseCmekName(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetCmekConfig")
	defer b.mu.RUnlock()

	if c, ok := b.cmek[name]; ok {
		return cloneCmek(c), nil
	}

	return &cloudtaskspb.CmekConfig{Name: name}, nil
}

// UpdateCmekConfig sets a location's customer-managed encryption key; the key is recorded, not used.
func (b *InMemoryBackend) UpdateCmekConfig(
	req *cloudtaskspb.UpdateCmekConfigRequest,
) (*cloudtaskspb.CmekConfig, error) {
	in := req.GetCmekConfig()
	if in == nil {
		return nil, errInvalid("cmek_config must be specified.")
	}

	if err := parseCmekName(in.GetName()); err != nil {
		return nil, err
	}

	if paths := req.GetUpdateMask().GetPaths(); len(paths) > 0 && !slices.Equal(paths, []string{"kms_key"}) {
		return nil, errInvalid("update_mask may only contain kms_key.")
	}

	if in.GetKmsKey() != "" && !kmsKeyRe.MatchString(in.GetKmsKey()) {
		return nil, errInvalid("kms_key %q must be a Cloud KMS crypto key name.", in.GetKmsKey())
	}

	b.mu.Lock("UpdateCmekConfig")
	defer b.mu.Unlock()

	b.cmek[in.GetName()] = cloneCmek(in)

	return cloneCmek(in), nil
}
