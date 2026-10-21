package gcplogging

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var kmsKeyRe = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/[^/]+$`)

func resourceParent(name, suffix string) (string, error) {
	parent, ok := strings.CutSuffix(name, "/"+suffix)
	if !ok || checkParent(parent) != nil {
		return "", errInvalid("Invalid name [%s]: expected [PARENT]/%s.", name, suffix)
	}

	return parent, nil
}

func logServiceAccount(parent string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(parent))

	return fmt.Sprintf("service-%012d@gcp-sa-logging.iam.gserviceaccount.com", h.Sum64()%projectNumberMod)
}

func checkKMSKey(key string) error {
	if key != "" && !kmsKeyRe.MatchString(key) {
		return errInvalid("Invalid kms_key_name [%s]: expected projects/P/locations/L/keyRings/R/cryptoKeys/K.", key)
	}

	return nil
}

// GetCmekSettings returns the stored CMEK settings, or empty defaults.
func (b *InMemoryBackend) GetCmekSettings(name string) (*loggingpb.CmekSettings, error) {
	parent, err := resourceParent(name, "cmekSettings")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetCmekSettings")
	defer b.mu.RUnlock()

	if c, ok := b.cmek[parent]; ok {
		out, _ := proto.Clone(c).(*loggingpb.CmekSettings)

		return out, nil
	}

	return &loggingpb.CmekSettings{Name: name}, nil
}

// UpdateCmekSettings stores the key used for new log buckets; the key is not validated against Cloud KMS.
func (b *InMemoryBackend) UpdateCmekSettings(
	req *loggingpb.UpdateCmekSettingsRequest,
) (*loggingpb.CmekSettings, error) {
	parent, err := resourceParent(req.GetName(), "cmekSettings")
	if err != nil {
		return nil, err
	}

	in := req.GetCmekSettings()
	if in == nil {
		return nil, errInvalid("cmek_settings is required.")
	}

	if err = checkKMSKey(in.GetKmsKeyName()); err != nil {
		return nil, err
	}

	if err = checkMask(req.GetUpdateMask(), "kms_key_name", "kms_key_version_name"); err != nil {
		return nil, err
	}

	next := &loggingpb.CmekSettings{Name: req.GetName(), KmsKeyName: in.GetKmsKeyName()}
	if next.GetKmsKeyName() != "" {
		next.ServiceAccountId = logServiceAccount(parent)
	}

	b.mu.Lock("UpdateCmekSettings")
	b.cmek[parent] = next
	b.mu.Unlock()

	out, _ := proto.Clone(next).(*loggingpb.CmekSettings)

	return out, nil
}

func checkMask(mask *fieldmaskpb.FieldMask, allowed ...string) error {
	for _, p := range mask.GetPaths() {
		if !slices.Contains(allowed, p) {
			return errInvalid("Invalid update_mask path [%s]: supported paths are %s.", p, strings.Join(allowed, ", "))
		}
	}

	return nil
}

// GetSettings returns the stored settings, or defaults.
func (b *InMemoryBackend) GetSettings(name string) (*loggingpb.Settings, error) {
	parent, err := resourceParent(name, "settings")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetSettings")
	defer b.mu.RUnlock()

	if s, ok := b.settings[parent]; ok {
		out, _ := proto.Clone(s).(*loggingpb.Settings)

		return out, nil
	}

	return &loggingpb.Settings{Name: name}, nil
}

// UpdateSettings stores organization or folder defaults; disable_default_sink disables the _Default sink.
func (b *InMemoryBackend) UpdateSettings(req *loggingpb.UpdateSettingsRequest) (*loggingpb.Settings, error) {
	parent, err := resourceParent(req.GetName(), "settings")
	if err != nil {
		return nil, err
	}

	in := req.GetSettings()
	if in == nil {
		return nil, errInvalid("settings is required.")
	}

	if err = checkKMSKey(in.GetKmsKeyName()); err != nil {
		return nil, err
	}

	if err = checkMask(req.GetUpdateMask(), "kms_key_name", "storage_location", "disable_default_sink"); err != nil {
		return nil, err
	}

	b.seed(parent)
	b.mu.Lock("UpdateSettings")
	defer b.mu.Unlock()

	cur := &loggingpb.Settings{Name: req.GetName()}
	if s, ok := b.settings[parent]; ok {
		cur, _ = proto.Clone(s).(*loggingpb.Settings)
	}

	applySettingsMask(cur, in, req.GetUpdateMask().GetPaths())

	if cur.GetKmsKeyName() != "" {
		cur.KmsServiceAccountId = logServiceAccount(parent)
	} else {
		cur.KmsServiceAccountId = ""
	}

	b.settings[parent] = cur

	if sink, ok := b.sinks[sinkName(parent, defaultBucketID)]; ok {
		sink.Disabled = cur.GetDisableDefaultSink()
	}

	out, _ := proto.Clone(cur).(*loggingpb.Settings)

	return out, nil
}

func applySettingsMask(dst, src *loggingpb.Settings, paths []string) {
	if len(paths) == 0 {
		paths = []string{"kms_key_name", "storage_location", "disable_default_sink"}
	}

	for _, p := range paths {
		switch p {
		case "kms_key_name":
			dst.KmsKeyName = src.GetKmsKeyName()
		case "storage_location":
			dst.StorageLocation = src.GetStorageLocation()
		case "disable_default_sink":
			dst.DisableDefaultSink = src.GetDisableDefaultSink()
		}
	}
}
