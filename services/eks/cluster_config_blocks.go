package eks

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// clusterConfigBlocksBody holds the cluster config members stored and echoed verbatim.
type clusterConfigBlocksBody struct {
	ControlPlaneScalingConfig   jsontext.Value `json:"controlPlaneScalingConfig"`
	KubeAPIServerConfig         jsontext.Value `json:"kubeApiServerConfig"`
	KubeControllerManagerConfig jsontext.Value `json:"kubeControllerManagerConfig"`
	KubeSchedulerConfig         jsontext.Value `json:"kubeSchedulerConfig"`
	OutpostConfig               jsontext.Value `json:"outpostConfig"`
	RemoteNetworkConfig         jsontext.Value `json:"remoteNetworkConfig"`
	ZonalShiftConfig            jsontext.Value `json:"zonalShiftConfig"`
}

// blocks returns the supplied members keyed by wire name; outpostConfig is create-only.
func (b clusterConfigBlocksBody) blocks(forCreate bool) map[string]jsontext.Value {
	all := map[string]jsontext.Value{
		"controlPlaneScalingConfig":   b.ControlPlaneScalingConfig,
		"kubeApiServerConfig":         b.KubeAPIServerConfig,
		"kubeControllerManagerConfig": b.KubeControllerManagerConfig,
		"kubeSchedulerConfig":         b.KubeSchedulerConfig,
		"remoteNetworkConfig":         b.RemoteNetworkConfig,
		"zonalShiftConfig":            b.ZonalShiftConfig,
	}
	if forCreate {
		all["outpostConfig"] = b.OutpostConfig
	}

	for k, v := range all {
		if len(v) == 0 || string(v) == jsonNull {
			delete(all, k)
		}
	}

	if len(all) == 0 {
		return nil
	}

	return all
}

// validTiers are types.ProvisionedControlPlaneTier values.
func validTiers() []string {
	return []string{"standard", "tier-xl", "tier-2xl", "tier-4xl", "tier-8xl"}
}

func validateConfigBlocks(blocks map[string]jsontext.Value) error {
	for key, raw := range blocks {
		var obj map[string]jsontext.Value
		if err := json.Unmarshal(raw, &obj, jsonx.V1()); err != nil {
			return fmt.Errorf("%w: %s must be an object", ErrValidation, key)
		}
	}

	if raw, ok := blocks["controlPlaneScalingConfig"]; ok {
		var cfg struct {
			Tier string `json:"tier"`
		}
		_ = json.Unmarshal(raw, &cfg, jsonx.V1())

		if cfg.Tier != "" && !slices.Contains(validTiers(), cfg.Tier) {
			return fmt.Errorf("%w: controlPlaneScalingConfig.tier must be one of %s",
				ErrValidation, strings.Join(validTiers(), ", "))
		}
	}

	return nil
}

func validateEncryptionConfigs(configs []EncryptionConfig) error {
	for _, cfg := range configs {
		if keyARN := cfg.Provider["keyArn"]; keyARN != "" && !isKMSARN(keyARN) {
			return fmt.Errorf("%w: provider.keyArn %q is not a valid KMS key ARN", ErrValidation, keyARN)
		}
	}

	return nil
}

// configBlockParamType maps a block's wire key to its types.UpdateParamType.
func configBlockParamType(key string) string {
	return strings.ToUpper(key[:1]) + key[1:]
}

func cloneConfigBlocks(src map[string]jsontext.Value) map[string]jsontext.Value {
	if src == nil {
		return nil
	}

	return maps.Clone(src)
}

// applyConfigBlocksLocked replaces the stored blocks and returns the update params. Caller holds b.mu.
func applyConfigBlocksLocked(c *Cluster, blocks map[string]jsontext.Value) []UpdateParam {
	if len(blocks) == 0 {
		return nil
	}

	merged := cloneConfigBlocks(c.ConfigBlocks)
	if merged == nil {
		merged = make(map[string]jsontext.Value, len(blocks))
	}

	var params []UpdateParam

	for _, key := range slices.Sorted(maps.Keys(blocks)) {
		if key == "controlPlaneScalingConfig" {
			params = append(params,
				UpdateParam{Type: "PreviousTier", Value: blockTier(merged[key])},
				UpdateParam{Type: "UpdatedTier", Value: blockTier(blocks[key])},
			)
		} else {
			params = append(params, UpdateParam{Type: configBlockParamType(key), Value: string(blocks[key])})
		}

		merged[key] = slices.Clone(blocks[key])
	}

	c.ConfigBlocks = merged

	return params
}

func blockTier(raw jsontext.Value) string {
	var cfg struct {
		Tier string `json:"tier"`
	}
	_ = json.Unmarshal(raw, &cfg, jsonx.V1())

	return cfg.Tier
}
