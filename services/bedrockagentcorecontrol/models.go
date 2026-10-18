package bedrockagentcorecontrol

import "time"

// Resource is one AgentCore control-plane resource. Doc holds the caller-supplied members verbatim
// (JSON-decoded); server-assigned identity, status, timestamps and tags live in typed fields.
type Resource struct {
	Created    time.Time         `json:"created"`
	Updated    time.Time         `json:"updated"`
	SettleAt   time.Time         `json:"settleAt"`
	Doc        map[string]any    `json:"doc"`
	Tags       map[string]string `json:"tags,omitempty"`
	Aux        map[string]any    `json:"aux,omitempty"`
	Account    string            `json:"account"`
	Region     string            `json:"region"`
	Kind       string            `json:"kind"`
	Parent     string            `json:"parent,omitempty"`
	ParentARN  string            `json:"parentArn,omitempty"`
	ID         string            `json:"id"`
	ARN        string            `json:"arn,omitempty"`
	Status     string            `json:"status,omitempty"`
	Transition string            `json:"transition,omitempty"`
	Versions   []Version         `json:"versions,omitempty"`
	Deleting   bool              `json:"deleting,omitempty"`
	System     bool              `json:"system,omitempty"`
}

// Version is an immutable snapshot taken on create and on every update of a versioned resource.
type Version struct {
	Created time.Time      `json:"created"`
	Doc     map[string]any `json:"doc"`
	ID      string         `json:"id"`
	Branch  string         `json:"branch,omitempty"`
	Parents []string       `json:"parents,omitempty"`
}

// ResourcePolicy is the resource-based policy attached to a runtime, endpoint or gateway ARN.
type ResourcePolicy struct {
	ARN    string `json:"arn"`
	Policy string `json:"policy"`
}

// TokenVault is the per-account token vault configuration.
type TokenVault struct {
	Modified time.Time      `json:"modified"`
	KMS      map[string]any `json:"kms"`
	Key      string         `json:"key"`
}

func resourceKey(r *Resource) string {
	return r.Account + "|" + r.Region + "|" + r.Kind + "|" + r.Parent + "|" + r.ID
}

func makeKey(account, region, kind, parent, id string) string {
	return account + "|" + region + "|" + kind + "|" + parent + "|" + id
}
