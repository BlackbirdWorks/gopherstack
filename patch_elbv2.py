import re

with open("services/elbv2/persistence.go", "r") as f:
    c = f.read()

# Struct
c = c.replace('	TrustStores   map[string]*TrustStore   `json:"trustStores"`',
              '	TrustStores   map[string]*TrustStore   `json:"trustStores"`\n\tTargetReadyAt map[string]map[string]time.Time `json:"targetReadyAt"`\n\tRuleCounter   int `json:"ruleCounter"`')

# Snapshot
c = c.replace('		TrustStores:   b.trustStores,',
              '		TrustStores:   b.trustStores,\n\t\tTargetReadyAt: b.targetReadyAt,\n\t\tRuleCounter:   b.ruleCounter,')

# Restore
c = c.replace('	b.trustStores = snap.TrustStores',
              '	b.trustStores = snap.TrustStores\n\tb.targetReadyAt = snap.TargetReadyAt\n\tb.ruleCounter = snap.RuleCounter')

with open("services/elbv2/persistence.go", "w") as f:
    f.write(c)

