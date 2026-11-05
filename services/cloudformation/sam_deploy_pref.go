package cloudformation

import (
	"maps"
	"slices"
)

const (
	samCodeDeployApp      = "ServerlessDeploymentApplication"
	samCodeDeployRole     = "CodeDeployServiceRole"
	samCodeDeployCondName = "ServerlessCodeDeployCondition"
	samTypeCDApp          = "AWS::CodeDeploy::Application"
	samTypeCDGroup        = "AWS::CodeDeploy::DeploymentGroup"
	samKeyUpdatePolicy    = "UpdatePolicy"
	samKeyDeployPref      = "DeploymentPreference"
)

func samCodeDeployConfigs() []string {
	return []string{"Canary10Percent5Minutes", "Canary10Percent10Minutes", "Canary10Percent15Minutes",
		"Canary10Percent30Minutes", "Linear10PercentEvery1Minute", "Linear10PercentEvery2Minutes",
		"Linear10PercentEvery3Minutes", "Linear10PercentEvery10Minutes", "AllAtOnce"}
}

// samDeployPref is one function's enabled DeploymentPreference.
type samDeployPref struct {
	dp        map[string]any
	tags      map[string]any
	fnID      string
	condition string
}

// deploymentPreference validates a function's DeploymentPreference and marks its alias with the CodeDeploy
// UpdatePolicy; the CodeDeploy resources themselves are emitted once for all functions.
func (t *samTranslator) deploymentPreference(id string, r, props map[string]any) error {
	dp, enabled, err := t.readDeploymentPreference(id, props)
	if err != nil || !enabled {
		return err
	}
	pref := &samDeployPref{fnID: id, dp: dp, tags: maps.Clone(t.curTags)}
	if pass, _ := t.paramDefault(dp["PassthroughCondition"]).(bool); pass {
		pref.condition = asString(r[samKeyCondition])
	}
	t.deploys = append(t.deploys, pref)
	hooks := asMap(dp["Hooks"])
	update := map[string]any{
		"ApplicationName":     samRef(samCodeDeployApp),
		"DeploymentGroupName": samRef(id + "DeploymentGroup"),
	}
	if v := hooks["PreTraffic"]; v != nil {
		update["BeforeAllowTrafficHook"] = v
	}
	if v := hooks["PostTraffic"]; v != nil {
		update["AfterAllowTrafficHook"] = v
	}
	alias := asMap(t.out[samAliasID(id, props)])
	alias[samKeyUpdatePolicy] = map[string]any{"CodeDeployLambdaAliasUpdate": update}

	return nil
}

// readDeploymentPreference validates the property and reports whether it is enabled.
func (t *samTranslator) readDeploymentPreference(id string, props map[string]any) (map[string]any, bool, error) {
	raw, ok := props[samKeyDeployPref]
	if !ok || raw == nil {
		return nil, false, nil
	}
	dp := maps.Clone(asMap(raw))
	if dp == nil {
		return nil, false, samErr(id, "'DeploymentPreference' must be a map.")
	}
	if enabled, has := dp[samKeyEnabled]; has {
		switch t.paramDefault(enabled) {
		case false, boolFalseText, "False":
			return nil, false, nil
		}
	}
	if _, has := dp[samKeyType]; !has {
		return nil, false, samErr(id, "'DeploymentPreference' is missing required Property 'Type'")
	}
	dp[samKeyType] = t.paramDefault(dp[samKeyType])
	if hooks, has := dp["Hooks"]; has && hooks != nil {
		if _, isMap := hooks.(map[string]any); !isMap {
			return nil, false, samErr(id, "'Hooks' property of 'DeploymentPreference' must be a dictionary")
		}
	}
	if props[samKeyAutoPublish] == nil {
		return nil, false, samErr(id, "'DeploymentPreference' requires AutoPublishAlias property to be specified.")
	}

	return dp, true, nil
}

// finalizeDeployments emits the CodeDeploy application, service role and one deployment group per function.
func (t *samTranslator) finalizeDeployments() error {
	if len(t.deploys) == 0 {
		return nil
	}
	cond := t.deployCondition()
	app := map[string]any{samKeyType: samTypeCDApp, samKeyProps: map[string]any{"ComputePlatform": samDestLambda}}
	if tags := t.deployTags(); tags != nil {
		asMap(app[samKeyProps])[samKeyTags] = tags
	}
	if cond != "" {
		app[samKeyCondition] = cond
	}
	if err := t.putPlain(samCodeDeployApp, app); err != nil {
		return err
	}
	if !t.canSkipDeployRole() {
		role := map[string]any{samKeyType: resTypeIAMRole, samKeyProps: map[string]any{
			samKeyAssumeRole:  servicePrincipalTrust("codedeploy.amazonaws.com"),
			samKeyManagedARNs: toSubs([]any{samManagedARNPrefix + "service-role/AWSCodeDeployRoleForLambda"}),
		}}
		if cond != "" {
			role[samKeyCondition] = cond
		}
		if tags := t.deployTags(); tags != nil {
			asMap(role[samKeyProps])[samKeyTags] = tags
		}
		if err := t.putPlain(samCodeDeployRole, role); err != nil {
			return err
		}
	}
	for _, p := range t.deploys {
		group, err := t.deploymentGroup(p)
		if err != nil {
			return err
		}
		if err = t.putPlain(p.fnID+"DeploymentGroup", group); err != nil {
			return err
		}
	}

	return nil
}

// putPlain adds a resource without the pass-through attributes of whichever SAM resource was last translated.
func (t *samTranslator) putPlain(id string, r map[string]any) error {
	saved := t.pass
	t.pass = nil
	defer func() { t.pass = saved }()

	return t.put(id, r)
}

func (t *samTranslator) canSkipDeployRole() bool {
	return !slices.ContainsFunc(t.deploys, func(p *samDeployPref) bool { return p.dp["Role"] == nil })
}

// deployCondition is the Condition on the shared CodeDeploy resources: set only when every enabled
// preference carries one.
func (t *samTranslator) deployCondition() string {
	var conds []string
	for _, p := range t.deploys {
		if p.condition == "" {
			return ""
		}
		if !slices.Contains(conds, p.condition) {
			conds = append(conds, p.condition)
		}
	}
	if len(conds) == 1 {
		return conds[0]
	}
	slices.Sort(conds)
	or := make([]any, len(conds))
	for i, c := range conds {
		or[i] = map[string]any{samKeyCondition: c}
	}
	t.conditions[samCodeDeployCondName] = map[string]any{samFnOr: or}

	return samCodeDeployCondName
}

// deployTags merges the propagated tags of every enabled preference.
func (t *samTranslator) deployTags() []any {
	merged := map[string]any{}
	for _, p := range t.deploys {
		maps.Copy(merged, p.tags)
	}
	if len(merged) == 0 {
		return nil
	}

	return plainTagList(merged)
}

func (t *samTranslator) deploymentGroup(p *samDeployPref) (map[string]any, error) {
	alarms, hasAlarms, err := deployAlarmConfig(p.fnID, p.dp["Alarms"])
	if err != nil {
		return nil, err
	}
	props := map[string]any{
		"ApplicationName": samRef(samCodeDeployApp),
		"AutoRollbackConfiguration": map[string]any{
			samKeyEnabled: true,
			samKeyEvents:  []any{"DEPLOYMENT_FAILURE", "DEPLOYMENT_STOP_ON_ALARM", "DEPLOYMENT_STOP_ON_REQUEST"},
		},
		"DeploymentConfigName": replaceDeployTypes(p.dp[samKeyType], ""),
		"DeploymentStyle": map[string]any{
			"DeploymentType":   "BLUE_GREEN",
			"DeploymentOption": "WITH_TRAFFIC_CONTROL",
		},
		"ServiceRoleArn": samGetAtt(samCodeDeployRole, attrNameArn),
	}
	if hasAlarms {
		props["AlarmConfiguration"] = alarms
	}
	if p.dp["Role"] != nil {
		props["ServiceRoleArn"] = p.dp["Role"]
	}
	if p.dp["TriggerConfigurations"] != nil {
		props["TriggerConfigurations"] = p.dp["TriggerConfigurations"]
	}
	if len(p.tags) > 0 {
		props[samKeyTags] = plainTagList(p.tags)
	}
	group := map[string]any{samKeyType: samTypeCDGroup, samKeyProps: props}
	if p.condition != "" {
		group[samKeyCondition] = p.condition
	}

	return group, nil
}

func deployAlarmConfig(fnID string, alarms any) (any, bool, error) {
	if alarms == nil || isNoValue(alarms) {
		return nil, false, nil
	}
	if cond, ok := asMap(alarms)[fnIf].([]any); ok && len(asMap(alarms)) == 1 {
		const ifItems = 3
		if len(cond) != ifItems {
			return nil, false, samErr(fnID, "Fn::If requires 3 arguments")
		}
		out := slices.Clone(cond)
		for _, i := range []int{1, 2} {
			cfg, err := buildAlarmConfig(fnID, out[i])
			if err != nil {
				return nil, false, err
			}
			out[i] = cfg
		}

		return map[string]any{fnIf: out}, true, nil
	}
	cfg, err := buildAlarmConfig(fnID, alarms)

	return cfg, err == nil, err
}

func buildAlarmConfig(fnID string, alarms any) (any, error) {
	list, ok := alarms.([]any)
	if !ok {
		return nil, samErr(fnID, "Alarms must be a list")
	}
	if len(list) == 0 || isNoValue(list[0]) {
		return map[string]any{}, nil
	}
	out := make([]any, len(list))
	for i, a := range list {
		out[i] = map[string]any{attrNameName: a}
	}

	return map[string]any{samKeyEnabled: true, "Alarms": out}, nil
}

// replaceDeployTypes turns the predefined configuration names into CodeDeployDefault.Lambda<name>,
// looking through intrinsics.
func replaceDeployTypes(v any, key string) any {
	switch t := v.(type) {
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = replaceDeployTypes(e, "")
		}

		return out
	case map[string]any:
		if isSAMIntrinsic(t) {
			out := make(map[string]any, len(t))
			for k, e := range t {
				out[k] = replaceDeployTypes(e, k)
			}

			return out
		}
	case string:
		if slices.Contains(samCodeDeployConfigs(), t) {
			if key == samKeyFnSub {
				return []any{"CodeDeployDefault.Lambda${ConfigName}", map[string]any{"ConfigName": t}}
			}

			return map[string]any{samKeyFnSub: []any{"CodeDeployDefault.Lambda${ConfigName}",
				map[string]any{"ConfigName": t}}}
		}
	}

	return v
}
