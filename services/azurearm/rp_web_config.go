package azurearm

import (
	"context"
	"encoding/xml"
	"fmt"
	"maps"
	"strings"

	"github.com/google/uuid"
)

const (
	webTypeConfig  = "Microsoft.Web/sites/config"
	webTypePolicy  = "Microsoft.Web/sites/basicPublishingCredentialsPolicies"
	webConfigConn  = "connectionstrings"
	webConfigMeta  = "metadata"
	webPolicyFTP   = "ftp"
	webPolicySCM   = "scm"
	webConfigPubCr = "publishingcredentials"
)

// webConfigNames are the sites/config children the provider serves; all but appsettings are stored with the
// site, appsettings live in the data plane because the function host reads them as its environment.
func webConfigNames() []string {
	return []string{
		webConfigWeb, webConfigAppSettings, webConfigConn, webConfigMeta, "authsettings", "authsettingsV2", "logs",
		"slotConfigNames", "pushsettings",
	}
}

func canonicalConfigName(name string) (string, bool) {
	for _, n := range webConfigNames() {
		if strings.EqualFold(n, name) {
			return n, true
		}
	}

	return "", false
}

// webConfigDefaults returns the site's configuration of one kind with defaults filled in. For "web" the
// defaults are those Azure applies to a Linux function app on a consumption or elastic plan.
func webConfigDefaults(s *storedWebSite) map[string]any {
	stored := s.Configs[webConfigWeb]
	linux := strings.Contains(strings.ToLower(s.Kind), "linux")

	out := map[string]any{
		webFieldWorkers:                          1,
		"defaultDocuments":                       []any{"Default.htm", "Default.html", "index.html", "index.htm"},
		"netFrameworkVersion":                    "v4.0",
		webFieldLinuxFx:                          "",
		"requestTracingEnabled":                  false,
		"remoteDebuggingEnabled":                 false,
		"httpLoggingEnabled":                     false,
		"logsDirectorySizeLimit":                 webLogsLimitMB,
		"detailedErrorLoggingEnabled":            false,
		"publishingUsername":                     s.publishUser(),
		"scmType":                                webNone,
		"use32BitWorkerProcess":                  false,
		"webSocketsEnabled":                      false,
		"alwaysOn":                               false,
		"appCommandLine":                         "",
		"managedPipelineMode":                    "Integrated",
		"loadBalancing":                          "LeastRequests",
		"autoHealEnabled":                        false,
		"localMySqlEnabled":                      false,
		"http20Enabled":                          linux,
		"minTlsVersion":                          redisDefaultTLS,
		"scmMinTlsVersion":                       redisDefaultTLS,
		"ftpsState":                              "FtpsOnly",
		"preWarmedInstanceCount":                 0,
		"functionAppScaleLimit":                  0,
		"functionsRuntimeScaleMonitoringEnabled": false,
		"minimumElasticInstanceCount":            0,
		"azureStorageAccounts":                   map[string]any{},
		"scmIpSecurityRestrictionsUseMain":       false,
		"virtualApplications": []any{
			map[string]any{"virtualPath": "/", "physicalPath": `site\wwwroot`, "preloadEnabled": false},
		},
		"ipSecurityRestrictions": []any{map[string]any{
			"ipAddress": "Any", "action": webAllow, "priority": webMaxPriority, fieldName: webAllow + " all",
			webDescription: webAllow + " all access",
		}},
		"scmIpSecurityRestrictions": []any{map[string]any{
			"ipAddress": "Any", "action": webAllow, "priority": webMaxPriority, fieldName: webAllow + " all",
			webDescription: webAllow + " all access",
		}},
	}

	if !isFunctionAppKind(s.Kind) {
		delete(out, "functionAppScaleLimit")
		delete(out, "functionsRuntimeScaleMonitoringEnabled")
		delete(out, "minimumElasticInstanceCount")
	}

	maps.Copy(out, stored)

	return out
}

func configDefaultProps(name string, s *storedWebSite) map[string]any {
	switch name {
	case webConfigWeb:
		return webConfigDefaults(s)
	case "authsettings":
		return map[string]any{
			acrWebhookEnabled:             false,
			"runtimeVersion":              "~1",
			"unauthenticatedClientAction": "RedirectToLoginPage",
		}
	case "authsettingsV2":
		return map[string]any{
			"platform":         map[string]any{acrWebhookEnabled: false, "runtimeVersion": "~1"},
			"globalValidation": map[string]any{"unauthenticatedClientAction": "RedirectToLoginPage"},
		}
	case "logs":
		return map[string]any{
			"applicationLogs": map[string]any{
				"fileSystem": map[string]any{"level": "Off"}, "azureBlobStorage": map[string]any{"level": "Off"},
			},
			"httpLogs": map[string]any{
				"fileSystem": map[string]any{
					acrWebhookEnabled: false,
					"retentionInMb":   webLogsLimitMB,
					"retentionInDays": 0,
				},
			},
			"failedRequestsTracing": map[string]any{acrWebhookEnabled: false},
			"detailedErrorMessages": map[string]any{acrWebhookEnabled: false},
		}
	case "slotConfigNames":
		return map[string]any{
			"appSettingNames":         []any{},
			"connectionStringNames":   []any{},
			"azureStorageConfigNames": []any{},
		}
	case "pushsettings":
		return map[string]any{"isPushEnabled": false}
	}

	return map[string]any{}
}

// configHolder is the record that stores configuration name: slotConfigNames is shared by a site and its slots.
func (p *WebProvider) configHolder(s *storedWebSite, name string) *storedWebSite {
	if name == webSlotConfigNames && s.Slot != "" {
		if parent, ok := p.sites[webSiteKey(s.Name)]; ok {
			return parent
		}
	}

	return s
}

func (p *WebProvider) siteFor(id ResourceID) (*storedWebSite, error) {
	s, ok := p.record(id)
	if !ok {
		return nil, ErrWebSiteNotFound
	}

	return s, nil
}

func configEnvelope(id ResourceID, name string, props map[string]any) map[string]any {
	return map[string]any{
		"id": id.ARMID() + "/config/" + name, fieldName: name, fieldType: webTypeConfig, fieldProperties: props,
	}
}

func (p *WebProvider) configProps(s *storedWebSite, name string) (map[string]any, error) {
	if name == webConfigAppSettings {
		settings, err := p.dataPlane.Settings(s.dpName())
		if err != nil {
			return nil, err
		}

		out := make(map[string]any, len(settings))
		for k, v := range settings {
			out[k] = v
		}

		return out, nil
	}

	props := configDefaultProps(name, s)
	if name != webConfigWeb {
		if stored, ok := s.Configs[name]; ok {
			props = maps.Clone(stored)
		}
	}

	return props, nil
}

// GetConfig serves GET and POST .../config/{name}[/list].
func (p *WebProvider) GetConfig(_ context.Context, id ResourceID, name string) (map[string]any, error) {
	canon, ok := canonicalConfigName(name)
	if !ok {
		return nil, ErrWebConfigNotFound
	}

	p.mu.RLock("GetConfig")
	defer p.mu.RUnlock()

	s, err := p.siteFor(id)
	if err != nil {
		return nil, err
	}

	props, err := p.configProps(p.configHolder(s, canon), canon)
	if err != nil {
		return nil, badWebRequest(err)
	}

	return configEnvelope(id, canon, props), nil
}

// PutConfig serves PUT (replace) and PATCH (merge) .../config/{name}.
func (p *WebProvider) PutConfig(
	_ context.Context, id ResourceID, name string, body map[string]any, merge bool,
) (map[string]any, error) {
	canon, ok := canonicalConfigName(name)
	if !ok {
		return nil, ErrWebConfigNotFound
	}

	p.mu.Lock("PutConfig")
	defer p.mu.Unlock()

	s, err := p.siteFor(id)
	if err != nil {
		return nil, err
	}

	if s.Pending != nil && (canon == webConfigAppSettings || canon == webConfigWeb || canon == webConfigConn) {
		return nil, ErrWebSwapPending
	}

	s = p.configHolder(s, canon)
	props, _ := body[fieldProperties].(map[string]any)

	switch canon {
	case webConfigAppSettings:
		err = p.putAppSettings(s, props, merge)
	case webConfigWeb:
		err = p.putWebConfig(s, props)
	default:
		cur := map[string]any{}
		if merge {
			maps.Copy(cur, s.Configs[canon])
		}

		maps.Copy(cur, props)
		s.Configs[canon] = cur
	}

	if err != nil {
		return nil, err
	}

	out, err := p.configProps(s, canon)
	if err != nil {
		return nil, badWebRequest(err)
	}

	return configEnvelope(id, canon, out), nil
}

func (p *WebProvider) putAppSettings(s *storedWebSite, props map[string]any, merge bool) error {
	next := map[string]string{}

	if merge {
		cur, err := p.dataPlane.Settings(s.dpName())
		if err != nil {
			return badWebRequest(err)
		}

		maps.Copy(next, cur)
	}

	for k, v := range props {
		str, isStr := v.(string)
		if !isStr {
			return badWebRequest(fmt.Errorf("%w: %q", errWebAppSetting, k))
		}

		next[k] = str
	}

	if err := p.dataPlane.SetSettings(s.dpName(), next); err != nil {
		return badWebRequest(err)
	}

	return nil
}

func (p *WebProvider) putWebConfig(s *storedWebSite, props map[string]any) error {
	cur := mergeWebConfig(s.Configs[webConfigWeb], props)

	oldCfg, newCfg := siteAppConfig(s.Kind, s.Configs[webConfigWeb]), siteAppConfig(s.Kind, cur)

	if oldCfg != newCfg {
		if _, err := p.dataPlane.PutApp(s.dpName(), newCfg); err != nil {
			return badWebRequest(err)
		}
	}

	s.Configs[webConfigWeb] = cur

	return nil
}

// PublishingCredentials serves POST .../config/publishingcredentials/list.
func (p *WebProvider) PublishingCredentials(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("PublishingCredentials")
	defer p.mu.RUnlock()

	s, err := p.siteFor(id)
	if err != nil {
		return nil, err
	}

	user, pass, scm, err := p.dataPlane.PublishingCredentials(s.dpName())
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"id": id.ARMID() + "/config/" + webConfigPubCr, fieldName: webConfigPubCr, fieldType: webTypeConfig,
		fieldProperties: map[string]any{
			"publishingUserName": user, "publishingPassword": pass, "scmUri": credentialURI(scm, user, pass),
		},
	}, nil
}

func credentialURI(scm, user, pass string) string {
	scheme, rest, ok := strings.Cut(scm, "://")
	if !ok {
		return scm
	}

	return scheme + "://" + strings.ReplaceAll(user, "$", "%24") + ":" + pass + "@" + rest
}

type publishProfile struct {
	XMLName       xml.Name `xml:"publishProfile"`
	ProfileName   string   `xml:"profileName,attr"`
	PublishMethod string   `xml:"publishMethod,attr"`
	PublishURL    string   `xml:"publishUrl,attr"`
	UserName      string   `xml:"userName,attr"`
	UserPWD       string   `xml:"userPWD,attr"`
	DestAppURL    string   `xml:"destinationAppUrl,attr"`
	WebSystem     string   `xml:"webSystem,attr"`
}

type publishData struct {
	XMLName  xml.Name         `xml:"publishData"`
	Profiles []publishProfile `xml:"publishProfile"`
}

// PublishXML serves POST .../publishxml: a ZipDeploy profile that points at the app's Kudu site.
func (p *WebProvider) PublishXML(_ context.Context, id ResourceID) ([]byte, error) {
	p.mu.RLock("PublishXML")
	defer p.mu.RUnlock()

	s, err := p.siteFor(id)
	if err != nil {
		return nil, err
	}

	user, pass, scm, err := p.dataPlane.PublishingCredentials(s.dpName())
	if err != nil {
		return nil, err
	}

	info, _ := p.dataPlane.AppInfo(s.dpName())

	return xml.Marshal(publishData{Profiles: []publishProfile{
		{
			ProfileName:   s.Name + s.slotSuffix() + " - Zip Deploy",
			PublishMethod: "ZipDeploy",
			PublishURL:    strings.TrimPrefix(scm, "http://"),
			UserName:      user,
			UserPWD:       pass,
			DestAppURL:    "http://" + info.HostName,
			WebSystem:     "WebSites",
		},
	}})
}

// Policy serves GET and PUT .../basicPublishingCredentialsPolicies/{ftp|scm}.
func (p *WebProvider) Policy(
	_ context.Context, id ResourceID, name string, body map[string]any, put bool,
) (map[string]any, error) {
	name = strings.ToLower(name)
	if name != webPolicyFTP && name != webPolicySCM {
		return nil, ErrWebConfigNotFound
	}

	p.mu.Lock("Policy")
	defer p.mu.Unlock()

	s, err := p.siteFor(id)
	if err != nil {
		return nil, err
	}

	key := "policy-" + name
	if put {
		props, _ := body[fieldProperties].(map[string]any)

		allow, ok := props["allow"].(bool)
		if !ok {
			return nil, badWebRequest(errWebAllowRequired)
		}

		s.Configs[key] = map[string]any{"allow": allow}
	}

	allow := true
	if v, ok := s.Configs[key]["allow"].(bool); ok {
		allow = v
	}

	return map[string]any{
		"id": id.ARMID() + "/basicPublishingCredentialsPolicies/" + name, fieldName: name, fieldType: webTypePolicy,
		fieldProperties: map[string]any{"allow": allow},
	}, nil
}

// webSnapshot is the Microsoft.Web provider's persisted ARM-side state.
type webSnapshot struct {
	Sites map[string]*storedWebSite `json:"sites,omitempty"`
	Plans map[string]*storedWebPlan `json:"plans,omitempty"`
}

func (p *WebProvider) snapshot() *webSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &webSnapshot{
		Sites: make(map[string]*storedWebSite, len(p.sites)), Plans: make(map[string]*storedWebPlan, len(p.plans)),
	}

	maps.Copy(snap.Sites, p.sites)
	maps.Copy(snap.Plans, p.plans)

	return snap
}

// restore replaces the provider's state; nil clears it. The data plane is not touched.
func (p *WebProvider) restore(snap *webSnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.sites, p.plans = map[string]*storedWebSite{}, map[string]*storedWebPlan{}

	if snap == nil {
		return
	}

	for k, s := range snap.Sites {
		if s != nil {
			if s.Configs == nil {
				s.Configs = map[string]map[string]any{}
			}

			p.migrateIdentity(s)

			for _, sl := range s.Slots {
				if sl.Configs == nil {
					sl.Configs = map[string]map[string]any{}
				}

				p.migrateIdentity(sl)
			}

			p.sites[k] = s
		}
	}

	for k, pl := range snap.Plans {
		if pl != nil {
			p.plans[k] = pl
		}
	}
}

// migrateIdentity upgrades the identity block older snapshots stored verbatim: the type is kept, a system-assigned
// principal is issued, and user-assigned references (never resolved back then) are dropped.
func (p *WebProvider) migrateIdentity(s *storedWebSite) {
	legacy := s.Identity
	s.Identity = nil

	if s.Managed.isSet() || len(legacy) == 0 {
		return
	}

	typ, ok := normIdentityType(strOf(legacy, fieldType))
	if !ok || typ == identityNone || typ == identityUser {
		return
	}

	s.Managed = resourceIdentity{Type: identitySystem, PrincipalID: uuid.NewString(), TenantID: p.ids.tenantID()}
}
