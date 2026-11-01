package azurearm

import (
	"context"
	"maps"
	"strings"
)

const (
	webTypeFunction = "Microsoft.Web/sites/functions"
	webKeyDefault   = "default"
)

func (p *WebProvider) appOrError(id ResourceID) (*storedWebSite, FunctionAppInfo, error) {
	s, err := p.siteFor(id)
	if err != nil {
		return nil, FunctionAppInfo{}, err
	}

	info, ok := p.dataPlane.AppInfo(s.dpName())
	if !ok {
		return nil, FunctionAppInfo{}, ErrWebSiteNotFound
	}

	return s, info, nil
}

// SetState serves POST .../start, .../stop and .../restart.
func (p *WebProvider) SetState(_ context.Context, id ResourceID, action string) error {
	p.mu.Lock("SetState")
	defer p.mu.Unlock()

	s, _, err := p.appOrError(id)
	if err != nil {
		return err
	}

	switch action {
	case "start":
		return p.dataPlane.SetStopped(s.dpName(), false)
	case "stop":
		return p.dataPlane.SetStopped(s.dpName(), true)
	}

	return p.dataPlane.Restart(s.dpName())
}

// Patch serves PATCH on a site: properties are merged over the stored ones.
func (p *WebProvider) Patch(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.RLock("Patch.read")
	s, ok := p.record(id)

	if !ok {
		p.mu.RUnlock()

		return nil, ErrWebSiteNotFound
	}

	merged := map[string]any{
		fieldLocation: s.Location, webFieldKind: s.Kind, fieldTags: tagsAsAny(s.Tags),
		fieldProperties: ehMerge(map[string]any{}, s.Extra, nil),
	}

	if ident := p.ids.body(s.Managed); ident != nil {
		merged[fieldIdentity] = ident
	}

	merged[webFieldKind] = s.Kind
	p.mu.RUnlock()

	for _, k := range []string{fieldLocation, webFieldKind, fieldIdentity} {
		if v, found := body[k]; found {
			merged[k] = v
		}
	}

	if tags, found := body[fieldTags]; found {
		merged[fieldTags] = tags
	}

	if props, found := body[fieldProperties].(map[string]any); found {
		mp := mapOf(merged[fieldProperties])
		maps.Copy(mp, props)
	}

	return p.Put(ctx, id, merged)
}

func (p *WebProvider) functionEnvelope(id ResourceID, host string, f FunctionAppFunction) map[string]any {
	base := "http://" + host
	fid := id.ARMID() + "/functions/" + f.Name

	props := map[string]any{
		fieldName:               f.Name,
		"function_app_id":       id.ARMID(),
		"script_root_path_href": base + "/admin/vfs/home/site/wwwroot/" + f.Name + "/",
		"config_href":           base + "/admin/vfs/home/site/wwwroot/" + f.Name + "/function.json",
		"test_data_href":        base + "/admin/vfs/data/Functions/sampledata/" + f.Name + ".dat",
		"secrets_file_href":     nil,
		"href":                  base + "/admin/functions/" + f.Name,
		"config":                f.Config,
		"files":                 nil,
		"test_data":             "",
		"language":              f.Language,
		"isDisabled":            f.IsDisabled,
	}

	if f.InvokeURL != "" {
		props["invoke_url_template"] = f.InvokeURL
	}

	return map[string]any{
		"id":            fid,
		fieldName:       strings.Join(id.Names, "/") + "/" + f.Name,
		fieldType:       webTypeFunction,
		fieldProperties: props,
	}
}

// ListFunctions serves GET .../functions.
func (p *WebProvider) ListFunctions(ctx context.Context, id ResourceID) ([]map[string]any, error) {
	s, info, err := p.lockedApp(id)
	if err != nil {
		return nil, err
	}

	fns, err := p.dataPlane.Functions(ctx, s.Name)
	if err != nil {
		return nil, badWebRequest(err)
	}

	out := make([]map[string]any, 0, len(fns))
	for _, f := range fns {
		out = append(out, p.functionEnvelope(id, info.HostName, f))
	}

	return out, nil
}

func (p *WebProvider) lockedFunctionApp(id ResourceID) (*storedWebSite, error) {
	s, _, err := p.lockedApp(id)
	if err == nil && !isFunctionAppKind(s.Kind) {
		return nil, ErrWebNotFunctionApp
	}

	return s, err
}

func (p *WebProvider) lockedApp(id ResourceID) (*storedWebSite, FunctionAppInfo, error) {
	p.mu.RLock("lockedApp")
	defer p.mu.RUnlock()

	return p.appOrError(id)
}

func (p *WebProvider) function(
	ctx context.Context,
	id ResourceID,
	fn string,
) (*storedWebSite, FunctionAppInfo, FunctionAppFunction, error) {
	s, info, err := p.lockedApp(id)
	if err != nil {
		return nil, info, FunctionAppFunction{}, err
	}

	fns, err := p.dataPlane.Functions(ctx, s.Name)
	if err != nil {
		return nil, info, FunctionAppFunction{}, badWebRequest(err)
	}

	for _, f := range fns {
		if strings.EqualFold(f.Name, fn) {
			return s, info, f, nil
		}
	}

	return nil, info, FunctionAppFunction{}, ErrWebFunctionNotFound
}

// GetFunction serves GET .../functions/{fn}.
func (p *WebProvider) GetFunction(ctx context.Context, id ResourceID, fn string) (map[string]any, error) {
	_, info, f, err := p.function(ctx, id, fn)
	if err != nil {
		return nil, err
	}

	return p.functionEnvelope(id, info.HostName, f), nil
}

// FunctionKeys serves POST .../functions/{fn}/listkeys.
func (p *WebProvider) FunctionKeys(ctx context.Context, id ResourceID, fn string) (map[string]any, error) {
	s, _, f, err := p.function(ctx, id, fn)
	if err != nil {
		return nil, err
	}

	keys, err := p.dataPlane.FunctionKeys(s.dpName(), f.Name)
	if err != nil {
		return nil, err
	}

	props := make(map[string]any, len(keys))
	for k, v := range keys {
		props[k] = v
	}

	return map[string]any{fieldProperties: props}, nil
}

// FunctionSecrets serves POST .../functions/{fn}/listsecrets.
func (p *WebProvider) FunctionSecrets(ctx context.Context, id ResourceID, fn string) (map[string]any, error) {
	s, _, f, err := p.function(ctx, id, fn)
	if err != nil {
		return nil, err
	}

	keys, err := p.dataPlane.FunctionKeys(s.dpName(), f.Name)
	if err != nil {
		return nil, err
	}

	key := keys[webKeyDefault]
	out := map[string]any{"key": key}

	if f.InvokeURL != "" {
		out["trigger_url"] = f.InvokeURL + "?code=" + key
	}

	return out, nil
}

// SetFunctionKey serves PUT .../functions/{fn}/keys/{name}.
func (p *WebProvider) SetFunctionKey(
	ctx context.Context, id ResourceID, fn, keyName string, body map[string]any,
) (map[string]any, error) {
	s, _, f, err := p.function(ctx, id, fn)
	if err != nil {
		return nil, err
	}

	val, err := p.dataPlane.SetFunctionKey(s.dpName(), f.Name, keyName, keyValue(body))
	if err != nil {
		return nil, err
	}

	return map[string]any{fieldName: keyName, fieldValue: val}, nil
}

// DeleteFunctionKey serves DELETE .../functions/{fn}/keys/{name}.
func (p *WebProvider) DeleteFunctionKey(ctx context.Context, id ResourceID, fn, keyName string) error {
	s, _, f, err := p.function(ctx, id, fn)
	if err != nil {
		return err
	}

	return p.dataPlane.DeleteFunctionKey(s.dpName(), f.Name, keyName)
}

// keyValue reads the key value from a flat or properties-wrapped KeyInfo body.
func keyValue(body map[string]any) string {
	if v, ok := body[fieldValue].(string); ok {
		return v
	}

	props, _ := body[fieldProperties].(map[string]any)
	v, _ := props[fieldValue].(string)

	return v
}

// HostKeys serves POST .../host/default/listkeys.
func (p *WebProvider) HostKeys(_ context.Context, id ResourceID) (map[string]any, error) {
	s, err := p.lockedFunctionApp(id)
	if err != nil {
		return nil, err
	}

	hk, err := p.dataPlane.HostKeys(s.dpName())
	if err != nil {
		return nil, err
	}

	toAny := func(m map[string]string) map[string]any {
		out := make(map[string]any, len(m))
		for k, v := range m {
			out[k] = v
		}

		return out
	}

	return map[string]any{
		"masterKey": hk.MasterKey, "functionKeys": toAny(hk.FunctionKeys), "systemKeys": toAny(hk.SystemKeys),
	}, nil
}

// SetHostKey serves PUT .../host/default/{keyType}/{name}.
func (p *WebProvider) SetHostKey(
	_ context.Context, id ResourceID, keyType, keyName string, body map[string]any,
) (map[string]any, error) {
	s, err := p.lockedFunctionApp(id)
	if err != nil {
		return nil, err
	}

	val, err := p.dataPlane.SetHostKey(s.dpName(), canonicalKeyType(keyType), keyName, keyValue(body))
	if err != nil {
		return nil, err
	}

	return map[string]any{fieldName: keyName, fieldValue: val}, nil
}

// DeleteHostKey serves DELETE .../host/default/{keyType}/{name}.
func (p *WebProvider) DeleteHostKey(_ context.Context, id ResourceID, keyType, keyName string) error {
	s, err := p.lockedFunctionApp(id)
	if err != nil {
		return err
	}

	return p.dataPlane.DeleteHostKey(s.dpName(), canonicalKeyType(keyType), keyName)
}

func canonicalKeyType(t string) string {
	for _, c := range []string{FunctionHostKeyTypeFunction, FunctionHostKeyTypeSystem} {
		if strings.EqualFold(t, c) {
			return c
		}
	}

	return t
}

// CheckName answers POST /subscriptions/{sub}/providers/Microsoft.Web/checknameavailability.
func (p *WebProvider) CheckName(name string) map[string]any {
	if !validWebName(name) {
		return map[string]any{
			acFieldAvailable: false, acFieldReason: webReasonInvalid,
			"message": "'" + name + "' is not a valid name; use 2-60 letters, digits or hyphens.",
		}
	}

	p.mu.RLock("CheckName")
	defer p.mu.RUnlock()

	if _, taken := p.sites[webSiteKey(name)]; taken {
		return map[string]any{
			acFieldAvailable: false, acFieldReason: kvReasonAlreadyExists,
			"message": "The name '" + name + "' is already in use.",
		}
	}

	return map[string]any{acFieldAvailable: true}
}

func tagsAsAny(tags map[string]string) map[string]any {
	out := make(map[string]any, len(tags))
	for k, v := range tags {
		out[k] = v
	}

	return out
}
