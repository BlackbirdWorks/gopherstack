package ociapigateway

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	entitySdk      = "sdk"
	sdkLangVersion = "1.0"

	wrCreateSdk = "CREATE_SDK"
	wrDeleteSdk = "DELETE_SDK"
)

// SDK is an SDK resource. No client library is generated: the record is stored and never carries an artifact URL.
type SDK struct {
	TimeCreated    time.Time         `json:"timeCreated"`
	TimeUpdated    time.Time         `json:"timeUpdated"`
	Tags           Tags              `json:"tags"`
	TransitionAt   *time.Time        `json:"transitionAt,omitempty"`
	Parameters     map[string]string `json:"parameters,omitempty"`
	ID             string            `json:"id"`
	APIID          string            `json:"apiId"`
	Compartment    string            `json:"compartmentId"`
	DisplayName    string            `json:"displayName"`
	TargetLanguage string            `json:"targetLanguage"`
	State          string            `json:"state"`
	Etag           string            `json:"etag"`
	Locks          []Lock            `json:"locks,omitempty"`
}

func (i *createSdkIn) setRetryToken(t string) { i.RetryToken = t }

type createSdkIn struct {
	Tags
	RetryToken     string            `json:"-"`
	Parameters     map[string]string `json:"parameters"`
	TargetLanguage string            `json:"targetLanguage"`
	APIID          string            `json:"apiId"`
	DisplayName    string            `json:"displayName"`
	Locks          []lockIn          `json:"locks"`
}

type updateSdkIn struct {
	Tags
	DisplayName *string           `json:"displayName"`
	Parameters  map[string]string `json:"parameters"`
}

// SdkWire is the wire form of an SDK resource (also used for list items).
type SdkWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	Parameters     map[string]string         `json:"parameters,omitempty"`
	ID             string                    `json:"id"`
	APIID          string                    `json:"apiId"`
	CompartmentID  string                    `json:"compartmentId"`
	DisplayName    string                    `json:"displayName"`
	TargetLanguage string                    `json:"targetLanguage"`
	LifecycleState string                    `json:"lifecycleState"`
	Locks          []Lock                    `json:"locks,omitempty"`
}

func sdkView(s *SDK) SdkWire {
	return SdkWire{
		TimeCreated: s.TimeCreated, TimeUpdated: s.TimeUpdated, DefinedTags: s.Tags.DefinedTags,
		FreeformTags: s.Tags.FreeformTags, Parameters: s.Parameters, ID: s.ID, APIID: s.APIID,
		CompartmentID: s.Compartment, DisplayName: s.DisplayName, TargetLanguage: s.TargetLanguage,
		LifecycleState: s.State, Locks: s.Locks,
	}
}

type sdkLanguageParam struct {
	ParamName   string `json:"paramName"`
	DisplayName string `json:"displayName,omitempty"`
}

// SdkLanguageWire is the SDK language wire shape.
type SdkLanguageWire struct {
	Name        string             `json:"name"`
	Version     string             `json:"version"`
	DisplayName string             `json:"displayName"`
	Parameters  []sdkLanguageParam `json:"parameters,omitempty"`
}

//nolint:gochecknoglobals // immutable catalog, recalled from the OCI console (unverified)
var sdkLanguages = []SdkLanguageWire{
	{Name: "JAVA", Version: sdkLangVersion, DisplayName: "Java"},
	{Name: "PYTHON", Version: sdkLangVersion, DisplayName: "Python"},
	{Name: "TYPESCRIPT", Version: sdkLangVersion, DisplayName: "TypeScript"},
	{Name: "ANDROID", Version: sdkLangVersion, DisplayName: "Android"},
	{Name: "SWIFT", Version: sdkLangVersion, DisplayName: "Swift"},
}

// ListSdkLanguageTypes lists the target languages SDK resources can name.
func (b *InMemoryBackend) ListSdkLanguageTypes(
	compartment, name string, o ListOpts,
) (page.Page[SdkLanguageWire], error) {
	if compartment == "" {
		return page.Page[SdkLanguageWire]{}, errMissing("compartmentId is required")
	}

	var all []SdkLanguageWire

	for _, l := range sdkLanguages {
		if name == "" || strs.Equal(l.DisplayName, name) || strs.Equal(l.Name, name) {
			all = append(all, l)
		}
	}

	return page.New(all, o.Page, o.Limit, defaultListLimit), nil
}

// CreateSDK records an SDK resource for an API.
func (b *InMemoryBackend) CreateSDK(in createSdkIn) (SdkWire, string, string, error) {
	b.mu.Lock("CreateSDK")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("CreateSDK", in.RetryToken, b.nowUTC()); ok {
		id, wr := ociretry.Split(v)
		s, live := b.sdks[id]
		if !live || s.State == stateDeleting || s.State == stateDeleted {
			return SdkWire{}, "", "", errInvalidatedToken()
		}

		return sdkView(s), wr, s.Etag, nil
	}

	if err := b.checkTags(in.Tags); err != nil {
		return SdkWire{}, "", "", err
	}

	if in.TargetLanguage == "" || in.APIID == "" {
		return SdkWire{}, "", "", errMissing("targetLanguage and apiId are required")
	}

	a, ok := b.apis[in.APIID]
	if !ok || a.State == stateDeleted {
		return SdkWire{}, "", "", errNotFound(in.APIID)
	}

	now := b.nowUTC()

	locks, lerr := newLocks(in.Locks, now)
	if lerr != nil {
		return SdkWire{}, "", "", lerr
	}

	name := in.DisplayName
	if name == "" {
		name = "sdk" + now.Format("20060102150405")
	}

	s := &SDK{
		ID: newOCID("apigatewaysdk"), APIID: a.ID, Compartment: a.Compartment, DisplayName: name,
		TargetLanguage: in.TargetLanguage, Parameters: in.Parameters, Locks: locks, State: stateCreating,
		TransitionAt: b.transitionAt(), TimeCreated: now, TimeUpdated: now, Etag: newEtag(), Tags: in.Tags,
	}
	b.sdks[s.ID] = s
	wr := b.newWR(wrCreateSdk, s.Compartment, entitySdk, s.ID, actionCreated, *s.TransitionAt)
	b.retry.Remember("CreateSDK", in.RetryToken, ociretry.Join(s.ID, wr), now)

	return sdkView(s), wr, s.Etag, nil
}

func (b *InMemoryBackend) liveSDK(id string) (*SDK, *APIError) {
	s, ok := b.sdks[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return s, nil
}

// GetSDK returns one SDK resource and its etag.
func (b *InMemoryBackend) GetSDK(id string) (SdkWire, string, error) {
	b.mu.RLock("GetSDK")
	defer b.mu.RUnlock()

	s, err := b.liveSDK(id)
	if err != nil {
		return SdkWire{}, "", err
	}

	return sdkView(s), s.Etag, nil
}

// ListSDKs lists SDK resources, optionally of one API.
func (b *InMemoryBackend) ListSDKs(sdkID, apiID, name, state string, o ListOpts) (page.Page[SdkWire], error) {
	b.mu.RLock("ListSDKs")
	defer b.mu.RUnlock()

	return listOf(b.sdks, o, func(s *SDK) bool {
		return (sdkID == "" || s.ID == sdkID) && (apiID == "" || s.APIID == apiID) && stateMatches(s.State, state) &&
			(name == "" || strs.Equal(s.DisplayName, name))
	}, func(s *SDK) string { return s.DisplayName }, func(s *SDK) time.Time { return s.TimeCreated }, sdkView), nil
}

// UpdateSDK changes the name, tags or parameters of an SDK resource.
func (b *InMemoryBackend) UpdateSDK(id, ifMatch string, in updateSdkIn) (SdkWire, string, error) {
	b.mu.Lock("UpdateSDK")
	defer b.mu.Unlock()

	if err := b.checkTags(in.Tags); err != nil {
		return SdkWire{}, "", err
	}

	s, err := b.liveSDK(id)
	if err != nil {
		return SdkWire{}, "", err
	}

	if err = checkEtag(s.Etag, ifMatch); err != nil {
		return SdkWire{}, "", err
	}

	if lerr := checkLocked(s.Locks, false); lerr != nil {
		return SdkWire{}, "", lerr
	}

	if in.DisplayName != nil && *in.DisplayName != "" {
		s.DisplayName = *in.DisplayName
	}

	if in.Parameters != nil {
		s.Parameters = in.Parameters
	}

	s.Tags.merge(in.Tags)
	s.TimeUpdated, s.Etag = b.nowUTC(), newEtag()

	return sdkView(s), s.Etag, nil
}

// DeleteSDK starts deleting an SDK resource.
func (b *InMemoryBackend) DeleteSDK(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteSDK")
	defer b.mu.Unlock()

	s, err := b.liveSDK(id)
	if err != nil {
		return "", err
	}

	if s.State != stateActive {
		return "", errState("sdk " + id + " is in state " + s.State)
	}

	if err = checkEtag(s.Etag, ifMatch); err != nil {
		return "", err
	}

	if lerr := checkLocked(s.Locks, true); lerr != nil {
		return "", lerr
	}

	s.State, s.TransitionAt = stateDeleting, b.transitionAt()
	s.TimeUpdated = b.nowUTC()

	return b.newWR(wrDeleteSdk, s.Compartment, entitySdk, id, actionDeleted, *s.TransitionAt), nil
}

func (h *Handler) createSDK(q *req) error {
	return created(q, "/sdks/", func(w SdkWire) string { return w.ID }, h.Backend.CreateSDK)
}

func (h *Handler) getSDK(q *req) error {
	w, etag, err := h.Backend.GetSDK(q.param("sdkId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listSDKs(q *req) error {
	pg, err := h.Backend.ListSDKs(q.query("sdkId"), q.query("apiId"), q.query("displayName"),
		q.query("lifecycleState"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) updateSDK(q *req) error {
	var in updateSdkIn
	if err := q.decode(&in); err != nil {
		return err
	}

	w, etag, err := h.Backend.UpdateSDK(q.param("sdkId"), q.ifMatch(), in)

	return reply(q, w, etag, err)
}

func (h *Handler) deleteSDK(q *req) error {
	wr, err := h.Backend.DeleteSDK(q.param("sdkId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) listSdkLanguageTypes(q *req) error {
	pg, err := h.Backend.ListSdkLanguageTypes(q.query("compartmentId"), q.query("displayName"), q.listOpts())

	return collectionReply(q, pg, err)
}
