package ocimysql

import (
	"context"
	"encoding/json/jsontext"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	entityBlueGreen = "bluegreendeployment"
	wrCreateBG      = "CREATE_BLUE_GREEN_DEPLOYMENT"
	wrUpdateBG      = "UPDATE_BLUE_GREEN_DEPLOYMENT"
	wrDeleteBG      = "DELETE_BLUE_GREEN_DEPLOYMENT"
	wrChangeBGComp  = "CHANGE_BLUE_GREEN_DEPLOYMENT_COMPARTMENT"
	wrSwitchoverBG  = "SWITCHOVER_BLUE_GREEN_DEPLOYMENT"

	bgNotStarted = "NOT_STARTED"
	bgReplicate  = "REPLICATING_DATA"
	bgReady      = "READY_FOR_SWITCHOVER"
	bgInProgress = "SWITCHOVER_IN_PROGRESS"
	bgCompleted  = "SWITCHOVER_COMPLETED"
	bgFailed     = "SWITCHOVER_FAILED"

	sslVerifyCA       = "VERIFY_CA"
	sslVerifyIdentity = "VERIFY_IDENTITY"
	defaultSwitchWait = 300
)

// BlueGreenTarget are the overrides of the green DB system.
type BlueGreenTarget struct {
	MysqlVersion    string `json:"mysqlVersion"`
	ShapeName       string `json:"shapeName,omitempty"`
	ConfigurationID string `json:"configurationId,omitempty"`
	DataGB          int    `json:"dataStorageSizeInGBs,omitempty"`
}

// BlueGreen is a blue/green deployment: a green copy of a DB system kept in sync by a channel until switchover.
type BlueGreen struct {
	TimeCreated   time.Time       `json:"timeCreated"`
	TimeUpdated   time.Time       `json:"timeUpdated"`
	Tags          Tags            `json:"tags"`
	TransitionAt  *time.Time      `json:"transitionAt,omitempty"`
	ID            string          `json:"id"`
	CompartmentID string          `json:"compartmentId"`
	Name          string          `json:"displayName"`
	State         string          `json:"state"`
	After         string          `json:"after,omitempty"`
	Details       string          `json:"details,omitempty"`
	Etag          string          `json:"etag"`
	SourceID      string          `json:"sourceDbSystemId"`
	TargetID      string          `json:"targetDbSystemId"`
	ActiveID      string          `json:"activeDbSystemId"`
	ChannelID     string          `json:"replicationChannelId"`
	SslMode       string          `json:"sslMode"`
	Switchover    string          `json:"switchoverStatus"`
	Target        BlueGreenTarget `json:"target"`
	Seq           int64           `json:"seq"`
}

// BGChannelIn is the channelDetails of CreateBlueGreenDeployment.
type BGChannelIn struct {
	SourceUsername   string         `json:"sourceUsername"`
	SourcePassword   string         `json:"sourcePassword"`
	SslMode          string         `json:"sslMode"`
	ApplierUsername  string         `json:"applierUsername"`
	SslCaCertificate jsontext.Value `json:"sslCaCertificate"`
}

// BGTargetIn is the targetDbSystemDetails of CreateBlueGreenDeployment.
type BGTargetIn struct {
	DataGB          *int   `json:"dataStorageSizeInGBs"`
	MysqlVersion    string `json:"mysqlVersion"`
	ShapeName       string `json:"shapeName"`
	ConfigurationID string `json:"configurationId"`
}

// CreateBlueGreenIn is the CreateBlueGreenDeployment body.
type CreateBlueGreenIn struct {
	Tags
	RetryToken    string      `json:"-"`
	CompartmentID string      `json:"compartmentId"`
	DisplayName   string      `json:"displayName"`
	SourceID      string      `json:"sourceDbSystemId"`
	Target        BGTargetIn  `json:"targetDbSystemDetails"`
	Channel       BGChannelIn `json:"channelDetails"`
}

func (b *InMemoryBackend) blueGreenOf(sysID string) *BlueGreen {
	for _, g := range b.blueGreens {
		if g.State != stateDeleted && g.State != stateDeleting && (g.SourceID == sysID || g.TargetID == sysID) {
			return g
		}
	}

	return nil
}

func validBGChannel(in BGChannelIn) *APIError {
	switch {
	case in.SourceUsername == "":
		return errMissing("channelDetails.sourceUsername is required")
	case !validPassword(in.SourcePassword):
		return errParam("channelDetails.sourcePassword must be 8 to 32 characters with a digit, a lowercase and an " +
			"uppercase letter and a special character")
	case !containsFold(sslModes, in.SslMode):
		return errParam("channelDetails.sslMode must be one of " + strings.Join(sslModes, ", "))
	}

	needsCA := strings.EqualFold(in.SslMode, sslVerifyCA) || strings.EqualFold(in.SslMode, sslVerifyIdentity)
	hasCA := len(in.SslCaCertificate) > 0

	switch {
	case needsCA && !hasCA:
		return errMissing("channelDetails.sslCaCertificate is required for sslMode " + in.SslMode)
	case !needsCA && hasCA:
		return errParam("channelDetails.sslCaCertificate must not be set for sslMode " + in.SslMode)
	}

	return nil
}

func (b *InMemoryBackend) checkBGSource(in CreateBlueGreenIn) (*DBSystem, *APIError) {
	if in.SourceID == "" {
		return nil, errMissing("sourceDbSystemId is required")
	}

	b.settleID(in.SourceID)

	s, ok := b.systems[in.SourceID]
	if !ok || s.State == stateDeleted {
		return nil, errNotFound(in.SourceID)
	}

	switch {
	case s.State != stateActive:
		return nil, errState("DB system " + s.ID + " is in state " + s.State + "; blue/green requires ACTIVE")
	case b.blueGreenOf(s.ID) != nil:
		return nil, errExists("DB system " + s.ID + " already takes part in a blue/green deployment")
	case in.Target.MysqlVersion == "":
		return nil, errMissing("targetDbSystemDetails.mysqlVersion is required")
	case !isKnownVersion(in.Target.MysqlVersion):
		return nil, errParam("targetDbSystemDetails.mysqlVersion " + in.Target.MysqlVersion + " is not supported")
	case versionBefore(in.Target.MysqlVersion, s.Version):
		return nil, errParam("targetDbSystemDetails.mysqlVersion cannot be lower than the source's " + s.Version)
	case in.Target.DataGB != nil && *in.Target.DataGB < s.DataGB:
		return nil, errParam("targetDbSystemDetails.dataStorageSizeInGBs cannot be smaller than the source's")
	}

	return s, nil
}

// CreateBlueGreen creates the green DB system and the channel that keeps it in sync with the blue one.
func (b *InMemoryBackend) CreateBlueGreen(in CreateBlueGreenIn) (Doc, string, string, error) {
	b.mu.Lock("CreateBlueGreen")
	defer b.mu.Unlock()

	id, rwr, hit, rerr := b.replayCreate("CreateBlueGreen", in.RetryToken, func(id string) (string, bool) {
		g, ok := b.blueGreens[id]
		if !ok {
			return "", false
		}

		return g.State, true
	})
	if hit {
		if rerr != nil {
			return nil, "", "", rerr
		}

		g := b.blueGreens[id]

		return b.blueGreenDoc(g), g.Etag, rwr, nil
	}

	if aerr := firstErr(
		b.checkCompartment(in.CompartmentID),
		validateName(in.DisplayName),
		validBGChannel(in.Channel),
	); aerr != nil {
		return nil, "", "", aerr
	}

	src, aerr := b.checkBGSource(in)
	if aerr != nil {
		return nil, "", "", aerr
	}

	green, gwr, aerr := b.createSystemLocked(greenInput(in, src))
	if aerr != nil {
		return nil, "", "", aerr
	}

	now := b.nowUTC()
	g := &BlueGreen{
		ID:            newOCID("mysqlbluegreen"),
		CompartmentID: in.CompartmentID,
		Name:          in.DisplayName,
		State:         stateCreating,
		After:         stateActive,
		SourceID:      src.ID,
		TargetID:      green.ID,
		ActiveID:      src.ID,
		SslMode:       strings.ToUpper(in.Channel.SslMode),
		Switchover:    bgNotStarted,
		Tags:          in.Tags,
		Etag:          newEtag(),
		TimeCreated:   now,
		TimeUpdated:   now,
		Seq:           b.nextSeq(),
		Target: BlueGreenTarget{
			MysqlVersion:    in.Target.MysqlVersion,
			ShapeName:       green.Shape,
			ConfigurationID: green.ConfigID,
			DataGB:          green.DataGB,
		},
	}
	g.ChannelID = b.newBGChannel(g, src, green, in.Channel).ID

	if b.eng == nil {
		g.TransitionAt = b.transitionAt()
	}

	b.blueGreens[g.ID] = g
	wr := b.newWR(wrCreateBG, g.CompartmentID, entityBlueGreen, g.ID, actionCreated, g.TransitionAt)
	b.retry.Remember("CreateBlueGreen", in.RetryToken, ociretry.Join(g.ID, wr), now)
	b.spawnBlueGreen(g.ID, wr, b.blueGreenCreateWork(g.ID, gwr), func(g *BlueGreen) {
		g.State, g.Switchover = stateActive, bgReady
	})

	return b.blueGreenDoc(g), g.Etag, wr, nil
}

// greenInput clones the blue DB system with the deployment's overrides.
func greenInput(in CreateBlueGreenIn, s *DBSystem) CreateDBSystemIn {
	ha := s.HA

	out := CreateDBSystemIn{
		CompartmentID:     s.CompartmentID,
		DisplayName:       in.DisplayName + "-green",
		ShapeName:         orStr(in.Target.ShapeName, s.Shape),
		MysqlVersion:      in.Target.MysqlVersion,
		SubnetID:          s.SubnetID,
		AvailabilityDom:   s.AD,
		AdminUsername:     s.AdminUser,
		AdminPassword:     s.AdminPass,
		ConfigurationID:   orStr(in.Target.ConfigurationID, s.ConfigID),
		IsHighlyAvailable: &ha,
		DataStorageGBs:    &s.DataGB,
		DatabaseMode:      s.DatabaseMode,
		AccessMode:        s.AccessMode,
		CrashRecovery:     s.CrashRecovery,
	}
	if in.Target.DataGB != nil {
		out.DataStorageGBs = in.Target.DataGB
	}

	return out
}

func (b *InMemoryBackend) newBGChannel(g *BlueGreen, src, green *DBSystem, in BGChannelIn) *Channel {
	host, port := b.endpointFor(src)
	now := b.nowUTC()
	c := &Channel{
		ID: newOCID("mysqlchannel"), CompartmentID: g.CompartmentID, Name: g.Name + "-channel", Enabled: true,
		State: stateCreating, After: stateActive, Etag: newEtag(), TimeCreated: now, TimeUpdated: now, Seq: b.nextSeq(),
		Source: ChannelSource{
			SourceType: sourceMysql, Hostname: host, Port: port, Username: in.SourceUsername, SslMode: g.SslMode,
			HasPassword: true, Password: in.SourcePassword,
		},
		Target: ChannelTarget{
			TargetType: targetDBSystem, DBSystemID: green.ID, ChannelName: "bg" + shortID(g.ID),
			ApplierUsername: orStr(in.ApplierUsername, "applier"), TablesWithoutPrimaryKeyHandling: channelRaise,
		},
	}
	if b.eng == nil {
		c.TransitionAt = b.transitionAt()
	} else {
		c.TransitionAt = nil
	}

	b.channels[c.ID] = c

	return c
}

func shortID(id string) string {
	const keep = 10

	if len(id) <= keep {
		return id
	}

	return id[len(id)-keep:]
}

func (b *InMemoryBackend) blueGreenDoc(g *BlueGreen) Doc {
	d := Doc{
		"id": g.ID, keyCompartment: g.CompartmentID, keyLifecycle: g.State, keyTimeCreated: g.TimeCreated,
		keyTimeUpdated: g.TimeUpdated, keyDisplayName: g.Name, "sourceDbSystemId": g.SourceID,
		"targetDbSystemId": g.TargetID, "activeDbSystemId": g.ActiveID, "switchoverStatus": g.Switchover,
		"replicationChannelId": g.ChannelID, "sslMode": g.SslMode,
		"targetDbSystemDetails": Doc{
			keyMysqlVer: g.Target.MysqlVersion, "shapeName": g.Target.ShapeName,
			keyConfigID: g.Target.ConfigurationID, "dataStorageSizeInGBs": g.Target.DataGB,
		},
	}
	if g.Details != "" {
		d["lifecycleDetails"] = g.Details
	}

	putTags(d, g.Tags)

	return d
}

func (b *InMemoryBackend) blueGreenFor(id, ifMatch string) (*BlueGreen, *APIError) {
	b.settleBlueGreen(id)

	g, ok := b.blueGreens[id]
	if !ok || g.State == stateDeleted {
		return nil, errNotFound(id)
	}

	return g, checkEtag(g.Etag, ifMatch)
}

func (b *InMemoryBackend) settleBlueGreen(id string) {
	now := b.nowUTC()

	for gid, g := range b.blueGreens {
		if (id == "" || gid == id) && due(g.TransitionAt, now) {
			g.TransitionAt = nil

			switch g.State {
			case stateDeleting:
				g.State = stateDeleted
			case stateCreating:
				g.State, g.Switchover = stateActive, bgReady
			default:
				g.State = stateActive
			}

			g.After = ""
		}
	}
}

// GetBlueGreen returns one blue/green deployment.
func (b *InMemoryBackend) GetBlueGreen(id string) (Doc, string, error) {
	b.mu.Lock("GetBlueGreen")
	defer b.mu.Unlock()

	g, aerr := b.blueGreenFor(id, "")
	if aerr != nil {
		return nil, "", aerr
	}

	return b.blueGreenDoc(g), g.Etag, nil
}

// BlueGreenListIn are the ListBlueGreenDeployments filters.
type BlueGreenListIn struct {
	Compartment string
	SourceID    string
	TargetID    string
	Name        string
	State       string
	ListOpts
}

// ListBlueGreens lists the blue/green deployments of a compartment.
func (b *InMemoryBackend) ListBlueGreens(in BlueGreenListIn) (page.Page[Doc], error) {
	if aerr := firstErr(
		b.checkCompartmentLocked(in.Compartment),
		validListOpts(in.ListOpts, keyDisplayName, keyTimeCreated),
	); aerr != nil {
		return page.Page[Doc]{}, aerr
	}

	b.mu.Lock("ListBlueGreens")
	defer b.mu.Unlock()

	b.settleBlueGreen("")

	var all []*BlueGreen

	for _, g := range b.blueGreens {
		if g.CompartmentID == in.Compartment && (in.SourceID == "" || g.SourceID == in.SourceID) &&
			(in.TargetID == "" || g.TargetID == in.TargetID) && (in.Name == "" || g.Name == in.Name) &&
			stateMatches(g.State, in.State) {
			all = append(all, g)
		}
	}

	pg := paginate(all, in.ListOpts, func(g *BlueGreen) sortKeys {
		return sortKeys{created: g.TimeCreated, updated: g.TimeUpdated, name: g.Name, seq: g.Seq}
	})

	out := make([]Doc, 0, len(pg.Data))
	for _, g := range pg.Data {
		out = append(out, b.blueGreenDoc(g))
	}

	return page.Page[Doc]{Data: out, Next: pg.Next}, nil
}

// UpdateBlueGreenIn is the UpdateBlueGreenDeployment body.
type UpdateBlueGreenIn struct {
	Tags
	DisplayName *string `json:"displayName"`
}

// UpdateBlueGreen changes the name and tags of a deployment.
func (b *InMemoryBackend) UpdateBlueGreen(id, ifMatch string, in UpdateBlueGreenIn) (string, error) {
	b.mu.Lock("UpdateBlueGreen")
	defer b.mu.Unlock()

	g, aerr := b.blueGreenFor(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if in.DisplayName != nil {
		if aerr = validateName(*in.DisplayName); aerr != nil {
			return "", aerr
		}

		g.Name = *in.DisplayName
	}

	g.Tags.merge(in.Tags)
	g.TimeUpdated, g.Etag = b.nowUTC(), newEtag()

	wr := b.newWR(wrUpdateBG, g.CompartmentID, entityBlueGreen, g.ID, actionUpdated, nil)
	b.finishWR(wr, "")

	return wr, nil
}

// ChangeBlueGreenCompartment moves a deployment to another compartment.
func (b *InMemoryBackend) ChangeBlueGreenCompartment(id, ifMatch, compartment string) (string, error) {
	b.mu.Lock("ChangeBlueGreenCompartment")
	defer b.mu.Unlock()

	g, aerr := b.blueGreenFor(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if aerr = b.checkCompartment(compartment); aerr != nil {
		return "", aerr
	}

	g.CompartmentID, g.TimeUpdated, g.Etag = compartment, b.nowUTC(), newEtag()

	wr := b.newWR(wrChangeBGComp, compartment, entityBlueGreen, g.ID, actionUpdated, nil)
	b.finishWR(wr, "")

	return wr, nil
}

// DeleteBlueGreen removes the deployment and its channel; both DB systems are kept.
func (b *InMemoryBackend) DeleteBlueGreen(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteBlueGreen")
	defer b.mu.Unlock()

	g, aerr := b.blueGreenFor(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if g.State != stateActive && g.State != stateFailed {
		return "", errState("blue/green deployment " + id + " is in state " + g.State)
	}

	g.State, g.After, g.TimeUpdated, g.Etag = stateDeleting, "", b.nowUTC(), newEtag()

	if b.eng == nil {
		g.TransitionAt = b.transitionAt()
	}

	wr := b.newWR(wrDeleteBG, g.CompartmentID, entityBlueGreen, g.ID, actionDeleted, g.TransitionAt)

	if c, ok := b.channels[g.ChannelID]; ok && c.State != stateDeleted {
		c.State, c.TransitionAt, c.Etag = stateDeleting, b.transitionAt(), newEtag()

		if b.eng != nil {
			c.TransitionAt = nil

			b.deleteChannelJob(c, "")
		}
	}

	b.spawnBlueGreen(
		g.ID,
		wr,
		func(context.Context) error { return nil },
		func(g *BlueGreen) { g.State = stateDeleted },
	)

	return wr, nil
}

func (h *Handler) createBlueGreen(q *req) error {
	var in CreateBlueGreenIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken = q.r.Header.Get(headerRetry)

	d, etag, wr, err := h.Backend.CreateBlueGreen(in)
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusOK, d, etag)
}

func (h *Handler) getBlueGreen(q *req) error {
	d, etag, err := h.Backend.GetBlueGreen(q.param("blueGreenDeploymentId"))

	return reply(q, d, etag, err)
}

func (h *Handler) listBlueGreens(q *req) error {
	pg, err := h.Backend.ListBlueGreens(BlueGreenListIn{
		Compartment: q.query(
			keyCompartment,
		),
		SourceID: q.query("sourceDbSystemId"),
		TargetID: q.query("targetDbSystemId"),
		Name:     q.query(keyDisplayName),
		State:    q.query(keyLifecycle),
		ListOpts: q.listOpts(),
	})

	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, Doc{"items": pg.Data}, "")
}

func (h *Handler) updateBlueGreen(q *req) error {
	var in UpdateBlueGreenIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateBlueGreen(q.param("blueGreenDeploymentId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeBlueGreenCompartment(q *req) error {
	var in struct {
		CompartmentID string `json:"compartmentId"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.ChangeBlueGreenCompartment(q.param("blueGreenDeploymentId"), q.ifMatch(), in.CompartmentID)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteBlueGreen(q *req) error {
	wr, err := h.Backend.DeleteBlueGreen(q.param("blueGreenDeploymentId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}
