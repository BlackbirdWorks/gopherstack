package connect

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	maxBatchContacts   = 25
	maxCreateName      = 1024
	participantTTL     = 25 * time.Hour
	expiryAttribute    = "connect:ContactExpiry"
	roleCustomBot      = "CUSTOM_BOT"
	initiateConnected  = "CONNECTED_TO_USER"
	initiateCompleted  = "COMPLETED"
	channelEmail       = "EMAIL"
	failMissingCust    = "MISSING_CUSTOMER_ENDPOINT"
	failInvalidCust    = "INVALID_CUSTOMER_ENDPOINT"
	failInvalidSystem  = "INVALID_SYSTEM_ENDPOINT"
	failInvalidQueue   = "INVALID_QUEUE"
	failInvalidAttr    = "INVALID_ATTRIBUTE_KEY"
	failMissingQSystem = "MISSING_QUEUE_ID_AND_SYSTEM_ENDPOINT"
	failMissingCamp    = "MISSING_CAMPAIGN"
	failInvalidOutbnd  = "INVALID_OUTBOUND_STRATEGY"
	failIdempotency    = "IDEMPOTENCY_EXCEPTION"
)

var attributeKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// UserInfoIn identifies the user a contact belongs to.
type UserInfoIn struct {
	UserID string `json:"UserId"`
}

// CreateContactInput is the CreateContact request.
type CreateContactInput struct {
	ExpiryDurationInMinutes *int32
	UserInfo                *UserInfoIn
	Attributes              map[string]string
	ClientToken             string
	Channel                 string
	InitiationMethod        string
	InitiateAs              string
	InstanceID              string `json:"InstanceId"`
	Description             string
	Name                    string
	PreviousContactID       string `json:"PreviousContactId"`
	RelatedContactID        string `json:"RelatedContactId"`
	SegmentAttributes       jsontext.Value
	References              jsontext.Value
}

// CreateContactOut is the CreateContact response.
type CreateContactOut struct {
	ContactArn string `json:"ContactArn"`
	ContactID  string `json:"ContactId"`
}

func userRefID(ref string) string {
	if _, after, ok := strings.CutLast(ref, "/agent/"); ok {
		return after
	}

	return ref
}

func validAttributeKeys(attrs map[string]string) error {
	for k := range attrs {
		if !attributeKeyPattern.MatchString(k) {
			return invalidParamErr("attribute key %q may only contain alphanumeric, dash and underscore characters", k)
		}
	}

	return checkAttributes(attrs)
}

func checkCreateContactMethod(in *CreateContactInput) error {
	if err := oneOf("Channel", in.Channel, channelVoice, channelEmail, channelTask); err != nil {
		return err
	}

	methods := map[string][]string{
		channelVoice: {"TRANSFER"},
		channelEmail: {methodOutbound, "AGENT_REPLY", methodFlow},
		channelTask:  {methodAPI},
	}[in.Channel]

	return oneOf("InitiationMethod", in.InitiationMethod, methods...)
}

func validateCreateContact(in *CreateContactInput) error {
	if err := checkCreateContactMethod(in); err != nil {
		return err
	}

	if in.InitiateAs != "" {
		if err := oneOf("InitiateAs", in.InitiateAs, initiateConnected, initiateCompleted); err != nil {
			return err
		}
	}

	if in.InitiateAs == initiateCompleted && in.Channel != channelTask {
		return invalidRequestErr("only TASK contacts can be initiated as COMPLETED")
	}

	userRequired := in.Channel == channelEmail && in.InitiationMethod != methodFlow
	if (userRequired || in.InitiateAs == initiateConnected) && (in.UserInfo == nil || in.UserInfo.UserID == "") {
		return invalidRequestErr("UserInfo is required")
	}

	if in.PreviousContactID != "" && in.Channel != channelVoice {
		return invalidRequestErr("PreviousContactId is only supported for external audio contacts")
	}

	return checkCreateContactText(in)
}

func checkCreateContactText(in *CreateContactInput) error {
	if in.ExpiryDurationInMinutes != nil && *in.ExpiryDurationInMinutes < 1 {
		return invalidParamErr("ExpiryDurationInMinutes must be positive")
	}

	if len(in.Name) > maxCreateName || len(in.Description) > maxContactDescLen {
		return invalidParamErr("Name must be at most %d and Description at most %d characters",
			maxCreateName, maxContactDescLen)
	}

	return validAttributeKeys(in.Attributes)
}

func expirySegments(raw jsontext.Value, minutes *int32) (jsontext.Value, error) {
	if minutes == nil {
		return raw, nil
	}

	segs := map[string]jsontext.Value{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &segs, jsonx.V1()); err != nil {
			return nil, invalidParamErr("SegmentAttributes is not valid")
		}
	}

	if _, ok := segs[expiryAttribute]; !ok {
		segs[expiryAttribute] = jsontext.Value(
			`{"ValueMap":{"ExpiryDuration":{"ValueInteger":` + strconv.FormatInt(int64(*minutes), 10) + `}}}`)
	}

	return json.Marshal(segs, jsonx.V1())
}

// CreateContact creates a voice, email or task contact.
func (b *Backend) CreateContact(ctx context.Context, in *CreateContactInput) (*CreateContactOut, error) {
	if err := validateCreateContact(in); err != nil {
		return nil, err
	}

	segs, err := expirySegments(in.SegmentAttributes, in.ExpiryDurationInMinutes)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateContact")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID

	for _, ref := range []string{in.PreviousContactID, in.RelatedContactID} {
		if ref != "" && !b.contacts.has(inst, ref) {
			return nil, notFoundErr("Contact", ref)
		}
	}

	c := b.newContact(ctx, inst, in.Channel, in.InitiationMethod)
	c.References, c.Contact.SegmentAttributes = in.References, segs
	c.Contact.Name, c.Contact.Description = in.Name, in.Description
	c.Contact.Attributes = cloneTags(in.Attributes)
	c.Contact.PreviousContactID, c.Contact.RelatedContactID = in.PreviousContactID, in.RelatedContactID

	if in.UserInfo != nil && in.UserInfo.UserID != "" {
		uid := userRefID(in.UserInfo.UserID)
		if !b.users.has(inst, uid) {
			return nil, notFoundErr("User", in.UserInfo.UserID)
		}

		if in.InitiateAs == initiateConnected {
			c.Contact.AgentInfo = &ContactAgentInfo{ID: uid, ConnectedToAgentTimestamp: c.Contact.InitiationTimestamp}
		}
	}

	if in.InitiateAs == initiateCompleted {
		c.Contact.DisconnectTimestamp = c.Contact.InitiationTimestamp
	}

	b.contacts.put(c)

	return &CreateContactOut{ContactArn: c.Contact.Arn, ContactID: c.Contact.ID}, nil
}

// ContactDataRequest is one contact of a BatchPutContact request.
type ContactDataRequest struct {
	Campaign          *Campaign
	CustomerEndpoint  *EndpointInfo
	SystemEndpoint    *EndpointInfo
	Attributes        map[string]string
	QueueID           string `json:"QueueId"`
	RequestIdentifier string
	OutboundStrategy  jsontext.Value
}

// BatchPutContactInput is the BatchPutContact request.
type BatchPutContactInput struct {
	InstanceID             string `json:"-" path:"InstanceId"`
	ClientToken            string
	ContactDataRequestList []ContactDataRequest
}

// FailedRequest is a BatchPutContact item that was not created.
type FailedRequest struct {
	FailureReasonCode    string `json:"FailureReasonCode"`
	FailureReasonMessage string `json:"FailureReasonMessage"`
	RequestIdentifier    string `json:"RequestIdentifier,omitempty"`
}

// SuccessfulRequest is a BatchPutContact item that created a contact.
type SuccessfulRequest struct {
	ContactID         string `json:"ContactId"`
	RequestIdentifier string `json:"RequestIdentifier,omitempty"`
}

// BatchPutContactOut is the BatchPutContact response.
type BatchPutContactOut struct {
	FailedRequestList     []FailedRequest     `json:"FailedRequestList"`
	SuccessfulRequestList []SuccessfulRequest `json:"SuccessfulRequestList"`
}

func validEndpoint(e *EndpointInfo) bool {
	switch e.Type {
	case endpointPhone:
		return e164Pattern.MatchString(e.Address)
	case "VOIP", endpointFlow, "CONNECT_PHONENUMBER_ARN", "EMAIL_ADDRESS":
		return e.Address != ""
	}

	return false
}

func (b *Backend) checkBatchItem(inst string, req *ContactDataRequest) (string, string) {
	if validAttributeKeys(req.Attributes) != nil {
		return failInvalidAttr, "an attribute key is not valid"
	}

	if req.CustomerEndpoint == nil {
		return failMissingCust, "CustomerEndpoint is required"
	}

	if !validEndpoint(req.CustomerEndpoint) {
		return failInvalidCust, "CustomerEndpoint is not valid"
	}

	if req.SystemEndpoint != nil && !validEndpoint(req.SystemEndpoint) {
		return failInvalidSystem, "SystemEndpoint is not valid"
	}

	if req.QueueID == "" && req.SystemEndpoint == nil {
		return failMissingQSystem, "QueueId or SystemEndpoint is required"
	}

	if req.QueueID != "" && !b.queues.has(inst, req.QueueID) {
		return failInvalidQueue, "QueueId " + req.QueueID + " was not found"
	}

	if len(req.OutboundStrategy) > 0 {
		var s struct {
			Type string `json:"Type"`
		}
		if json.Unmarshal(req.OutboundStrategy, &s, jsonx.V1()) != nil || s.Type != "AGENT_FIRST" {
			return failInvalidOutbnd, "OutboundStrategy.Type must be AGENT_FIRST"
		}

		if req.Campaign == nil || req.Campaign.CampaignID == "" {
			return failMissingCamp, "Campaign is required with an OutboundStrategy"
		}
	}

	return "", ""
}

// BatchPutContact creates up to 25 outbound voice contacts, reporting each item's outcome.
func (b *Backend) BatchPutContact(ctx context.Context, in *BatchPutContactInput) (*BatchPutContactOut, error) {
	if n := len(in.ContactDataRequestList); n < 1 || n > maxBatchContacts {
		return nil, invalidRequestErr("ContactDataRequestList must hold 1 to %d items", maxBatchContacts)
	}

	b.mu.Lock("BatchPutContact")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID
	out := &BatchPutContactOut{FailedRequestList: []FailedRequest{}, SuccessfulRequestList: []SuccessfulRequest{}}
	seen := map[string]bool{}

	for i := range in.ContactDataRequestList {
		req := &in.ContactDataRequestList[i]

		code, msg := b.checkBatchItem(inst, req)
		if code == "" && req.RequestIdentifier != "" && seen[req.RequestIdentifier] {
			code, msg = failIdempotency, "RequestIdentifier "+req.RequestIdentifier+" is repeated in the batch"
		}

		if code != "" {
			out.FailedRequestList = append(
				out.FailedRequestList,
				FailedRequest{
					FailureReasonCode:    code,
					FailureReasonMessage: msg,
					RequestIdentifier:    req.RequestIdentifier,
				},
			)

			continue
		}

		seen[req.RequestIdentifier] = true
		c := b.newContact(ctx, inst, channelVoice, methodOutbound)
		c.Contact.Attributes = cloneTags(req.Attributes)
		c.Contact.CustomerEndpoint, c.Contact.SystemEndpoint = req.CustomerEndpoint, req.SystemEndpoint
		c.Contact.Campaign, c.Contact.OutboundStrategy = req.Campaign, req.OutboundStrategy

		if req.QueueID != "" {
			c.Contact.QueueInfo = &ContactQueueInfo{ID: req.QueueID, EnqueueTimestamp: c.Contact.InitiationTimestamp}
		}

		b.contacts.put(c)
		out.SuccessfulRequestList = append(out.SuccessfulRequestList,
			SuccessfulRequest{ContactID: c.Contact.ID, RequestIdentifier: req.RequestIdentifier})
	}

	return out, nil
}

// AssociateContactWithUserInput is the AssociateContactWithUser request.
type AssociateContactWithUserInput struct {
	ContactID  string `json:"-"      path:"ContactId"`
	InstanceID string `json:"-"      path:"InstanceId"`
	UserID     string `json:"UserId"`
}

// AssociateContactWithUser offers a queued contact to an agent.
func (b *Backend) AssociateContactWithUser(in *AssociateContactWithUserInput) (*EmptyOutput, error) {
	if in.UserID == "" {
		return nil, invalidParamErr("UserId is required")
	}

	b.mu.Lock("AssociateContactWithUser")
	defer b.mu.Unlock()

	c, err := b.activeContactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	uid := userRefID(in.UserID)
	if !b.users.has(c.InstanceID, uid) {
		return nil, notFoundErr("User", in.UserID)
	}

	if c.Contact.QueueInfo == nil || c.Contact.AgentInfo != nil {
		return nil, invalidRequestErr("contact %q is not queued", in.ContactID)
	}

	t := awstime.NewEpochTime(b.now())
	c.Contact.AgentInfo = &ContactAgentInfo{ID: uid, ConnectedToAgentTimestamp: &t}
	b.touch(c)

	return &EmptyOutput{}, nil
}

// activeContactLocked resolves a contact that has not ended, with ResourceNotFoundException for a missing one.
func (b *Backend) activeContactLocked(instRef, id string) (*contactRec, error) {
	c, err := b.resourceContactLocked(instRef, id)
	if err != nil {
		return nil, err
	}

	if !c.active() {
		return nil, invalidRequestErr("contact %q has ended", id)
	}

	return c, nil
}

func (b *Backend) resourceContactLocked(instRef, id string) (*contactRec, error) {
	r, err := b.instanceLocked(instRef)
	if err != nil {
		return nil, err
	}

	c, err := b.contacts.get(r.Instance.ID, id)
	if err != nil {
		return nil, notFoundErr("Contact", id)
	}

	return c, nil
}

// ParticipantDetailsToAdd describes the participant CreateParticipant adds.
type ParticipantDetailsToAdd struct {
	DisplayName             string
	ParticipantRole         string
	ParticipantCapabilities jsontext.Value
}

// CreateParticipantInput is the CreateParticipant request.
type CreateParticipantInput struct {
	ParticipantDetails *ParticipantDetailsToAdd
	ContactID          string `json:"ContactId"`
	InstanceID         string `json:"InstanceId"`
	ClientToken        string
}

type participantCredentials struct {
	Expiry           string `json:"Expiry"`
	ParticipantToken string `json:"ParticipantToken"`
}

// CreateParticipantOut is the CreateParticipant response.
type CreateParticipantOut struct {
	ParticipantCredentials participantCredentials `json:"ParticipantCredentials"`
	ParticipantID          string                 `json:"ParticipantId"`
}

// CreateParticipant adds a custom bot participant to an ongoing chat contact.
func (b *Backend) CreateParticipant(in *CreateParticipantInput) (*CreateParticipantOut, error) {
	d := in.ParticipantDetails
	if d == nil || strings.TrimSpace(d.DisplayName) == "" {
		return nil, invalidRequestErr("ParticipantDetails.DisplayName is required")
	}

	b.mu.Lock("CreateParticipant")
	defer b.mu.Unlock()

	c, err := b.activeContactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if c.Contact.Channel != channelChat {
		return nil, invalidRequestErr("participants can only be added to chat contacts")
	}

	if d.ParticipantRole != roleCustomBot {
		return nil, invalidRequestErr("ParticipantRole must be %s for a chat contact", roleCustomBot)
	}

	p := participantRec{ID: uuid.NewString(), Role: d.ParticipantRole, DisplayName: d.DisplayName, Token: newToken()}
	c.Participants = append(c.Participants, p)
	b.touch(c)

	return &CreateParticipantOut{
		ParticipantID: p.ID,
		ParticipantCredentials: participantCredentials{
			ParticipantToken: p.Token,
			Expiry:           b.now().Add(participantTTL).UTC().Format("2006-01-02T15:04:05.000Z"),
		},
	}, nil
}

// CreatePersistentContactAssociationInput is the CreatePersistentContactAssociation request.
type CreatePersistentContactAssociationInput struct {
	InitialContactID string `json:"-"               path:"InitialContactId"`
	InstanceID       string `json:"-"               path:"InstanceId"`
	RehydrationType  string
	SourceContactID  string `json:"SourceContactId"`
	ClientToken      string
}

// PersistentAssociationOut is the persistent contact association response.
type PersistentAssociationOut struct {
	ContinuedFromContactID string `json:"ContinuedFromContactId"`
}

func (b *Backend) rehydrationSource(inst string, in *CreatePersistentContactAssociationInput) (*contactRec, error) {
	if in.RehydrationType == "FROM_SEGMENT" {
		return b.persistentSource(
			inst,
			&PersistentChat{RehydrationType: in.RehydrationType, SourceContactID: in.SourceContactID},
		)
	}

	if err := oneOf("RehydrationType", in.RehydrationType, "ENTIRE_PAST_SESSION", "FROM_SEGMENT"); err != nil {
		return nil, err
	}

	var latest *contactRec

	for _, c := range b.contacts.in(inst) {
		if c.Contact.InitialContactID != in.SourceContactID || c.Contact.Channel != channelChat ||
			c.Contact.DisconnectTimestamp == nil {
			continue
		}

		if latest == nil || c.Contact.DisconnectTimestamp.After(latest.Contact.DisconnectTimestamp.Time) {
			latest = c
		}
	}

	if latest == nil {
		return nil, notFoundErr("Contact", in.SourceContactID)
	}

	return latest, nil
}

// CreatePersistentContactAssociation links a chat contact to the past chat it continues.
func (b *Backend) CreatePersistentContactAssociation(
	in *CreatePersistentContactAssociationInput,
) (*PersistentAssociationOut, error) {
	b.mu.Lock("CreatePersistentContactAssociation")
	defer b.mu.Unlock()

	cur, err := b.activeContactLocked(in.InstanceID, in.InitialContactID)
	if err != nil {
		return nil, err
	}

	if cur.Contact.Channel != channelChat {
		return nil, invalidRequestErr("contact %q is not a chat contact", in.InitialContactID)
	}

	if in.SourceContactID == in.InitialContactID {
		return nil, invalidRequestErr("SourceContactId must differ from the current contact")
	}

	src, err := b.rehydrationSource(cur.InstanceID, in)
	if err != nil {
		return nil, err
	}

	cur.Contact.PreviousContactID = src.Contact.ID
	b.touch(cur)

	return &PersistentAssociationOut{ContinuedFromContactID: src.Contact.ID}, nil
}
