package cognitoidp

import "time"

// AuthEvent represents an adaptive-authentication (risk) sign-in event for a
// user, and its feedback state once reviewed via [Admin]UpdateAuthEventFeedback.
type AuthEvent struct {
	CreatedAt     time.Time `json:"createdAt"`
	FeedbackDate  time.Time `json:"feedbackDate"`
	EventID       string    `json:"eventID,omitempty"`
	EventType     string    `json:"eventType,omitempty"`
	EventResponse string    `json:"eventResponse,omitempty"`
	FeedbackValue string    `json:"feedbackValue,omitempty"`
}

type authEventFeedbackType struct {
	FeedbackValue string  `json:"FeedbackValue,omitempty"`
	FeedbackDate  float64 `json:"FeedbackDate,omitempty"`
}

type authEventOutputType struct {
	EventFeedback *authEventFeedbackType `json:"EventFeedback,omitempty"`
	EventID       string                 `json:"EventId,omitempty"`
	EventType     string                 `json:"EventType,omitempty"`
	EventResponse string                 `json:"EventResponse,omitempty"`
	CreationDate  float64                `json:"CreationDate,omitempty"`
}

type adminListUserAuthEventsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type adminListUserAuthEventsOutput struct {
	NextToken  string                `json:"NextToken,omitempty"`
	AuthEvents []authEventOutputType `json:"AuthEvents,omitempty"`
}

type adminUpdateAuthEventFeedbackInput struct {
	UserPoolID    string `json:"UserPoolId,omitempty"`
	Username      string `json:"Username,omitempty"`
	EventID       string `json:"EventId,omitempty"`
	FeedbackValue string `json:"FeedbackValue,omitempty"`
}

type adminUpdateAuthEventFeedbackOutput struct{}

type updateAuthEventFeedbackInput struct {
	UserPoolID    string `json:"UserPoolId,omitempty"`
	Username      string `json:"Username,omitempty"`
	EventID       string `json:"EventId,omitempty"`
	FeedbackToken string `json:"FeedbackToken,omitempty"`
	FeedbackValue string `json:"FeedbackValue,omitempty"`
}

type updateAuthEventFeedbackOutput struct{}
