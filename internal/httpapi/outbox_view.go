package httpapi

import (
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

// messageFailureView mirrors domain.MessageFailure for HTTP output.
type messageFailureView struct {
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

// messageView is the public projection of an outbox message. The internal
// claim capability token is intentionally absent; only the claiming terminal
// receives it in the claim response.
type messageView struct {
	ID        string               `json:"id"`
	Sequence  int64                `json:"sequence"`
	DedupKey  string               `json:"dedup_key"`
	Type      domain.MessageType   `json:"type"`
	Subject   string               `json:"subject"`
	Recipient string               `json:"recipient"`
	Priority  string               `json:"priority"`
	FormulaID string               `json:"formula_id,omitempty"`
	RunID     *string              `json:"run_id,omitempty"`
	VesselID  *string              `json:"vessel_id,omitempty"`
	Details   map[string]string    `json:"details,omitempty"`
	State     domain.MessageState  `json:"state"`
	Attempts  int                  `json:"attempts"`
	Failures  []messageFailureView `json:"failures,omitempty"`

	ClaimedBy      *string    `json:"claimed_by,omitempty"`
	ClaimedAt      *time.Time `json:"claimed_at,omitempty"`
	ClaimExpiresAt *time.Time `json:"claim_expires_at,omitempty"`
	HandledBy      *string    `json:"handled_by,omitempty"`
	HandledAt      *time.Time `json:"handled_at,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Version   int64     `json:"version"`
}

func projectMessage(message domain.OutboxMessage) messageView {
	view := messageView{
		ID:             message.ID,
		Sequence:       message.Sequence,
		DedupKey:       message.DedupKey,
		Type:           message.Type,
		Subject:        message.Subject,
		Recipient:      message.Recipient,
		Priority:       message.Priority,
		FormulaID:      message.FormulaID,
		RunID:          cloneStr(message.RunID),
		VesselID:       cloneStr(message.VesselID),
		State:          message.State,
		Attempts:       message.Attempts,
		ClaimedBy:      cloneStr(message.ClaimedBy),
		ClaimedAt:      cloneTime(message.ClaimedAt),
		ClaimExpiresAt: cloneTime(message.ClaimExpiresAt),
		HandledBy:      cloneStr(message.HandledBy),
		HandledAt:      cloneTime(message.HandledAt),
		CreatedAt:      message.CreatedAt,
		UpdatedAt:      message.UpdatedAt,
		Version:        message.Version,
	}
	if len(message.Details) > 0 {
		view.Details = make(map[string]string, len(message.Details))
		for key, value := range message.Details {
			view.Details[key] = value
		}
	}
	if len(message.Failures) > 0 {
		view.Failures = make([]messageFailureView, len(message.Failures))
		for index, failure := range message.Failures {
			view.Failures[index] = messageFailureView{
				Reason: failure.Reason,
				At:     failure.At,
			}
		}
	}
	return view
}

func projectMessages(messages []domain.OutboxMessage) []messageView {
	views := make([]messageView, len(messages))
	for index, message := range messages {
		views[index] = projectMessage(message)
	}
	return views
}

func cloneStr(value *string) *string {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}
