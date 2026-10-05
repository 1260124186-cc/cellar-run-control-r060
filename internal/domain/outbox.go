package domain

import "time"

// MessageState is the local processing state of an outbox message.
type MessageState string

const (
	// MessagePending waits for a duty terminal to claim it.
	MessagePending MessageState = "pending"
	// MessageClaimed is held exclusively by one terminal for processing.
	MessageClaimed MessageState = "claimed"
	// MessageHandled is terminal: the duty room has confirmed the fact.
	MessageHandled MessageState = "handled"
)

// Message types produced by business actions.
const (
	MessageFormulaApproved        = "formula_approved"
	MessageRunFermentationStarted = "run_fermentation_started"
	MessageTemperatureBreach      = "temperature_breach"
	MessageVesselCleaningFinished = "vessel_cleaning_finished"
)

// Local duty roles that messages are addressed to. Nothing is delivered off
// the machine; these only route messages inside the duty room interface.
const (
	RecipientDutyPlanner  = "duty-planner"
	RecipientDutyOperator = "duty-operator"
	RecipientDutyObserver = "duty-observer"
)

// OutboxMessage is a locally persisted notification about one confirmed
// business fact. It is written in the same atomic snapshot revision as the
// fact itself, so the fact and its message are either both present or both
// absent.
type OutboxMessage struct {
	ID             string         `json:"id"`
	Sequence       int64          `json:"sequence"`
	DedupKey       string         `json:"dedup_key"`
	Type           string         `json:"type"`
	Recipient      string         `json:"recipient"`
	RunID          string         `json:"run_id,omitempty"`
	Subject        string         `json:"subject"`
	Detail         string         `json:"detail"`
	Payload        map[string]any `json:"payload,omitempty"`
	State          MessageState   `json:"state"`
	ClaimToken     string         `json:"claim_token,omitempty"`
	ClaimedBy      string         `json:"claimed_by,omitempty"`
	Attempts       int            `json:"attempts"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	ClaimedAt      *time.Time     `json:"claimed_at,omitempty"`
	LeaseExpiresAt *time.Time     `json:"lease_expires_at,omitempty"`
	HandledAt      *time.Time     `json:"handled_at,omitempty"`
}

func (m OutboxMessage) Clone() OutboxMessage {
	cloned := m
	if m.Payload != nil {
		cloned.Payload = make(map[string]any, len(m.Payload))
		for key, value := range m.Payload {
			cloned.Payload[key] = value
		}
	}
	if m.ClaimedAt != nil {
		value := *m.ClaimedAt
		cloned.ClaimedAt = &value
	}
	if m.LeaseExpiresAt != nil {
		value := *m.LeaseExpiresAt
		cloned.LeaseExpiresAt = &value
	}
	if m.HandledAt != nil {
		value := *m.HandledAt
		cloned.HandledAt = &value
	}
	return cloned
}

// IsClaimable reports whether a terminal may take the message right now.
// Pending messages are open; claimed messages reopen only when their lease
// has expired (for example after the holder crashed or was restarted).
func (m OutboxMessage) IsClaimable(now time.Time) bool {
	stamp := now.UTC()
	switch m.State {
	case MessagePending:
		return true
	case MessageClaimed:
		return m.LeaseExpiresAt != nil && !stamp.Before(m.LeaseExpiresAt.UTC())
	default:
		return false
	}
}

// AssignClaim moves the message to the claimed state under the given token.
// The caller must first verify IsClaimable.
func (m *OutboxMessage) AssignClaim(terminalID, token string, now, leaseExpiresAt time.Time) {
	stamp := now.UTC()
	lease := leaseExpiresAt.UTC()
	m.State = MessageClaimed
	m.ClaimToken = token
	m.ClaimedBy = terminalID
	m.Attempts++
	m.ClaimedAt = &stamp
	m.LeaseExpiresAt = &lease
	m.UpdatedAt = stamp
}

// Complete marks the message handled. Only the terminal holding the current
// claim token can do so; handled is terminal and is never reopened.
func (m *OutboxMessage) Complete(token string, now time.Time) error {
	if m.State == MessageHandled {
		return NewError(CodeMessageAlreadyHandled, "message has already been handled")
	}
	if m.State != MessageClaimed || token == "" || m.ClaimToken != token {
		return NewError(CodeMessageClaimConflict,
			"message can only be completed with its current claim token")
	}
	stamp := now.UTC()
	m.State = MessageHandled
	m.ClaimToken = ""
	m.LeaseExpiresAt = nil
	m.HandledAt = &stamp
	m.UpdatedAt = stamp
	return nil
}

// Release returns a claimed message to the pending queue so another terminal
// can claim it. It does not apply to handled messages.
func (m *OutboxMessage) Release(token string, now time.Time) error {
	if m.State == MessageHandled {
		return NewError(CodeMessageAlreadyHandled, "message has already been handled")
	}
	if m.State != MessageClaimed {
		return NewError(CodeMessageClaimConflict, "message is not claimed")
	}
	if token == "" || m.ClaimToken != token {
		return NewError(CodeMessageClaimConflict,
			"message can only be released with its current claim token")
	}
	m.State = MessagePending
	m.ClaimToken = ""
	m.ClaimedBy = ""
	m.ClaimedAt = nil
	m.LeaseExpiresAt = nil
	m.UpdatedAt = now.UTC()
	return nil
}
