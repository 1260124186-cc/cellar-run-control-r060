package domain

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// MessageState is the local processing state of an outbox message.
type MessageState string

const (
	// MessagePending waits for a duty officer to claim it.
	MessagePending MessageState = "pending"
	// MessageClaimed is held exclusively by one terminal.
	MessageClaimed MessageState = "claimed"
	// MessageHandled is a confirmed fact and is never delivered again.
	MessageHandled MessageState = "handled"
)

// MessageType identifies the business fact that produced a message.
type MessageType string

const (
	MessageFormulaApproved      MessageType = "formula_approved"
	MessageFermentationStarted  MessageType = "fermentation_started"
	MessageTemperatureExcursion MessageType = "temperature_excursion"
	MessageCleaningFinished     MessageType = "cleaning_finished"
)

// Recipients are local duty roles, not external addresses. Nothing leaves
// the machine.
const (
	RecipientProcessPlanner = "process_planner"
	RecipientCellarOperator = "cellar_operator"
)

// Message priorities.
const (
	MessagePriorityNormal = "normal"
	MessagePriorityHigh   = "high"
)

// MessageFailure records one failed handling attempt. The message itself is
// retained and returns to the pending queue.
type MessageFailure struct {
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

// maxMessageFailures bounds the retained failure history per message; older
// entries are dropped from the front while the attempt count keeps growing.
const maxMessageFailures = 20

// OutboxMessage is a locally persisted pending notification. There is no
// external delivery channel; the outbox is the durable hand-off between
// business actions and the duty terminal that handles them.
type OutboxMessage struct {
	ID        string      `json:"id"`
	Sequence  int64       `json:"sequence"`
	DedupKey  string      `json:"dedup_key"`
	Type      MessageType `json:"type"`
	Subject   string      `json:"subject"`
	Recipient string      `json:"recipient"`
	Priority  string      `json:"priority"`

	FormulaID string  `json:"formula_id,omitempty"`
	RunID     *string `json:"run_id,omitempty"`
	VesselID  *string `json:"vessel_id,omitempty"`

	Details map[string]string `json:"details,omitempty"`

	State    MessageState     `json:"state"`
	Attempts int              `json:"attempts"`
	Failures []MessageFailure `json:"failures,omitempty"`

	ClaimedBy      *string    `json:"claimed_by,omitempty"`
	ClaimedAt      *time.Time `json:"claimed_at,omitempty"`
	ClaimExpiresAt *time.Time `json:"claim_expires_at,omitempty"`
	HandledBy      *string    `json:"handled_by,omitempty"`
	HandledAt      *time.Time `json:"handled_at,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Version   int64     `json:"version"`

	// ClaimToken is a server-side capability token. It is persisted in the
	// local snapshot so an in-flight claim is internally consistent across a
	// restart; recovery immediately requeues such messages and clears it.
	// HTTP views never include it: the token is returned once from the claim
	// action and required to complete or fail the claim.
	ClaimToken *string `json:"claim_token,omitempty"`
}

// Clone returns a deep copy so repository-owned values never escape.
func (m OutboxMessage) Clone() OutboxMessage {
	cloned := m
	cloned.RunID = cloneStringPtr(m.RunID)
	cloned.VesselID = cloneStringPtr(m.VesselID)
	cloned.ClaimedBy = cloneStringPtr(m.ClaimedBy)
	cloned.HandledBy = cloneStringPtr(m.HandledBy)
	cloned.ClaimToken = cloneStringPtr(m.ClaimToken)
	cloned.ClaimedAt = cloneTimePtr(m.ClaimedAt)
	cloned.ClaimExpiresAt = cloneTimePtr(m.ClaimExpiresAt)
	cloned.HandledAt = cloneTimePtr(m.HandledAt)
	if m.Details != nil {
		cloned.Details = make(map[string]string, len(m.Details))
		for key, value := range m.Details {
			cloned.Details[key] = value
		}
	}
	cloned.Failures = append([]MessageFailure(nil), m.Failures...)
	return cloned
}

func cloneStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

// Validate checks a persisted or newly enqueued message.
func (m OutboxMessage) Validate() error {
	if m.ID == "" {
		return NewError(CodeStorage, "outbox message is missing an identifier")
	}
	if m.Sequence < 1 {
		return fmt.Errorf("outbox message %s has a non-positive sequence", m.ID)
	}
	if m.DedupKey == "" {
		return fmt.Errorf("outbox message %s is missing its dedup key", m.ID)
	}
	switch m.Type {
	case MessageFormulaApproved, MessageFermentationStarted,
		MessageTemperatureExcursion, MessageCleaningFinished:
	default:
		return fmt.Errorf("outbox message %s has an unknown type %q", m.ID, m.Type)
	}
	if strings.TrimSpace(m.Subject) == "" {
		return fmt.Errorf("outbox message %s is missing a subject", m.ID)
	}
	if strings.TrimSpace(m.Recipient) == "" {
		return fmt.Errorf("outbox message %s is missing a recipient", m.ID)
	}
	if m.Priority != MessagePriorityNormal && m.Priority != MessagePriorityHigh {
		return fmt.Errorf("outbox message %s has an invalid priority", m.ID)
	}
	switch m.State {
	case MessagePending:
		if m.ClaimedBy != nil || m.ClaimToken != nil || m.ClaimedAt != nil ||
			m.ClaimExpiresAt != nil || m.HandledAt != nil {
			return fmt.Errorf("pending message %s carries claim or handling data", m.ID)
		}
	case MessageClaimed:
		if m.ClaimedBy == nil || m.ClaimToken == nil || m.ClaimedAt == nil ||
			m.ClaimExpiresAt == nil {
			return fmt.Errorf("claimed message %s is missing claim data", m.ID)
		}
		if m.HandledAt != nil {
			return fmt.Errorf("claimed message %s carries a handled timestamp", m.ID)
		}
	case MessageHandled:
		if m.HandledBy == nil || m.HandledAt == nil {
			return fmt.Errorf("handled message %s is missing handling data", m.ID)
		}
		if m.ClaimToken != nil || m.ClaimExpiresAt != nil {
			return fmt.Errorf("handled message %s still carries a live claim", m.ID)
		}
	default:
		return fmt.Errorf("outbox message %s has an unknown state %q", m.ID, m.State)
	}
	return nil
}

// Claim assigns the message to the given operator. When the same operator
// replays a claim while it is still live, the existing token is returned so
// retries do not lose the claim. An expired claim can be taken over by any
// operator.
func (m *OutboxMessage) Claim(operator, token string, ttl time.Duration, now time.Time) (string, bool, error) {
	stamp := now.UTC()
	switch m.State {
	case MessagePending:
	case MessageClaimed:
		live := m.ClaimExpiresAt != nil && stamp.Before(*m.ClaimExpiresAt)
		if live {
			if m.ClaimedBy != nil && *m.ClaimedBy == operator && m.ClaimToken != nil {
				return *m.ClaimToken, true, nil
			}
			return "", false, NewError(CodeMessageClaim,
				"message is already claimed by another terminal")
		}
	case MessageHandled:
		return "", false, NewError(CodeMessageState,
			"message has already been handled")
	default:
		return "", false, NewError(CodeMessageState, "message cannot be claimed")
	}
	holder := operator
	m.State = MessageClaimed
	m.ClaimedBy = &holder
	m.ClaimedAt = &stamp
	expires := stamp.Add(ttl)
	m.ClaimExpiresAt = &expires
	secret := token
	m.ClaimToken = &secret
	m.UpdatedAt = stamp
	m.Version++
	return token, false, nil
}

// RequeueForRetry clears a claim after a failed handling attempt. The message
// stays in the outbox and becomes claimable again. An expired lease is
// rejected; the terminal must claim the message again.
func (m *OutboxMessage) RequeueForRetry(token, reason string, now time.Time) error {
	if err := m.requireLiveClaim(token, now); err != nil {
		return err
	}
	stamp := now.UTC()
	m.Failures = append(m.Failures, MessageFailure{Reason: reason, At: stamp})
	if len(m.Failures) > maxMessageFailures {
		m.Failures = m.Failures[len(m.Failures)-maxMessageFailures:]
	}
	m.Attempts++
	m.State = MessagePending
	m.ClaimedBy = nil
	m.ClaimedAt = nil
	m.ClaimExpiresAt = nil
	m.ClaimToken = nil
	m.UpdatedAt = stamp
	m.Version++
	return nil
}

// Complete marks the message handled. The fact is confirmed and is never
// delivered again. An expired lease is rejected; the terminal must claim the
// message again so a stalled terminal cannot confirm a fact someone else may
// have taken over.
func (m *OutboxMessage) Complete(token, note string, now time.Time) error {
	if err := m.requireLiveClaim(token, now); err != nil {
		return err
	}
	stamp := now.UTC()
	holder := *m.ClaimedBy
	m.State = MessageHandled
	m.HandledBy = &holder
	m.HandledAt = &stamp
	m.ClaimToken = nil
	m.ClaimExpiresAt = nil
	if strings.TrimSpace(note) != "" {
		if m.Details == nil {
			m.Details = map[string]string{}
		}
		m.Details["resolution_note"] = strings.TrimSpace(note)
	}
	m.UpdatedAt = stamp
	m.Version++
	return nil
}

// RecoverInterruptedClaim is used after a process restart. The terminal that
// held the claim is gone, so the message returns to pending without being
// lost. It reports whether any state changed.
func (m *OutboxMessage) RecoverInterruptedClaim(now time.Time) bool {
	if m.State != MessageClaimed {
		return false
	}
	stamp := now.UTC()
	m.State = MessagePending
	m.ClaimedBy = nil
	m.ClaimedAt = nil
	m.ClaimExpiresAt = nil
	m.ClaimToken = nil
	m.UpdatedAt = stamp
	m.Version++
	return true
}

// RequeueExpiredClaim requeues a live-record claim whose lease has elapsed
// without a completed restart. Unlike a marked failure it records no failure
// reason; the lease simply lapsed. It reports whether the message changed.
func (m *OutboxMessage) RequeueExpiredClaim(now time.Time) bool {
	if m.State != MessageClaimed || m.ClaimExpiresAt == nil {
		return false
	}
	if now.UTC().Before(*m.ClaimExpiresAt) {
		return false
	}
	stamp := now.UTC()
	m.State = MessagePending
	m.ClaimedBy = nil
	m.ClaimedAt = nil
	m.ClaimExpiresAt = nil
	m.ClaimToken = nil
	m.UpdatedAt = stamp
	m.Version++
	return true
}

func (m *OutboxMessage) requireLiveClaim(token string, now time.Time) error {
	if m.State != MessageClaimed {
		return NewError(CodeMessageState, "only a claimed message can be processed")
	}
	if m.ClaimToken == nil || *m.ClaimToken != token {
		return NewError(CodeMessageClaim,
			"the claim token does not match the terminal holding the message")
	}
	if m.ClaimExpiresAt == nil || !now.UTC().Before(*m.ClaimExpiresAt) {
		return NewError(CodeMessageClaim,
			"the claim has expired; claim the message again")
	}
	return nil
}

// NewClaimToken generates an opaque capability token for one claim.
func NewClaimToken() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("generate claim token: %w", err)
	}
	return "clm_" + hex.EncodeToString(buffer), nil
}
