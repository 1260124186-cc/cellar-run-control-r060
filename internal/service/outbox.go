package service

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

// DefaultClaimLease bounds how long one terminal can hold a message. A held
// message becomes claimable again after the lease expires, so a crash or
// restart while processing never strands an unfinished message.
const DefaultClaimLease = 5 * time.Minute

// OutboxMessageView is the external representation. The claim token is only
// handed back to the terminal that successfully claimed the message; listing
// and reading never reveal it.
type OutboxMessageView struct {
	ID             string         `json:"id"`
	Sequence       int64          `json:"sequence"`
	Type           string         `json:"type"`
	Recipient      string         `json:"recipient"`
	RunID          string         `json:"run_id,omitempty"`
	Subject        string         `json:"subject"`
	Detail         string         `json:"detail"`
	Payload        map[string]any `json:"payload,omitempty"`
	State          string         `json:"state"`
	ClaimedBy      string         `json:"claimed_by,omitempty"`
	Attempts       int            `json:"attempts"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	ClaimedAt      *time.Time     `json:"claimed_at,omitempty"`
	LeaseExpiresAt *time.Time     `json:"lease_expires_at,omitempty"`
	HandledAt      *time.Time     `json:"handled_at,omitempty"`
	ClaimToken     string         `json:"claim_token,omitempty"`
}

type ListMessagesInput struct {
	State     string
	Recipient string
	Type      string
	RunID     string
}

type ClaimMessageInput struct {
	TerminalID string
	Recipient  string
}

type CompleteMessageInput struct {
	TerminalID string
	ClaimToken string
}

type ReleaseMessageInput struct {
	TerminalID string
	ClaimToken string
}

func messageView(message domain.OutboxMessage) OutboxMessageView {
	view := OutboxMessageView{
		ID:             message.ID,
		Sequence:       message.Sequence,
		Type:           message.Type,
		Recipient:      message.Recipient,
		RunID:          message.RunID,
		Subject:        message.Subject,
		Detail:         message.Detail,
		State:          string(message.State),
		ClaimedBy:      message.ClaimedBy,
		Attempts:       message.Attempts,
		CreatedAt:      message.CreatedAt,
		UpdatedAt:      message.UpdatedAt,
		ClaimedAt:      message.ClaimedAt,
		LeaseExpiresAt: message.LeaseExpiresAt,
		HandledAt:      message.HandledAt,
	}
	if message.Payload != nil {
		view.Payload = make(map[string]any, len(message.Payload))
		for key, value := range message.Payload {
			view.Payload[key] = value
		}
	}
	return view
}

// emitMessage creates one message inside an open snapshot mutation. It must
// be called inside the same store.Execute callback as the business change so
// the fact and its notification share one atomic revision. The dedup key plus
// state-machine guards ensure a confirmed fact is never published twice.
func emitMessage(
	snapshot *domain.Snapshot,
	dedupKey, messageType, recipient, runID, subject, detail string,
	payload map[string]any,
	now time.Time,
) error {
	if _, exists := snapshot.MessageDedup[dedupKey]; exists {
		return domain.NewError(domain.CodeStorage,
			"outbox message dedup key already exists")
	}
	id, err := domain.NewID("msg")
	if err != nil {
		return domain.NewError(domain.CodeStorage, err.Error())
	}
	snapshot.MessageSequence++
	message := domain.OutboxMessage{
		ID:        id,
		Sequence:  snapshot.MessageSequence,
		DedupKey:  dedupKey,
		Type:      messageType,
		Recipient: recipient,
		RunID:     runID,
		Subject:   subject,
		Detail:    detail,
		State:     domain.MessagePending,
		CreatedAt: now.UTC(),
		UpdatedAt: now.UTC(),
	}
	if len(payload) > 0 {
		message.Payload = make(map[string]any, len(payload))
		for key, value := range payload {
			message.Payload[key] = value
		}
	}
	snapshot.Messages[id] = message
	snapshot.MessageDedup[dedupKey] = id
	return nil
}

func newClaimToken() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("generate claim token: %w", err)
	}
	return hex.EncodeToString(buffer), nil
}

func (s *Service) ListMessages(input ListMessagesInput) ([]OutboxMessageView, error) {
	stateFilter, err := normalizeStateFilter(input.State)
	if err != nil {
		return nil, err
	}
	var views []OutboxMessageView
	err = s.store.View(func(snapshot domain.Snapshot) error {
		views = make([]OutboxMessageView, 0, len(snapshot.Messages))
		for _, message := range snapshot.Messages {
			if !matchesMessageFilters(message, stateFilter, input.Recipient, input.Type, input.RunID) {
				continue
			}
			views = append(views, messageView(message.Clone()))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(views, func(i, j int) bool {
		return views[i].Sequence < views[j].Sequence
	})
	return views, nil
}

func (s *Service) GetMessage(id string) (OutboxMessageView, error) {
	var found OutboxMessageView
	err := s.store.View(func(snapshot domain.Snapshot) error {
		message, ok := snapshot.Messages[id]
		if !ok {
			return domain.NewError(domain.CodeMessageNotFound, "message was not found")
		}
		found = messageView(message.Clone())
		return nil
	})
	if err != nil {
		return OutboxMessageView{}, err
	}
	return found, nil
}

// ClaimNextMessage atomically assigns the oldest eligible message to one
// terminal. An optional recipient restricts the queue. Expired leases are
// reclaimable: the stale token is replaced and the old terminal can no longer
// complete the message.
func (s *Service) ClaimNextMessage(input ClaimMessageInput) (OutboxMessageView, error) {
	terminalID := normalizeTerminalID(input.TerminalID)
	if terminalID == "" {
		return OutboxMessageView{}, domain.FieldError(domain.CodeInvalidInput,
			"terminal_id", "is required")
	}
	recipient := strings.TrimSpace(input.Recipient)
	token, err := newClaimToken()
	if err != nil {
		return OutboxMessageView{}, domain.NewError(domain.CodeStorage, err.Error())
	}
	var claimed OutboxMessageView
	err = s.store.Execute(func(snapshot *domain.Snapshot) error {
		now := s.clock.Now()
		eligible, ok := nextClaimableMessage(*snapshot, now, recipient)
		if !ok {
			return domain.NewError(domain.CodeMessageNotClaimable,
				"no pending message is available to claim")
		}
		leaseExpiresAt := now.Add(DefaultClaimLease)
		eligible.AssignClaim(terminalID, token, now, leaseExpiresAt)
		snapshot.Messages[eligible.ID] = eligible
		claimed = messageView(eligible.Clone())
		claimed.ClaimToken = token
		return nil
	})
	if err != nil {
		return OutboxMessageView{}, err
	}
	return claimed, nil
}

// ClaimMessageByID claims one explicitly selected message. A terminal uses
// this after choosing a row in the local interface.
func (s *Service) ClaimMessageByID(id string, input ClaimMessageInput) (OutboxMessageView, error) {
	terminalID := normalizeTerminalID(input.TerminalID)
	if terminalID == "" {
		return OutboxMessageView{}, domain.FieldError(domain.CodeInvalidInput,
			"terminal_id", "is required")
	}
	token, err := newClaimToken()
	if err != nil {
		return OutboxMessageView{}, domain.NewError(domain.CodeStorage, err.Error())
	}
	var claimed OutboxMessageView
	err = s.store.Execute(func(snapshot *domain.Snapshot) error {
		message, ok := snapshot.Messages[id]
		if !ok {
			return domain.NewError(domain.CodeMessageNotFound, "message was not found")
		}
		now := s.clock.Now()
		if !message.IsClaimable(now) {
			return domain.NewError(domain.CodeMessageNotClaimable,
				"message is not available to claim")
		}
		message.AssignClaim(terminalID, token, now, now.Add(DefaultClaimLease))
		snapshot.Messages[id] = message
		claimed = messageView(message.Clone())
		claimed.ClaimToken = token
		return nil
	})
	if err != nil {
		return OutboxMessageView{}, err
	}
	return claimed, nil
}

func nextClaimableMessage(snapshot domain.Snapshot, now time.Time, recipient string) (domain.OutboxMessage, bool) {
	var chosen domain.OutboxMessage
	found := false
	for _, message := range snapshot.Messages {
		if recipient != "" && message.Recipient != recipient {
			continue
		}
		if !message.IsClaimable(now) {
			continue
		}
		if !found || message.Sequence < chosen.Sequence {
			chosen = message
			found = true
		}
	}
	return chosen, found
}

func (s *Service) CompleteMessage(id string, input CompleteMessageInput) (OutboxMessageView, error) {
	token := normalizeTerminalID(input.ClaimToken)
	if token == "" {
		return OutboxMessageView{}, domain.FieldError(domain.CodeInvalidInput,
			"claim_token", "is required")
	}
	if normalizeTerminalID(input.TerminalID) == "" {
		return OutboxMessageView{}, domain.FieldError(domain.CodeInvalidInput,
			"terminal_id", "is required")
	}
	var updated OutboxMessageView
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		message, ok := snapshot.Messages[id]
		if !ok {
			return domain.NewError(domain.CodeMessageNotFound, "message was not found")
		}
		if message.ClaimedBy != "" && message.ClaimedBy != normalizeTerminalID(input.TerminalID) {
			return domain.NewError(domain.CodeMessageClaimConflict,
				"message is held by another terminal")
		}
		if err := message.Complete(token, s.clock.Now()); err != nil {
			return err
		}
		snapshot.Messages[id] = message
		updated = messageView(message.Clone())
		return nil
	})
	if err != nil {
		return OutboxMessageView{}, err
	}
	return updated, nil
}

func (s *Service) ReleaseMessage(id string, input ReleaseMessageInput) (OutboxMessageView, error) {
	token := normalizeTerminalID(input.ClaimToken)
	if token == "" {
		return OutboxMessageView{}, domain.FieldError(domain.CodeInvalidInput,
			"claim_token", "is required")
	}
	var updated OutboxMessageView
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		message, ok := snapshot.Messages[id]
		if !ok {
			return domain.NewError(domain.CodeMessageNotFound, "message was not found")
		}
		if err := message.Release(token, s.clock.Now()); err != nil {
			return err
		}
		snapshot.Messages[id] = message
		updated = messageView(message.Clone())
		return nil
	})
	if err != nil {
		return OutboxMessageView{}, err
	}
	return updated, nil
}

func normalizeStateFilter(value string) (domain.MessageState, error) {
	switch value {
	case "":
		return "", nil
	case string(domain.MessagePending):
		return domain.MessagePending, nil
	case string(domain.MessageClaimed):
		return domain.MessageClaimed, nil
	case string(domain.MessageHandled):
		return domain.MessageHandled, nil
	default:
		return "", domain.FieldError(domain.CodeInvalidInput, "state",
			"must be one of pending, claimed, or handled")
	}
}

func matchesMessageFilters(
	message domain.OutboxMessage,
	state domain.MessageState,
	recipient, messageType, runID string,
) bool {
	if state != "" && message.State != state {
		return false
	}
	if recipient != "" && message.Recipient != recipient {
		return false
	}
	if messageType != "" && message.Type != messageType {
		return false
	}
	if runID != "" && message.RunID != runID {
		return false
	}
	return true
}

func normalizeTerminalID(value string) string {
	return strings.Join(strings.Fields(value), " ")
}
