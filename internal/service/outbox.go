package service

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

const (
	defaultClaimTTLSeconds = 120
	minClaimTTLSeconds     = 30
	maxClaimTTLSeconds     = 900
	maxOperatorRunes       = 120
	maxReasonRunes         = 500
	maxNoteRunes           = 500
)

// enqueueInput describes a business fact that must become one outbox message.
// The call happens inside the same store revision as the business write, so
// the fact and its message commit atomically or not at all.
type enqueueInput struct {
	DedupKey  string
	Type      domain.MessageType
	Subject   string
	Recipient string
	Priority  string
	FormulaID string
	RunID     *string
	VesselID  *string
	Details   map[string]string
}

// enqueueMessage appends one message and registers its dedup key. It must be
// called inside a store.Update transaction, after the business fact itself is
// valid. A repeated key is a programming error because the business guard has
// already prevented the second fact.
func enqueueMessage(snapshot *domain.Snapshot, now time.Time, input enqueueInput) error {
	if _, exists := snapshot.OutboxDedup[input.DedupKey]; exists {
		return domain.NewError(domain.CodeMessageState,
			"a message for this confirmed fact already exists")
	}
	id, err := generateEntityID("msg")
	if err != nil {
		return domain.NewError(domain.CodeStorage, err.Error())
	}
	snapshot.OutboxCounter++
	message := domain.OutboxMessage{
		ID:        id,
		Sequence:  snapshot.OutboxCounter,
		DedupKey:  input.DedupKey,
		Type:      input.Type,
		Subject:   input.Subject,
		Recipient: input.Recipient,
		Priority:  input.Priority,
		FormulaID: input.FormulaID,
		RunID:     cloneOptionalString(input.RunID),
		VesselID:  cloneOptionalString(input.VesselID),
		Details:   cloneDetails(input.Details),
		State:     domain.MessagePending,
		CreatedAt: now.UTC(),
		UpdatedAt: now.UTC(),
		Version:   1,
	}
	if err := message.Validate(); err != nil {
		return domain.NewError(domain.CodeStorage, err.Error())
	}
	snapshot.Outbox[message.ID] = message
	snapshot.OutboxDedup[message.DedupKey] = message.ID
	return nil
}

func cloneOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func cloneDetails(value map[string]string) map[string]string {
	if value == nil {
		return nil
	}
	cloned := make(map[string]string, len(value))
	for key, entry := range value {
		cloned[key] = entry
	}
	return cloned
}

func fermentationStartedInput(
	run domain.FermentationRun,
	vessel domain.Vessel,
	formulaName string,
) enqueueInput {
	runID := run.ID
	vesselID := vessel.ID
	return enqueueInput{
		DedupKey:  "fermentation_started:" + run.ID,
		Type:      domain.MessageFermentationStarted,
		Subject:   "Fermentation started: " + run.Code,
		Recipient: domain.RecipientCellarOperator,
		Priority:  domain.MessagePriorityNormal,
		FormulaID: run.FormulaID,
		RunID:     &runID,
		VesselID:  &vesselID,
		Details: map[string]string{
			"run_id":       run.ID,
			"run_code":     run.Code,
			"vessel_id":    vessel.ID,
			"vessel_code":  vessel.Code,
			"formula_id":   run.FormulaID,
			"formula_name": formulaName,
			"started_at":   formatTime(run.StartedAt),
		},
	}
}

func temperatureExcursionInput(
	run domain.FermentationRun,
	vessel *domain.Vessel,
	formula domain.Formula,
	observation domain.GravityObservation,
) enqueueInput {
	runID := run.ID
	input := enqueueInput{
		DedupKey: "temperature_excursion:" + run.ID +
			":" + strconv.Itoa(observation.Sequence),
		Type:      domain.MessageTemperatureExcursion,
		Subject:   "Temperature excursion on " + run.Code,
		Recipient: domain.RecipientCellarOperator,
		Priority:  domain.MessagePriorityHigh,
		FormulaID: run.FormulaID,
		RunID:     &runID,
		Details: map[string]string{
			"run_id":              run.ID,
			"run_code":            run.Code,
			"formula_id":          run.FormulaID,
			"formula_name":        formula.Name,
			"observation_seq":     strconv.Itoa(observation.Sequence),
			"observed_c":          strconv.FormatFloat(observation.TemperatureC, 'f', 2, 64),
			"max_allowed_c":       strconv.FormatFloat(formula.MaxFermentationTempC, 'f', 2, 64),
			"observed_at":         observation.ObservedAt.UTC().Format(time.RFC3339),
			"excursion_direction": "above_max",
		},
	}
	if vessel != nil {
		vesselID := vessel.ID
		input.VesselID = &vesselID
		input.Details["vessel_id"] = vessel.ID
		input.Details["vessel_code"] = vessel.Code
	}
	return input
}

func cleaningFinishedInput(vessel domain.Vessel, run domain.FermentationRun, now time.Time) enqueueInput {
	runID := run.ID
	vesselID := vessel.ID
	return enqueueInput{
		DedupKey:  "cleaning_finished:" + run.ID,
		Type:      domain.MessageCleaningFinished,
		Subject:   "Cleaning completed: " + vessel.Code,
		Recipient: domain.RecipientCellarOperator,
		Priority:  domain.MessagePriorityNormal,
		FormulaID: run.FormulaID,
		RunID:     &runID,
		VesselID:  &vesselID,
		Details: map[string]string{
			"vessel_id":   vessel.ID,
			"vessel_code": vessel.Code,
			"run_id":      run.ID,
			"run_code":    run.Code,
			"finished_at": now.UTC().Format(time.RFC3339),
		},
	}
}

func formatTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

// ListMessages returns messages visible to the local duty interface, with
// stable ordering by creation sequence.
func (s *Service) ListMessages(filter MessageFilter) ([]domain.OutboxMessage, error) {
	if err := validateFilter(filter); err != nil {
		return nil, err
	}
	var messages []domain.OutboxMessage
	err := s.store.View(func(snapshot domain.Snapshot) error {
		messages = make([]domain.OutboxMessage, 0, len(snapshot.Outbox))
		for _, message := range snapshot.Outbox {
			if !matchesFilter(message, filter) {
				continue
			}
			messages = append(messages, message.Clone())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(messages, func(i, j int) bool {
		if messages[i].Sequence == messages[j].Sequence {
			return messages[i].ID < messages[j].ID
		}
		return messages[i].Sequence < messages[j].Sequence
	})
	return messages, nil
}

func validateFilter(filter MessageFilter) error {
	switch filter.State {
	case "", string(domain.MessagePending), string(domain.MessageClaimed),
		string(domain.MessageHandled):
	default:
		return domain.FieldError(domain.CodeInvalidInput, "state",
			"must be pending, claimed, or handled")
	}
	return nil
}

func matchesFilter(message domain.OutboxMessage, filter MessageFilter) bool {
	if filter.State != "" && string(message.State) != filter.State {
		return false
	}
	if filter.Recipient != "" && message.Recipient != filter.Recipient {
		return false
	}
	if filter.Type != "" && string(message.Type) != filter.Type {
		return false
	}
	return true
}

// GetMessage reads a single outbox message.
func (s *Service) GetMessage(id string) (domain.OutboxMessage, error) {
	var found domain.OutboxMessage
	err := s.store.View(func(snapshot domain.Snapshot) error {
		message, ok := snapshot.Outbox[id]
		if !ok {
			return domain.NewError(domain.CodeMessageNotFound, "message was not found")
		}
		found = message.Clone()
		return nil
	})
	if err != nil {
		return domain.OutboxMessage{}, err
	}
	return found, nil
}

// ClaimMessage grants exactly one terminal the exclusive right to handle a
// pending message. The returned token is the capability needed to finish or
// fail the work. A replay by the same holder returns the same token.
func (s *Service) ClaimMessage(
	id string,
	input ClaimMessageInput,
) (domain.OutboxMessage, string, error) {
	operator := strings.TrimSpace(input.Operator)
	if operator == "" {
		return domain.OutboxMessage{}, "",
			domain.FieldError(domain.CodeInvalidInput, "operator", "is required")
	}
	if len([]rune(operator)) > maxOperatorRunes {
		return domain.OutboxMessage{}, "",
			domain.FieldError(domain.CodeInvalidInput, "operator",
				"must not exceed 120 characters")
	}
	ttlSeconds := input.ClaimSeconds
	if ttlSeconds == 0 {
		ttlSeconds = defaultClaimTTLSeconds
	}
	if ttlSeconds < minClaimTTLSeconds || ttlSeconds > maxClaimTTLSeconds {
		return domain.OutboxMessage{}, "",
			domain.FieldError(domain.CodeInvalidInput, "claim_seconds",
				"must be between 30 and 900")
	}
	token, err := domain.NewClaimToken()
	if err != nil {
		return domain.OutboxMessage{}, "", domain.NewError(domain.CodeStorage, err.Error())
	}

	var updated domain.OutboxMessage
	var issuedToken string
	err = s.store.Execute(func(snapshot *domain.Snapshot) error {
		message, ok := snapshot.Outbox[id]
		if !ok {
			return domain.NewError(domain.CodeMessageNotFound, "message was not found")
		}
		issued, _, err := message.Claim(operator, token,
			time.Duration(ttlSeconds)*time.Second, s.clock.Now())
		if err != nil {
			return err
		}
		snapshot.Outbox[id] = message
		updated = message.Clone()
		issuedToken = issued
		return nil
	})
	if err != nil {
		return domain.OutboxMessage{}, "", err
	}
	return updated, issuedToken, nil
}

// CompleteMessage confirms a claim. The message is marked handled by the
// holding terminal and the fact is never presented again.
func (s *Service) CompleteMessage(id string, input CompleteMessageInput) (domain.OutboxMessage, error) {
	if err := requireClaimToken(input.ClaimToken); err != nil {
		return domain.OutboxMessage{}, err
	}
	if len([]rune(input.Note)) > maxNoteRunes {
		return domain.OutboxMessage{},
			domain.FieldError(domain.CodeInvalidInput, "note",
				"must not exceed 500 characters")
	}
	var updated domain.OutboxMessage
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		message, ok := snapshot.Outbox[id]
		if !ok {
			return domain.NewError(domain.CodeMessageNotFound, "message was not found")
		}
		if err := message.Complete(input.ClaimToken, input.Note, s.clock.Now()); err != nil {
			return err
		}
		snapshot.Outbox[id] = message
		updated = message.Clone()
		return nil
	})
	if err != nil {
		return domain.OutboxMessage{}, err
	}
	return updated, nil
}

// FailMessage records a failed handling attempt and returns the message to
// the pending queue so another terminal can pick it up. The failure is not
// lost.
func (s *Service) FailMessage(id string, input FailMessageInput) (domain.OutboxMessage, error) {
	if err := requireClaimToken(input.ClaimToken); err != nil {
		return domain.OutboxMessage{}, err
	}
	reason := strings.TrimSpace(input.Reason)
	if reason == "" {
		return domain.OutboxMessage{},
			domain.FieldError(domain.CodeInvalidInput, "reason", "is required")
	}
	if len([]rune(reason)) > maxReasonRunes {
		return domain.OutboxMessage{},
			domain.FieldError(domain.CodeInvalidInput, "reason",
				"must not exceed 500 characters")
	}
	var updated domain.OutboxMessage
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		message, ok := snapshot.Outbox[id]
		if !ok {
			return domain.NewError(domain.CodeMessageNotFound, "message was not found")
		}
		if err := message.RequeueForRetry(input.ClaimToken, reason, s.clock.Now()); err != nil {
			return err
		}
		snapshot.Outbox[id] = message
		updated = message.Clone()
		return nil
	})
	if err != nil {
		return domain.OutboxMessage{}, err
	}
	return updated, nil
}

func requireClaimToken(token string) error {
	if strings.TrimSpace(token) == "" {
		return domain.FieldError(domain.CodeInvalidInput,
			"X-Claim-Token", "claim token is required")
	}
	return nil
}

// RequeueExpiredClaims returns claims whose leases have elapsed to the
// pending queue in one durable revision. The local duty loop calls it on an
// interval so stalled terminals do not leave messages looking held forever.
func (s *Service) RequeueExpiredClaims() (int, error) {
	requeued := 0
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		for id, message := range snapshot.Outbox {
			if message.RequeueExpiredClaim(s.clock.Now()) {
				snapshot.Outbox[id] = message
				requeued++
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return requeued, nil
}
