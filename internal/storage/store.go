package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

const stateFileName = "state.json"

type Store struct {
	mu       sync.RWMutex
	path     string
	snapshot domain.Snapshot
	now      func() time.Time
}

func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	store := &Store{
		path:     filepath.Join(dataDir, stateFileName),
		snapshot: domain.EmptySnapshot(),
		now:      func() time.Time { return time.Now().UTC() },
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) load() error {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}
	var loaded domain.Snapshot
	if err := json.Unmarshal(raw, &loaded); err != nil {
		return fmt.Errorf("decode state: %w", err)
	}
	normalized, err := normalizeSnapshot(loaded)
	if err != nil {
		return err
	}
	s.snapshot = normalized
	return nil
}

func normalizeSnapshot(value domain.Snapshot) (domain.Snapshot, error) {
	if value.Revision < 0 {
		return domain.Snapshot{}, fmt.Errorf("state revision cannot be negative")
	}
	if value.Formulas == nil {
		value.Formulas = map[string]domain.Formula{}
	}
	if value.Vessels == nil {
		value.Vessels = map[string]domain.Vessel{}
	}
	if value.Runs == nil {
		value.Runs = map[string]domain.FermentationRun{}
	}
	if value.NameIndex == nil {
		value.NameIndex = map[string]string{}
	}
	if value.VesselIndex == nil {
		value.VesselIndex = map[string]string{}
	}
	if value.RunIndex == nil {
		value.RunIndex = map[string]string{}
	}
	if value.Messages == nil {
		value.Messages = map[string]domain.OutboxMessage{}
	}
	if value.MessageDedup == nil {
		value.MessageDedup = map[string]string{}
	}
	if value.MessageSequence < 0 {
		return domain.Snapshot{}, fmt.Errorf("message sequence cannot be negative")
	}
	if err := validateIndexes(value); err != nil {
		return domain.Snapshot{}, err
	}
	if err := validateMessages(&value); err != nil {
		return domain.Snapshot{}, err
	}
	return value, nil
}

func validateIndexes(value domain.Snapshot) error {
	expectedNames := make(map[string]string, len(value.Formulas))
	for id, formula := range value.Formulas {
		key := domain.NameKey(formula.Name)
		if key == "" {
			return fmt.Errorf("formula %s has an empty normalized name", id)
		}
		if existing, found := expectedNames[key]; found && existing != id {
			return fmt.Errorf("formula name index contains a duplicate key")
		}
		expectedNames[key] = id
	}
	if err := compareIndexes("formula name", value.NameIndex, expectedNames); err != nil {
		return err
	}

	expectedVessels := make(map[string]string, len(value.Vessels))
	for id, vessel := range value.Vessels {
		if existing, found := expectedVessels[vessel.Code]; found && existing != id {
			return fmt.Errorf("vessel index contains a duplicate code")
		}
		expectedVessels[vessel.Code] = id
	}
	if err := compareIndexes("vessel code", value.VesselIndex, expectedVessels); err != nil {
		return err
	}

	expectedRuns := make(map[string]string, len(value.Runs))
	for id, run := range value.Runs {
		if existing, found := expectedRuns[run.Code]; found && existing != id {
			return fmt.Errorf("run index contains a duplicate code")
		}
		expectedRuns[run.Code] = id
	}
	if err := compareIndexes("run code", value.RunIndex, expectedRuns); err != nil {
		return err
	}

	expectedMessages := make(map[string]string, len(value.Messages))
	for id, message := range value.Messages {
		if message.DedupKey == "" {
			return fmt.Errorf("message %s has an empty dedup key", id)
		}
		if existing, found := expectedMessages[message.DedupKey]; found && existing != id {
			return fmt.Errorf("message dedup index contains a duplicate key")
		}
		expectedMessages[message.DedupKey] = id
	}
	return compareIndexes("message dedup", value.MessageDedup, expectedMessages)
}

// validateMessages enforces the state invariants the recovery path relies on
// and reconciles the derived monotonic sequence counter without inventing
// business data.
func validateMessages(value *domain.Snapshot) error {
	var maxSequence int64
	for id, message := range value.Messages {
		if message.ID != id {
			return fmt.Errorf("message %s does not own its snapshot key", id)
		}
		if message.Sequence < 1 {
			return fmt.Errorf("message %s has an invalid sequence", id)
		}
		if message.Sequence > maxSequence {
			maxSequence = message.Sequence
		}
		switch message.State {
		case domain.MessagePending, domain.MessageClaimed, domain.MessageHandled:
		default:
			return fmt.Errorf("message %s has an unknown state %q", id, message.State)
		}
		if message.State == domain.MessageHandled {
			if message.HandledAt == nil {
				return fmt.Errorf("handled message %s lacks a handled timestamp", id)
			}
			if message.ClaimToken != "" {
				return fmt.Errorf("handled message %s must not retain a claim token", id)
			}
		}
		if message.State == domain.MessageClaimed {
			if message.ClaimToken == "" || message.ClaimedBy == "" ||
				message.LeaseExpiresAt == nil {
				return fmt.Errorf("claimed message %s lacks claim metadata", id)
			}
		}
	}
	if value.MessageSequence < maxSequence {
		value.MessageSequence = maxSequence
	}
	return nil
}

func compareIndexes(label string, actual, expected map[string]string) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("%s index does not match its collection", label)
	}
	for key, expectedID := range expected {
		actualID, found := actual[key]
		if !found || actualID != expectedID {
			return fmt.Errorf("%s index does not match its collection", label)
		}
	}
	return nil
}

func (s *Store) Revision() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot.Revision
}

func (s *Store) View(visitor func(domain.Snapshot) error) error {
	s.mu.RLock()
	copied := s.snapshot.Clone()
	s.mu.RUnlock()
	return visitor(copied)
}

func (s *Store) Update(expected *int64, mutate func(*domain.Snapshot) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if expected != nil && *expected != s.snapshot.Revision {
		return domain.NewError(domain.CodeRevisionConflict,
			"the supplied revision does not match the current snapshot")
	}
	candidate := s.snapshot.Clone()
	if err := mutate(&candidate); err != nil {
		return err
	}
	candidate.Revision = s.snapshot.Revision + 1
	candidate.UpdatedAt = s.now()
	if err := writeSnapshot(s.path, candidate); err != nil {
		return domain.NewError(domain.CodeStorage, err.Error())
	}
	s.snapshot = candidate
	return nil
}

func (s *Store) Execute(mutator func(*domain.Snapshot) error) error {
	return s.Update(nil, mutator)
}
