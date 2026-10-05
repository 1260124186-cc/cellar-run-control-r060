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
	if err := store.recoverClaimedMessages(); err != nil {
		return nil, err
	}
	return store, nil
}

// OpenView opens an existing snapshot for read-only inspection, e.g. a local
// command-line terminal. It never creates files, mutates state, or requeues
// claims. A missing snapshot is reported as an error so operators notice a
// wrong data directory.
func OpenView(dataDir string) (*Store, error) {
	store := &Store{
		path:     filepath.Join(dataDir, stateFileName),
		snapshot: domain.EmptySnapshot(),
		now:      func() time.Time { return time.Now().UTC() },
	}
	if _, err := os.Stat(store.path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("no state snapshot found in %s", dataDir)
		}
		return nil, fmt.Errorf("inspect state: %w", err)
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

// recoverClaimedMessages requeues messages left in the claimed state by a
// previous process. A claim is an in-memory lease held by one terminal, so a
// restart cannot trust it; the message must not be lost and must become
// claimable again.
func (s *Store) recoverClaimedMessages() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	changed := false
	candidate := s.snapshot.Clone()
	for id, message := range candidate.Outbox {
		if message.RecoverInterruptedClaim(s.now()) {
			candidate.Outbox[id] = message
			changed = true
		}
	}
	if !changed {
		return nil
	}
	candidate.Revision = s.snapshot.Revision + 1
	candidate.UpdatedAt = s.now()
	if err := writeSnapshot(s.path, candidate); err != nil {
		return domain.NewError(domain.CodeStorage, err.Error())
	}
	s.snapshot = candidate
	return nil
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
	if value.Outbox == nil {
		value.Outbox = map[string]domain.OutboxMessage{}
	}
	if value.OutboxDedup == nil {
		value.OutboxDedup = map[string]string{}
	}
	if value.OutboxCounter < 0 {
		return domain.Snapshot{}, fmt.Errorf("outbox sequence cannot be negative")
	}
	if err := validateIndexes(value); err != nil {
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

	expectedDedup := make(map[string]string, len(value.Outbox))
	highestSequence := int64(0)
	for id, message := range value.Outbox {
		if err := message.Validate(); err != nil {
			return err
		}
		if existing, found := expectedDedup[message.DedupKey]; found && existing != id {
			return fmt.Errorf("outbox dedup index contains a duplicate key")
		}
		expectedDedup[message.DedupKey] = id
		if message.Sequence > highestSequence {
			highestSequence = message.Sequence
		}
	}
	if err := compareIndexes("outbox dedup", value.OutboxDedup, expectedDedup); err != nil {
		return err
	}
	if value.OutboxCounter < highestSequence {
		return fmt.Errorf("outbox sequence is behind the highest stored message")
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
