package workflowcheck

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"os"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/httpapi"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/outboxcli"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
)

type outboxMessageView struct {
	ID        string              `json:"id"`
	Sequence  int64               `json:"sequence"`
	DedupKey  string              `json:"dedup_key"`
	Type      string              `json:"type"`
	Subject   string              `json:"subject"`
	Recipient string              `json:"recipient"`
	Priority  string              `json:"priority"`
	State     domain.MessageState `json:"state"`
	RunID     *string             `json:"run_id"`
	VesselID  *string             `json:"vessel_id"`
	FormulaID string              `json:"formula_id"`
	Attempts  int                 `json:"attempts"`
	Failures  []struct {
		Reason string    `json:"reason"`
		At     time.Time `json:"at"`
	} `json:"failures"`
	ClaimedBy      *string    `json:"claimed_by"`
	ClaimExpiresAt *time.Time `json:"claim_expires_at"`
	HandledBy      *string    `json:"handled_by"`
}

type outboxListResponse struct {
	Messages []outboxMessageView `json:"messages"`
}

type claimEnvelope struct {
	Message outboxMessageView `json:"message"`
	Claim   struct {
		Token string `json:"token"`
	} `json:"claim"`
}

// outboxHarness owns one snapshot directory and lets the check simulate
// service restarts by reopening the durable state.
type outboxHarness struct {
	dataDir string
	server  *httptest.Server
	api     *client
}

func newOutboxHarness() (*outboxHarness, error) {
	dataDir, err := os.MkdirTemp("", "cellar-outbox-check-*")
	if err != nil {
		return nil, fmt.Errorf("create check directory: %w", err)
	}
	harness := &outboxHarness{dataDir: dataDir}
	if err := harness.restart(); err != nil {
		return nil, err
	}
	return harness, nil
}

func (h *outboxHarness) restart() error {
	if h.server != nil {
		h.server.Close()
	}
	store, err := storage.Open(h.dataDir)
	if err != nil {
		return err
	}
	app := service.New(store, clock.System{})
	h.server = httptest.NewServer(httpapi.NewHandler(app))
	h.api = newClient(h.server.URL)
	return nil
}

func (h *outboxHarness) close() {
	if h.server != nil {
		h.server.Close()
	}
	_ = os.RemoveAll(h.dataDir)
}

func localOutbox(api *client) error {
	harness, err := newOutboxHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	outbox := harness.api

	formula, err := ensureApprovedFormula(outbox, "Outbox Formula")
	if err != nil {
		return err
	}
	vessel, err := createCheckVessel(outbox, "VSL-303", 500)
	if err != nil {
		return err
	}
	started, err := createStartedRun(outbox, formula.ID, vessel.ID, "RUN-OUTBOX")
	if err != nil {
		return err
	}

	// A hot observation crosses the formula's maximum fermentation
	// temperature (22.0 Celsius in the check payloads).
	hot := time.Now().UTC().Add(10 * time.Second)
	hotResponse, err := outbox.call("POST",
		"/v1/runs/"+started.Run.ID+"/observations", map[string]any{
			"sequence":      1,
			"gravity":       1.050,
			"temperature_c": 24.0,
			"observed_at":   hot,
			"note":          "too warm",
		}, ptrInt64(started.Run.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(hotResponse, 200, "append hot observation"); err != nil {
		return err
	}
	var hotRun runView
	if err := hotResponse.decode(&hotRun); err != nil {
		return err
	}

	// Complete the lifecycle so the cleaning-finished fact is produced.
	secondAt := hot.Add(10 * time.Second)
	secondResponse, err := outbox.call("POST",
		"/v1/runs/"+started.Run.ID+"/observations", map[string]any{
			"sequence":      2,
			"gravity":       1.030,
			"temperature_c": 20.0,
			"observed_at":   secondAt,
		}, ptrInt64(hotRun.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(secondResponse, 200, "append second observation"); err != nil {
		return err
	}
	if err := secondResponse.decode(&hotRun); err != nil {
		return err
	}
	thirdAt := secondAt.Add(10 * time.Second)
	thirdResponse, err := outbox.call("POST",
		"/v1/runs/"+started.Run.ID+"/observations", map[string]any{
			"sequence":      3,
			"gravity":       1.012,
			"temperature_c": 19.5,
			"observed_at":   thirdAt,
		}, ptrInt64(hotRun.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(thirdResponse, 200, "append third observation"); err != nil {
		return err
	}
	if err := thirdResponse.decode(&hotRun); err != nil {
		return err
	}
	conditioningResponse, err := outbox.call("POST",
		"/v1/runs/"+started.Run.ID+"/conditioning", nil, ptrInt64(hotRun.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(conditioningResponse, 200, "conditioning for outbox"); err != nil {
		return err
	}
	if err := conditioningResponse.decode(&hotRun); err != nil {
		return err
	}
	completeResponse, err := outbox.call("POST",
		"/v1/runs/"+started.Run.ID+"/complete", nil, ptrInt64(hotRun.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(completeResponse, 200, "complete for cleaning"); err != nil {
		return err
	}
	var completedPair pairView
	if err := completeResponse.decode(&completedPair); err != nil {
		return err
	}
	finishResponse, err := outbox.call("POST",
		"/v1/vessels/"+vessel.ID+"/finish-cleaning", nil,
		ptrInt64(completedPair.Vessel.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(finishResponse, 200, "finish cleaning"); err != nil {
		return err
	}

	byType, err := listMessages(outbox, "")
	if err != nil {
		return err
	}
	if err := requireMessageTypes(byType, map[string]int{
		string(domain.MessageFormulaApproved):      1,
		string(domain.MessageFermentationStarted):  1,
		string(domain.MessageTemperatureExcursion): 1,
		string(domain.MessageCleaningFinished):     1,
	}); err != nil {
		return err
	}
	for _, message := range byType {
		if message.Subject == "" || message.Recipient == "" {
			return errInvalidWorkflow("message is missing its recipient or subject")
		}
	}
	hotMessage, err := findMessage(byType, string(domain.MessageTemperatureExcursion))
	if err != nil {
		return err
	}
	if hotMessage.Priority != domain.MessagePriorityHigh || hotMessage.RunID == nil ||
		*hotMessage.RunID != started.Run.ID {
		return errInvalidWorkflow("temperature excursion is not linked to the run as high priority")
	}

	// Replaying a business action must not produce a duplicate message.
	replayed, err := outbox.call("POST", "/v1/formulas/"+formula.ID+"/approve", nil, ptrInt64(formula.Version))
	if err != nil {
		return err
	}
	if err := requireCode(replayed, 409, string(domain.CodeFormulaState),
		"replay formula approval"); err != nil {
		return err
	}
	afterReplay, err := listMessages(outbox, "")
	if err != nil {
		return err
	}
	if len(afterReplay) != len(byType) {
		return errInvalidWorkflow("a confirmed fact was published a second time")
	}

	// Claiming is exclusive and requires an operator.
	missingOperator, err := outbox.call("POST",
		"/v1/outbox/messages/"+hotMessage.ID+"/claim",
		map[string]any{}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(missingOperator, 400, string(domain.CodeInvalidInput),
		"claim without operator"); err != nil {
		return err
	}

	firstClaim, err := claimMessage(outbox, hotMessage.ID, "terminal-A", 60)
	if err != nil {
		return err
	}
	if firstClaim.Message.State != domain.MessageClaimed ||
		firstClaim.Claim.Token == "" {
		return errInvalidWorkflow("claim did not return a held message with a token")
	}
	if firstClaim.Message.ClaimedBy == nil || *firstClaim.Message.ClaimedBy != "terminal-A" {
		return errInvalidWorkflow("claim holder was not recorded")
	}

	// A second terminal cannot take the same message.
	secondClaim, err := outbox.call("POST",
		"/v1/outbox/messages/"+hotMessage.ID+"/claim",
		map[string]any{"operator": "terminal-B", "claim_seconds": 60}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(secondClaim, 409, string(domain.CodeMessageClaim),
		"second terminal claims held message"); err != nil {
		return err
	}

	// The holder cannot complete or fail the work without its token.
	wrongToken, err := outbox.callWithClaim("POST",
		"/v1/outbox/messages/"+hotMessage.ID+"/complete",
		map[string]any{"note": "x"}, stringPtr("clm-not-the-token"))
	if err != nil {
		return err
	}
	if err := requireCode(wrongToken, 409, string(domain.CodeMessageClaim),
		"complete with wrong token"); err != nil {
		return err
	}
	noToken, err := outbox.call("POST",
		"/v1/outbox/messages/"+hotMessage.ID+"/complete", map[string]any{}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(noToken, 400, string(domain.CodeInvalidInput),
		"complete without token header"); err != nil {
		return err
	}

	// Marking a failure requeues the message without losing its history.
	failed, err := outbox.callWithClaim("POST",
		"/v1/outbox/messages/"+hotMessage.ID+"/fail",
		map[string]any{"reason": "terminal interrupted"},
		stringPtr(firstClaim.Claim.Token))
	if err != nil {
		return err
	}
	if err := requireStatus(failed, 200, "fail claimed message"); err != nil {
		return err
	}
	var failedView outboxMessageView
	if err := failed.decode(&failedView); err != nil {
		return err
	}
	if failedView.State != domain.MessagePending || failedView.Attempts != 1 ||
		len(failedView.Failures) != 1 || failedView.Failures[0].Reason != "terminal interrupted" {
		return errInvalidWorkflow("failed message was not requeued with its failure record")
	}

	// Another terminal can now claim and complete it.
	reclaimed, err := claimMessage(outbox, hotMessage.ID, "terminal-B", 60)
	if err != nil {
		return err
	}
	completedMessage, err := outbox.callWithClaim("POST",
		"/v1/outbox/messages/"+hotMessage.ID+"/complete",
		map[string]any{"note": "log reviewed"},
		stringPtr(reclaimed.Claim.Token))
	if err != nil {
		return err
	}
	if err := requireStatus(completedMessage, 200, "complete requeued message"); err != nil {
		return err
	}
	var handledView outboxMessageView
	if err := completedMessage.decode(&handledView); err != nil {
		return err
	}
	if handledView.State != domain.MessageHandled || handledView.HandledBy == nil ||
		*handledView.HandledBy != "terminal-B" {
		return errInvalidWorkflow("message was not recorded as handled by its claim holder")
	}

	// A handled message cannot be claimed or delivered again.
	thirdClaim, err := outbox.call("POST",
		"/v1/outbox/messages/"+hotMessage.ID+"/claim",
		map[string]any{"operator": "terminal-C", "claim_seconds": 60}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(thirdClaim, 409, string(domain.CodeMessageState),
		"claim handled message"); err != nil {
		return err
	}

	// A restart while a message is claimed must return it to pending.
	pendingList, err := listMessages(outbox, string(domain.MessagePending))
	if err != nil {
		return err
	}
	if len(pendingList) == 0 {
		return errInvalidWorkflow("expected remaining pending messages")
	}
	victim := pendingList[0]
	if _, err := claimMessage(outbox, victim.ID, "terminal-A", 120); err != nil {
		return err
	}
	if err := harness.restart(); err != nil {
		return err
	}
	recovered, err := getMessage(harness.api, victim.ID)
	if err != nil {
		return err
	}
	if recovered.State != domain.MessagePending || recovered.ClaimedBy != nil {
		return errInvalidWorkflow("restart lost or retained a stale claim instead of requeuing")
	}

	// Every message, including the recovered one, survives the restart.
	persisted, err := listMessages(harness.api, "")
	if err != nil {
		return err
	}
	if len(persisted) != len(byType) {
		return errInvalidWorkflow("restart lost outbox messages")
	}

	// A second restart on an all-pending/handled queue must not invent messages.
	if err := harness.restart(); err != nil {
		return err
	}
	again, err := listMessages(harness.api, "")
	if err != nil {
		return err
	}
	if len(again) != len(byType) {
		return errInvalidWorkflow("recovery duplicated outbox messages")
	}

	if err := verifyLocalOutboxTerminal(harness.dataDir, byType); err != nil {
		return err
	}
	return nil
}

func listMessages(api *client, state string) ([]outboxMessageView, error) {
	path := "/v1/outbox/messages"
	if state != "" {
		path += "?state=" + state
	}
	response, err := api.call("GET", path, nil, nil)
	if err != nil {
		return nil, err
	}
	if err := requireStatus(response, 200, "list messages"); err != nil {
		return nil, err
	}
	var decoded outboxListResponse
	if err := response.decode(&decoded); err != nil {
		return nil, err
	}
	return decoded.Messages, nil
}

func getMessage(api *client, id string) (outboxMessageView, error) {
	response, err := api.call("GET", "/v1/outbox/messages/"+id, nil, nil)
	if err != nil {
		return outboxMessageView{}, err
	}
	if err := requireStatus(response, 200, "get message"); err != nil {
		return outboxMessageView{}, err
	}
	var decoded outboxMessageView
	if err := response.decode(&decoded); err != nil {
		return outboxMessageView{}, err
	}
	return decoded, nil
}

func claimMessage(api *client, id, operator string, ttlSeconds int) (claimEnvelope, error) {
	response, err := api.call("POST", "/v1/outbox/messages/"+id+"/claim", map[string]any{
		"operator":      operator,
		"claim_seconds": ttlSeconds,
	}, nil)
	if err != nil {
		return claimEnvelope{}, err
	}
	if err := requireStatus(response, 200, "claim message"); err != nil {
		return claimEnvelope{}, err
	}
	var decoded claimEnvelope
	if err := response.decode(&decoded); err != nil {
		return claimEnvelope{}, err
	}
	return decoded, nil
}

func findMessage(messages []outboxMessageView, messageType string) (outboxMessageView, error) {
	for _, message := range messages {
		if message.Type == messageType {
			return message, nil
		}
	}
	return outboxMessageView{}, fmt.Errorf("expected a %s message", messageType)
}

func requireMessageTypes(messages []outboxMessageView, expected map[string]int) error {
	counts := make(map[string]int)
	for _, message := range messages {
		counts[message.Type]++
	}
	for messageType, count := range expected {
		if counts[messageType] != count {
			return fmt.Errorf("expected %d %s messages, got %d",
				count, messageType, counts[messageType])
		}
	}
	if len(counts) != len(expected) {
		return fmt.Errorf("unexpected outbox message types: %v", counts)
	}
	return nil
}

func stringPtr(value string) *string {
	return &value
}

// verifyLocalOutboxTerminal exercises the on-machine command-line interface
// directly against the same snapshot directory; no network is involved.
func verifyLocalOutboxTerminal(dataDir string, expected []outboxMessageView) error {
	original := os.Getenv("CELLAR_CONTROL_DATA_DIR")
	if err := os.Setenv("CELLAR_CONTROL_DATA_DIR", dataDir); err != nil {
		return err
	}
	defer func() {
		if original == "" {
			_ = os.Unsetenv("CELLAR_CONTROL_DATA_DIR")
		} else {
			_ = os.Setenv("CELLAR_CONTROL_DATA_DIR", original)
		}
	}()

	var output bytes.Buffer
	if err := outboxcli.Run([]string{"list", "-data-dir", dataDir}, &output); err != nil {
		return fmt.Errorf("local outbox terminal list failed: %w", err)
	}
	printed := output.String()
	if !bytes.Contains([]byte(printed), []byte("formula_approved")) ||
		!bytes.Contains([]byte(printed), []byte("cleaning_finished")) {
		return fmt.Errorf("local outbox terminal did not list business messages:\n%s", printed)
	}

	hot, err := findMessage(expected, string(domain.MessageTemperatureExcursion))
	if err != nil {
		return err
	}
	output.Reset()
	if err := outboxcli.Run([]string{"show", "-data-dir", dataDir, hot.ID}, &output); err != nil {
		return fmt.Errorf("local outbox terminal show failed: %w", err)
	}
	detail := output.String()
	if !bytes.Contains([]byte(detail), []byte(hot.ID)) ||
		!bytes.Contains([]byte(detail), []byte("recipient: cellar_operator")) {
		return fmt.Errorf("local outbox terminal did not show message details:\n%s", detail)
	}
	return nil
}
