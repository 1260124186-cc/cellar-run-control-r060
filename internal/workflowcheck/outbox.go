package workflowcheck

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/httpapi"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
)

type messageView struct {
	ID         string         `json:"id"`
	Sequence   int64          `json:"sequence"`
	Type       string         `json:"type"`
	Recipient  string         `json:"recipient"`
	RunID      string         `json:"run_id"`
	Subject    string         `json:"subject"`
	State      string         `json:"state"`
	ClaimToken string         `json:"claim_token"`
	ClaimedBy  string         `json:"claimed_by"`
	Attempts   int            `json:"attempts"`
	Payload    map[string]any `json:"payload"`
}

func outboxDutyRoom(api *client, clockSource *clock.Fixed, dataDir string) error {
	// --- business actions emit messages ------------------------------------
	formula, err := ensureApprovedFormula(api, "Outbox Formula")
	if err != nil {
		return err
	}
	pending, err := listMessages(api, "?state=pending")
	if err != nil {
		return err
	}
	approval := findMessage(pending, domain.MessageFormulaApproved)
	if approval == nil {
		return errInvalidWorkflow("formula approval did not create an outbox message")
	}
	if approval.Recipient != domain.RecipientDutyPlanner || approval.RunID != "" {
		return errInvalidWorkflow("approval message has the wrong recipient or run link")
	}

	// A rejected re-approval must not publish the same fact twice.
	repeat, err := api.call("POST", "/v1/formulas/"+formula.ID+"/approve", nil, ptrInt64(3))
	if err != nil {
		return err
	}
	if err := requireCode(repeat, http.StatusConflict, string(domain.CodeFormulaState),
		"approve formula twice"); err != nil {
		return err
	}
	if total := countMessages(api, "", domain.MessageFormulaApproved); total != 1 {
		return errInvalidWorkflow("a rejected re-approval duplicated the approval message")
	}

	vessel, err := createCheckVessel(api, "VSL-303", 500)
	if err != nil {
		return err
	}
	started, err := createStartedRun(api, formula.ID, vessel.ID, "RUN-OUTBOX")
	if err != nil {
		return err
	}
	linked, err := listMessages(api, "?run_id="+started.Run.ID)
	if err != nil {
		return err
	}
	startedMessage := findMessage(linked, domain.MessageRunFermentationStarted)
	if startedMessage == nil {
		return errInvalidWorkflow("starting fermentation did not create a linked run message")
	}
	if startedMessage.Recipient != domain.RecipientDutyOperator ||
		startedMessage.Payload["vessel_code"] != "VSL-303" {
		return errInvalidWorkflow("start message is not addressed to the operator with vessel context")
	}

	// Observations: in-range temperature emits nothing, above-limit emits one.
	clockSource.Advance(time.Minute)
	if _, err := appendOutboxObservation(api, started.Run.ID, 1, 21.0, clockSource.Now()); err != nil {
		return err
	}
	clockSource.Advance(time.Minute)
	if _, err := appendOutboxObservation(api, started.Run.ID, 2, 25.5, clockSource.Now()); err != nil {
		return err
	}
	clockSource.Advance(time.Minute)
	if _, err := appendOutboxObservation(api, started.Run.ID, 3, 20.0, clockSource.Now()); err != nil {
		return err
	}
	breaches, err := listMessages(api, "?type="+domain.MessageTemperatureBreach)
	if err != nil {
		return err
	}
	if len(breaches) != 1 {
		return errInvalidWorkflow("expected exactly one temperature breach message")
	}
	breach := breaches[0]
	if breach.Recipient != domain.RecipientDutyObserver || breach.RunID != started.Run.ID {
		return errInvalidWorkflow("breach message is not linked to the run and observer")
	}

	// --- only one terminal can finish a claimed message --------------------
	claimResponse, err := api.call("POST", "/v1/outbox/claims",
		map[string]any{"terminal_id": "desk-alpha"}, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(claimResponse, http.StatusCreated, "claim next message"); err != nil {
		return err
	}
	var claimed messageView
	if err := claimResponse.decode(&claimed); err != nil {
		return err
	}
	if claimed.Sequence != approval.Sequence || claimed.State != "claimed" ||
		claimed.ClaimToken == "" || claimed.ClaimedBy != "desk-alpha" {
		return errInvalidWorkflow("claim did not return the oldest message with an exclusive token")
	}

	// Reading the message back must never expose the claim token.
	readResponse, err := api.call("GET", "/v1/outbox/messages/"+claimed.ID, nil, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(readResponse, http.StatusOK, "read claimed message"); err != nil {
		return err
	}
	var readBack messageView
	if err := readResponse.decode(&readBack); err != nil {
		return err
	}
	if readBack.ClaimToken != "" {
		return errInvalidWorkflow("message read leaked the secret claim token")
	}

	// A competing terminal cannot claim while the lease is live.
	race, err := api.call("POST", "/v1/outbox/messages/"+claimed.ID+"/claim",
		map[string]any{"terminal_id": "desk-beta"}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(race, http.StatusConflict, string(domain.CodeMessageNotClaimable),
		"competing terminal claim"); err != nil {
		return err
	}
	// Neither another terminal nor a forged token can complete it.
	if err := requireTerminalComplete(api, claimed.ID, "desk-beta", claimed.ClaimToken,
		"complete from another terminal"); err != nil {
		return err
	}
	if err := requireTerminalComplete(api, claimed.ID, "desk-alpha", "forged-token",
		"complete with a forged token"); err != nil {
		return err
	}

	// The legitimate holder completes it; handled is terminal.
	completeResponse, err := api.call("POST", "/v1/outbox/messages/"+claimed.ID+"/complete",
		map[string]any{"terminal_id": "desk-alpha", "claim_token": claimed.ClaimToken}, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(completeResponse, http.StatusOK, "complete claimed message"); err != nil {
		return err
	}
	double, err := api.call("POST", "/v1/outbox/messages/"+claimed.ID+"/complete",
		map[string]any{"terminal_id": "desk-alpha", "claim_token": claimed.ClaimToken}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(double, http.StatusConflict, string(domain.CodeMessageAlreadyHandled),
		"complete a handled message twice"); err != nil {
		return err
	}

	// An empty filtered queue is a stable conflict rather than a silent no-op.
	emptyQueue, err := api.call("POST", "/v1/outbox/claims",
		map[string]any{"terminal_id": "desk-alpha", "recipient": domain.RecipientDutyPlanner}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(emptyQueue, http.StatusConflict, string(domain.CodeMessageNotClaimable),
		"claim from an empty planner queue"); err != nil {
		return err
	}

	// A failed processing attempt leaves the message in place.
	failed, err := api.call("POST", "/v1/outbox/messages/"+breach.ID+"/complete",
		map[string]any{"terminal_id": "desk-alpha", "claim_token": "never-claimed"}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(failed, http.StatusConflict, string(domain.CodeMessageClaimConflict),
		"complete a pending message without a claim"); err != nil {
		return err
	}
	if stillPending, _ := listMessages(api, "?state=pending&type="+domain.MessageTemperatureBreach); len(stillPending) != 1 {
		return errInvalidWorkflow("a failed completion removed or moved the pending message")
	}

	// --- finishing cleaning emits the fourth message type ------------------
	conditioning, err := api.call("POST",
		"/v1/runs/"+started.Run.ID+"/conditioning", nil, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(conditioning, http.StatusOK, "begin conditioning"); err != nil {
		return err
	}
	var conditioned runView
	if err := conditioning.decode(&conditioned); err != nil {
		return err
	}
	completeRunResponse, err := api.call("POST",
		"/v1/runs/"+started.Run.ID+"/complete", nil, ptrInt64(conditioned.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(completeRunResponse, http.StatusOK, "complete run for cleaning"); err != nil {
		return err
	}
	var completed pairView
	if err := completeRunResponse.decode(&completed); err != nil {
		return err
	}
	finishResponse, err := api.call("POST",
		"/v1/vessels/"+vessel.ID+"/finish-cleaning", nil, ptrInt64(completed.Vessel.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(finishResponse, http.StatusOK, "finish cleaning"); err != nil {
		return err
	}
	cleaning, err := listMessages(api, "?type="+domain.MessageVesselCleaningFinished)
	if err != nil {
		return err
	}
	if len(cleaning) != 1 || cleaning[0].RunID != started.Run.ID ||
		cleaning[0].Recipient != domain.RecipientDutyOperator {
		return errInvalidWorkflow("finishing cleaning did not create one operator message linked to the run")
	}

	// --- restart while processing: nothing is lost, tokens stay exclusive --
	if err := assertRecoveredState(dataDir, clockSource); err != nil {
		return err
	}
	return nil
}

// assertRecoveredState opens the same snapshot directory through independent
// stores and HTTP handlers, proving the outbox survives process restarts and
// that an expired lease is the only way to take over an unfinished message.
func assertRecoveredState(dataDir string, clockSource *clock.Fixed) error {
	// A message claimed just before a crash is still claimed afterward.
	crashClaim, err := apiClaim(currentCheckAPI, "crash-desk")
	if err != nil {
		return err
	}
	server, err := startCheckApp(dataDir, clockSource)
	if err != nil {
		return err
	}
	defer server.Close()
	recovered := newClient(server.URL)

	all, err := listMessages(recovered, "")
	if err != nil {
		return err
	}
	if len(all) != 4 {
		return errInvalidWorkflow("restart changed the number of outbox messages")
	}
	var handled, stillClaimed int
	for _, message := range all {
		switch message.State {
		case "handled":
			handled++
		case "claimed":
			if message.ID == crashClaim.ID && message.ClaimedBy == "crash-desk" {
				stillClaimed++
			}
		}
	}
	if handled != 1 {
		return errInvalidWorkflow("restart did not preserve the confirmed handled message")
	}
	if stillClaimed != 1 {
		return errInvalidWorkflow("restart lost the in-progress claim and its owner")
	}

	// The claim and its lease survive the restart, so the unfinished message is
	// not lost. A second desk cannot take it over until the lease expires.
	early, err := recovered.call("POST", "/v1/outbox/messages/"+crashClaim.ID+"/claim",
		map[string]any{"terminal_id": "recover-desk"}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(early, http.StatusConflict, string(domain.CodeMessageNotClaimable),
		"take over a message inside its lease"); err != nil {
		return err
	}

	clockSource.Advance(6 * time.Minute)
	takeover, err := recovered.call("POST", "/v1/outbox/messages/"+crashClaim.ID+"/claim",
		map[string]any{"terminal_id": "recover-desk"}, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(takeover, http.StatusCreated, "reclaim after lease expiry"); err != nil {
		return err
	}
	var retaken messageView
	if err := takeover.decode(&retaken); err != nil {
		return err
	}
	if retaken.Attempts != crashClaim.Attempts+1 {
		return errInvalidWorkflow("reclaimed message did not record another processing attempt")
	}
	oldToken, err := recovered.call("POST", "/v1/outbox/messages/"+crashClaim.ID+"/complete",
		map[string]any{"terminal_id": "crash-desk", "claim_token": crashClaim.ClaimToken}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(oldToken, http.StatusConflict, string(domain.CodeMessageClaimConflict),
		"stale token still completed a reclaimed message"); err != nil {
		return err
	}
	finalComplete, err := recovered.call("POST", "/v1/outbox/messages/"+retaken.ID+"/complete",
		map[string]any{"terminal_id": "recover-desk", "claim_token": retaken.ClaimToken}, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(finalComplete, http.StatusOK, "complete recovered message"); err != nil {
		return err
	}
	return nil
}

func appendOutboxObservation(
	api *client, runID string, sequence int, temperature float64, observed time.Time,
) (runView, error) {
	response, err := api.call("POST", "/v1/runs/"+runID+"/observations", map[string]any{
		"sequence":      sequence,
		"gravity":       1.040 - float64(sequence)*0.01,
		"temperature_c": temperature,
		"observed_at":   observed,
	}, nil)
	if err != nil {
		return runView{}, err
	}
	if err := requireStatus(response, http.StatusOK, "append outbox check observation"); err != nil {
		return runView{}, err
	}
	var run runView
	if err := response.decode(&run); err != nil {
		return runView{}, err
	}
	return run, nil
}

func listMessages(api *client, query string) ([]messageView, error) {
	response, err := api.call("GET", "/v1/outbox/messages"+query, nil, nil)
	if err != nil {
		return nil, err
	}
	if err := requireStatus(response, http.StatusOK, "list messages"); err != nil {
		return nil, err
	}
	var body struct {
		Messages []messageView `json:"messages"`
	}
	if err := response.decode(&body); err != nil {
		return nil, err
	}
	return body.Messages, nil
}

func countMessages(api *client, query, messageType string) int {
	messages, err := listMessages(api, query)
	if err != nil {
		return -1
	}
	return len(findAll(messages, messageType))
}

func findMessage(messages []messageView, messageType string) *messageView {
	for index := range messages {
		if messages[index].Type == messageType {
			return &messages[index]
		}
	}
	return nil
}

func findAll(messages []messageView, messageType string) []messageView {
	var matched []messageView
	for _, message := range messages {
		if message.Type == messageType {
			matched = append(matched, message)
		}
	}
	return matched
}

func requireTerminalComplete(api *client, id, terminal, token, operation string) error {
	response, err := api.call("POST", "/v1/outbox/messages/"+id+"/complete",
		map[string]any{"terminal_id": terminal, "claim_token": token}, nil)
	if err != nil {
		return err
	}
	return requireCode(response, http.StatusConflict, string(domain.CodeMessageClaimConflict),
		operation)
}

func apiClaim(api *client, terminal string) (messageView, error) {
	response, err := api.call("POST", "/v1/outbox/claims",
		map[string]any{"terminal_id": terminal}, nil)
	if err != nil {
		return messageView{}, err
	}
	if err := requireStatus(response, http.StatusCreated, "claim before simulated crash"); err != nil {
		return messageView{}, err
	}
	var claimed messageView
	if err := response.decode(&claimed); err != nil {
		return messageView{}, err
	}
	return claimed, nil
}

func startCheckApp(dataDir string, clockSource clock.Clock) (*httptest.Server, error) {
	store, err := storage.Open(dataDir)
	if err != nil {
		return nil, fmt.Errorf("reopen snapshot: %w", err)
	}
	app := service.New(store, clockSource)
	return httptest.NewServer(httpapi.NewHandler(app)), nil
}
