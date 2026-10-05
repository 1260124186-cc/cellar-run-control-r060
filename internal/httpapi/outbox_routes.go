package httpapi

import (
	"net/http"
	"strings"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
)

type claimMessageRequest struct {
	Operator     string `json:"operator"`
	ClaimSeconds int    `json:"claim_seconds"`
}

type completeMessageRequest struct {
	Note string `json:"note"`
}

type failMessageRequest struct {
	Reason string `json:"reason"`
}

// claimResponse intentionally echoes the token so the claiming terminal can
// store it. The token is never present in list or detail views.
type claimResponse struct {
	Message messageView    `json:"message"`
	Claim   claimTokenView `json:"claim"`
}

type claimTokenView struct {
	Token     string `json:"token"`
	ClaimedBy string `json:"claimed_by"`
	ExpiresAt string `json:"claim_expires_at"`
}

func registerOutboxRoutes(mux *http.ServeMux, app *service.Service) {
	mux.HandleFunc("GET /v1/outbox/messages", func(response http.ResponseWriter, request *http.Request) {
		filter := service.MessageFilter{
			State:     request.URL.Query().Get("state"),
			Recipient: request.URL.Query().Get("recipient"),
			Type:      request.URL.Query().Get("type"),
		}
		messages, err := app.ListMessages(filter)
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, map[string]any{
			"messages": projectMessages(messages),
		})
	})

	mux.HandleFunc("GET /v1/outbox/messages/{id}", func(response http.ResponseWriter, request *http.Request) {
		message, err := app.GetMessage(request.PathValue("id"))
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, projectMessage(message))
	})

	mux.HandleFunc("POST /v1/outbox/messages/{id}/claim", func(response http.ResponseWriter, request *http.Request) {
		body, err := decodeJSON[claimMessageRequest](response, request)
		if !handleDecode(response, err) {
			return
		}
		message, token, err := app.ClaimMessage(request.PathValue("id"), service.ClaimMessageInput{
			Operator:     body.Operator,
			ClaimSeconds: body.ClaimSeconds,
		})
		if err != nil {
			writeDomainError(response, err)
			return
		}
		expires := ""
		if message.ClaimExpiresAt != nil {
			expires = message.ClaimExpiresAt.Format("2006-01-02T15:04:05Z07:00")
		}
		holder := ""
		if message.ClaimedBy != nil {
			holder = *message.ClaimedBy
		}
		writeJSON(response, http.StatusOK, claimResponse{
			Message: projectMessage(message),
			Claim: claimTokenView{
				Token:     token,
				ClaimedBy: holder,
				ExpiresAt: expires,
			},
		})
	})

	mux.HandleFunc("POST /v1/outbox/messages/{id}/complete", func(response http.ResponseWriter, request *http.Request) {
		body, ok := decodeOptionalJSON[completeMessageRequest](response, request)
		if !ok {
			return
		}
		message, err := app.CompleteMessage(request.PathValue("id"), service.CompleteMessageInput{
			ClaimToken: strings.TrimSpace(request.Header.Get("X-Claim-Token")),
			Note:       body.Note,
		})
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, projectMessage(message))
	})

	mux.HandleFunc("POST /v1/outbox/messages/{id}/fail", func(response http.ResponseWriter, request *http.Request) {
		body, err := decodeJSON[failMessageRequest](response, request)
		if !handleDecode(response, err) {
			return
		}
		message, err := app.FailMessage(request.PathValue("id"), service.FailMessageInput{
			ClaimToken: strings.TrimSpace(request.Header.Get("X-Claim-Token")),
			Reason:     body.Reason,
		})
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, projectMessage(message))
	})
}
