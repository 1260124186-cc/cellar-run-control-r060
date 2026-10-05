package httpapi

import (
	"net/http"
	"strings"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
)

type claimNextRequest struct {
	TerminalID string `json:"terminal_id"`
	Recipient  string `json:"recipient"`
}

type claimMessageRequest struct {
	TerminalID string `json:"terminal_id"`
}

type completeMessageRequest struct {
	TerminalID string `json:"terminal_id"`
	ClaimToken string `json:"claim_token"`
}

type releaseMessageRequest struct {
	TerminalID string `json:"terminal_id"`
	ClaimToken string `json:"claim_token"`
}

func registerOutboxRoutes(mux *http.ServeMux, app *service.Service) {
	mux.HandleFunc("GET /v1/outbox/messages", func(response http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		messages, err := app.ListMessages(service.ListMessagesInput{
			State:     strings.TrimSpace(query.Get("state")),
			Recipient: strings.TrimSpace(query.Get("recipient")),
			Type:      strings.TrimSpace(query.Get("type")),
			RunID:     strings.TrimSpace(query.Get("run_id")),
		})
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, map[string]any{"messages": messages})
	})

	mux.HandleFunc("GET /v1/outbox/messages/{id}", func(response http.ResponseWriter, request *http.Request) {
		message, err := app.GetMessage(request.PathValue("id"))
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, message)
	})

	mux.HandleFunc("POST /v1/outbox/claims", func(response http.ResponseWriter, request *http.Request) {
		body, err := decodeJSON[claimNextRequest](response, request)
		if !handleDecode(response, err) {
			return
		}
		message, err := app.ClaimNextMessage(service.ClaimMessageInput{
			TerminalID: body.TerminalID,
			Recipient:  body.Recipient,
		})
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusCreated, message)
	})

	mux.HandleFunc("POST /v1/outbox/messages/{id}/claim", func(response http.ResponseWriter, request *http.Request) {
		body, err := decodeJSON[claimMessageRequest](response, request)
		if !handleDecode(response, err) {
			return
		}
		message, err := app.ClaimMessageByID(request.PathValue("id"),
			service.ClaimMessageInput{TerminalID: body.TerminalID})
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusCreated, message)
	})

	mux.HandleFunc("POST /v1/outbox/messages/{id}/complete", func(response http.ResponseWriter, request *http.Request) {
		body, err := decodeJSON[completeMessageRequest](response, request)
		if !handleDecode(response, err) {
			return
		}
		message, err := app.CompleteMessage(request.PathValue("id"), service.CompleteMessageInput{
			TerminalID: body.TerminalID,
			ClaimToken: body.ClaimToken,
		})
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, message)
	})

	mux.HandleFunc("POST /v1/outbox/messages/{id}/release", func(response http.ResponseWriter, request *http.Request) {
		body, err := decodeJSON[releaseMessageRequest](response, request)
		if !handleDecode(response, err) {
			return
		}
		message, err := app.ReleaseMessage(request.PathValue("id"), service.ReleaseMessageInput{
			TerminalID: body.TerminalID,
			ClaimToken: body.ClaimToken,
		})
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, message)
	})
}
