// Package outboxcli implements the on-machine duty terminal for the local
// notification outbox. It reads the durable snapshot directly and never
// contacts an external service.
package outboxcli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
)

// Run dispatches the local outbox terminal subcommands.
func Run(arguments []string, output io.Writer) error {
	subcommand := "list"
	if len(arguments) > 0 {
		subcommand = arguments[0]
		arguments = arguments[1:]
	}
	switch subcommand {
	case "list", "ls":
		return List(arguments, output)
	case "show", "get":
		return Show(arguments, output)
	default:
		return errors.New("usage: fermentctl outbox [list|show] [flags]")
	}
}

func DefaultDataDir() string {
	if value := strings.TrimSpace(os.Getenv("CELLAR_CONTROL_DATA_DIR")); value != "" {
		return value
	}
	return "./data"
}

func openService(dataDir string) (*service.Service, error) {
	trimmed := strings.TrimSpace(dataDir)
	if trimmed == "" {
		return nil, errors.New("data directory cannot be empty")
	}
	store, err := storage.OpenView(trimmed)
	if err != nil {
		return nil, err
	}
	return service.New(store, clock.System{}), nil
}

// List prints a table of messages matching the local filters.
func List(arguments []string, output io.Writer) error {
	flags := flag.NewFlagSet("outbox list", flag.ContinueOnError)
	flags.SetOutput(output)
	dataDir := flags.String("data-dir", DefaultDataDir(), "directory containing state.json")
	state := flags.String("state", "", "filter by pending, claimed, or handled")
	recipient := flags.String("recipient", "", "filter by local recipient role")
	messageType := flags.String("type", "", "filter by message type")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	app, err := openService(*dataDir)
	if err != nil {
		return err
	}
	messages, err := app.ListMessages(service.MessageFilter{
		State:     *state,
		Recipient: *recipient,
		Type:      *messageType,
	})
	if err != nil {
		return err
	}
	if len(messages) == 0 {
		fmt.Fprintln(output, "no messages")
		return nil
	}
	writer := tabwriter.NewWriter(output, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "SEQUENCE\tID\tTYPE\tPRIORITY\tRECIPIENT\tSTATE\tRUN\tSUBJECT")
	for _, message := range messages {
		runCode := ""
		if code, ok := message.Details["run_code"]; ok {
			runCode = code
		}
		fmt.Fprintf(writer, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			message.Sequence, message.ID, message.Type, message.Priority,
			message.Recipient, message.State, runCode, message.Subject)
	}
	return writer.Flush()
}

// Show prints one message in full detail.
func Show(arguments []string, output io.Writer) error {
	flags := flag.NewFlagSet("outbox show", flag.ContinueOnError)
	flags.SetOutput(output)
	dataDir := flags.String("data-dir", DefaultDataDir(), "directory containing state.json")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("usage: fermentctl outbox show [-data-dir DIR] <message-id>")
	}
	app, err := openService(*dataDir)
	if err != nil {
		return err
	}
	message, err := app.GetMessage(flags.Arg(0))
	if err != nil {
		return err
	}
	fmt.Fprintf(output, "id:        %s\n", message.ID)
	fmt.Fprintf(output, "sequence:  %d\n", message.Sequence)
	fmt.Fprintf(output, "type:      %s\n", message.Type)
	fmt.Fprintf(output, "priority:  %s\n", message.Priority)
	fmt.Fprintf(output, "recipient: %s\n", message.Recipient)
	fmt.Fprintf(output, "state:     %s\n", message.State)
	fmt.Fprintf(output, "subject:   %s\n", message.Subject)
	if message.FormulaID != "" {
		fmt.Fprintf(output, "formula:   %s\n", message.FormulaID)
	}
	if message.RunID != nil {
		fmt.Fprintf(output, "run:       %s\n", *message.RunID)
	}
	if message.VesselID != nil {
		fmt.Fprintf(output, "vessel:    %s\n", *message.VesselID)
	}
	fmt.Fprintf(output, "created:   %s\n", message.CreatedAt.Format("2006-01-02 15:04:05 UTC"))
	if message.ClaimedBy != nil {
		fmt.Fprintf(output, "claimed_by:     %s\n", *message.ClaimedBy)
	}
	if message.ClaimExpiresAt != nil {
		fmt.Fprintf(output, "claim_expires:  %s\n",
			message.ClaimExpiresAt.Format("2006-01-02 15:04:05 UTC"))
	}
	if message.HandledBy != nil {
		fmt.Fprintf(output, "handled_by:     %s\n", *message.HandledBy)
	}
	if message.HandledAt != nil {
		fmt.Fprintf(output, "handled_at:     %s\n",
			message.HandledAt.Format("2006-01-02 15:04:05 UTC"))
	}
	if message.Attempts > 0 {
		fmt.Fprintf(output, "failed attempts: %d\n", message.Attempts)
		for index, failure := range message.Failures {
			fmt.Fprintf(output, "  [%d] %s  %s\n",
				index+1, failure.At.Format("2006-01-02 15:04:05 UTC"), failure.Reason)
		}
	}
	if len(message.Details) > 0 {
		fmt.Fprintln(output, "details:")
		for key, value := range message.Details {
			fmt.Fprintf(output, "  %s: %s\n", key, value)
		}
	}
	return nil
}
