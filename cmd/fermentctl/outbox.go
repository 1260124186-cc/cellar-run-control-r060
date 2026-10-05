package main

import (
	"io"
	"os"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/outboxcli"
)

// runOutbox wires the on-machine outbox terminal into the executable. The
// terminal only reads the local snapshot; no notification leaves the host.
func runOutbox(arguments []string, output io.Writer) error {
	if output == nil {
		output = os.Stdout
	}
	return outboxcli.Run(arguments, output)
}
