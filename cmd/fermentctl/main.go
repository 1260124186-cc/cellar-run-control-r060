package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/config"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/httpapi"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/workflowcheck"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "fermentctl:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 {
		return usageError()
	}
	switch arguments[0] {
	case "serve":
		if len(arguments) != 1 {
			return usageError()
		}
		return serve()
	case "check":
		if len(arguments) != 2 {
			return usageError()
		}
		return workflowcheck.Run(arguments[1])
	case "outbox":
		return runOutbox(arguments[1:], os.Stdout)
	default:
		return usageError()
	}
}

func usageError() error {
	return errors.New("usage: fermentctl serve | fermentctl check <workflow> | fermentctl outbox [list|show]")
}

func serve() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	if err := cfg.EnsureDataDir(); err != nil {
		return err
	}
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	app := service.New(store, clock.System{})
	server := &http.Server{
		Addr:              cfg.Address,
		Handler:           httpapi.NewHandler(app),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}

	rootContext, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()
	go runClaimReaper(rootContext, app)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		cancelRoot()
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
		}
	}()

	log.Printf("cellar run control listening on %s with data %s", cfg.Address, cfg.DataDir)
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("serve HTTP: %w", err)
}

func init() {
	log.SetFlags(log.LstdFlags | log.LUTC)
}

// claimReapInterval is how often elapsed claim leases are returned to the
// pending queue. Claims are short (default two minutes), so a 15-second sweep
// keeps the duty board accurate without adding write pressure.
const claimReapInterval = 15 * time.Second

func runClaimReaper(ctx context.Context, app *service.Service) {
	ticker := time.NewTicker(claimReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			requeued, err := app.RequeueExpiredClaims()
			if err != nil {
				log.Printf("reap expired claims failed: %v", err)
				continue
			}
			if requeued > 0 {
				log.Printf("requeued %d expired outbox claim(s)", requeued)
			}
		}
	}
}
