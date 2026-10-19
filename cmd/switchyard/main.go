// Command switchyard is an HTTP reverse proxy and load balancer.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/asmahdy1420-cmyk/switchyard/internal/config"
	"github.com/asmahdy1420-cmyk/switchyard/internal/metrics"
	"github.com/asmahdy1420-cmyk/switchyard/internal/proxy"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "switchyard:", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "switchyard.yaml", "path to the YAML configuration file")
	checkOnly := flag.Bool("check", false, "validate the configuration and exit")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if *checkOnly {
		fmt.Println("configuration OK")
		return nil
	}

	log := newLogger(cfg.LogFormat)
	reg := metrics.NewRegistry()
	handler, err := proxy.New(cfg, proxy.Options{Logger: log, Registry: reg})
	if err != nil {
		return err
	}
	defer handler.Close()

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	adminMux := http.NewServeMux()
	adminMux.Handle("GET /metrics", reg)
	adminMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	admin := &http.Server{Addr: cfg.AdminListen, Handler: adminMux, ReadHeaderTimeout: 5 * time.Second}

	errc := make(chan error, 2)
	for _, s := range []*http.Server{srv, admin} {
		go func() {
			log.Info("listening", "addr", s.Addr)
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("serve %s: %w", s.Addr, err)
			}
		}()
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigc)

	for {
		select {
		case err := <-errc:
			return err
		case sig := <-sigc:
			if sig == syscall.SIGHUP {
				reload(log, handler, *configPath)
				continue
			}
			log.Info("shutting down", "signal", sig.String(), "timeout", cfg.ShutdownTimeout.String())
			return shutdown(handler.Config().ShutdownTimeout, srv, admin)
		}
	}
}

// reload re-reads the file and swaps the handler's runtime. A broken file
// is logged and ignored; the previous configuration keeps serving.
func reload(log *slog.Logger, h *proxy.Handler, path string) {
	prev := h.Config()
	next, err := config.Load(path)
	if err == nil {
		err = h.Reload(next)
	}
	if err != nil {
		log.Error("config reload failed, keeping previous config", "error", err)
		return
	}
	if next.Listen != prev.Listen || next.AdminListen != prev.AdminListen {
		log.Warn("listen addresses changed; restart to apply them")
	}
	log.Info("config reloaded", "routes", len(next.Routes), "pools", len(next.Pools))
}

// shutdown stops accepting connections and waits for in-flight requests.
func shutdown(timeout time.Duration, servers ...*http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var errs []error
	for _, s := range servers {
		if err := s.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func newLogger(format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if format == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
