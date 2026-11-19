package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/demo"
)

const (
	watchEvery = time.Second
	stopWithin = 8 * time.Second
)

func main() {
	check := flag.Bool("check", false, "ask /healthz at DEMO_ADDR, exit 0 if the demo is alive and 1 if not")
	flag.Parse()
	if flag.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "demo: no arguments, the only flag is -check")
		os.Exit(2)
	}
	if *check {
		os.Exit(probe(os.Environ(), os.Stderr))
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	code := run(ctx, os.Environ(), os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, environ []string, stderr io.Writer) int {
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintln(stderr, "demo cannot start:", err)
		return 3
	}
	cfg, err := parse(environ, exe)
	if err != nil {
		fmt.Fprintln(stderr, "demo:", err)
		return 2
	}
	logger := log.New(stderr, "", log.LstdFlags|log.Lmicroseconds|log.LUTC)
	if err := cfg.prepare(); err != nil {
		logger.Printf("demo cannot start: %v", err)
		return 3
	}
	l, err := net.Listen("tcp", cfg.addr.String())
	if err != nil {
		logger.Printf("demo cannot start: %v", err)
		return 3
	}
	logger.Print(greeting(cfg))
	stage := demo.New(demo.Config{Dir: cfg.dir, Ledgerd: cfg.ledgerd, Log: logger})
	web := demo.NewWeb(stage, cfg.origin, cfg.proxies)
	return serve(ctx, stage.Run, func(ctx context.Context) error { return web.Serve(ctx, l) }, stage.Alive, logger)
}

func serve(ctx context.Context, runStage, runWeb func(context.Context) error, alive func() bool, logger *log.Logger) int {
	stageCtx, stopStage := context.WithCancel(context.Background())
	defer stopStage()
	webCtx, stopWeb := context.WithCancel(context.Background())
	defer stopWeb()
	var stageErr, webErr error
	staged, served := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(staged)
		stageErr = runStage(stageCtx)
	}()
	go func() {
		defer close(served)
		webErr = runWeb(webCtx)
	}()
	watch := time.NewTicker(watchEvery)
	defer watch.Stop()
	code, end := 1, ""
	for end == "" {
		select {
		case <-ctx.Done():
			code, end = 0, "demo stopped: "+context.Cause(ctx).Error()
		case <-staged:
			end = fmt.Sprintf("demo halted: %v", stageErr)
		case <-served:
			end = fmt.Sprintf("demo halted: %v", webErr)
		case <-watch.C:
			if !alive() {
				end = "demo halted: the stage or its sight stopped reporting"
			}
		}
	}
	stopWeb()
	<-served
	stopStage()
	select {
	case <-staged:
	case <-time.After(stopWithin):
		logger.Printf("demo leaves its nodes to the kernel: the stage did not stop within %v", stopWithin)
	}
	logger.Print(end)
	return code
}
