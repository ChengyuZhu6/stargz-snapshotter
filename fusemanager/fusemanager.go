/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package fusemanager

import (
	"context"
	"flag"
	"fmt"
	golog "log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/containerd/log"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"

	pb "github.com/containerd/stargz-snapshotter/fusemanager/api"
	"github.com/containerd/stargz-snapshotter/version"
)

var (
	versionFlag   bool
	fuseStoreAddr string
	address       string
	logLevel      string
	logPath       string
	action        string
)

func parseFlags() {
	flag.BoolVar(&versionFlag, "v", false, "show the fusemanager version and exit")
	flag.StringVar(&action, "action", "", "action of fusemanager")
	flag.StringVar(&fuseStoreAddr, "fusestore-path", "/var/lib/containerd-stargz-grpc/fusestore.db", "address for the fusemanager's store")
	flag.StringVar(&address, "address", "/run/containerd-stargz-grpc/fuse-manager.sock", "address for the fusemanager's gRPC socket")
	flag.StringVar(&logLevel, "log-level", logrus.InfoLevel.String(), "set the logging level [trace, debug, info, warn, error, fatal, panic]")
	flag.StringVar(&logPath, "log-path", "", "path to fusemanager's logs, logs to stderr by default, pass --log-level=panic to disable logging")

	flag.Parse()
}

func Run() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to run fusemanager: %v", err)
		os.Exit(1)
	}
}

func run() error {
	parseFlags()
	if versionFlag {
		fmt.Printf("%s:\n", os.Args[0])
		fmt.Println("  Version: ", version.Version)
		fmt.Println("  Revision:", version.Revision)
		fmt.Println("")
		return nil
	}

	if fuseStoreAddr == "" || address == "" {
		return fmt.Errorf("fusemanager fusestore and socket path cannot be empty")
	}

	ctx := log.WithLogger(context.Background(), log.L)

	switch action {
	case "start":
		return startNew(ctx, logPath, address, fuseStoreAddr, logLevel)
	default:
		return runFuseManager(ctx)
	}
}

func startNew(ctx context.Context, logPath, address, fusestore, logLevel string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	args := []string{
		"-address", address,
		"-fusestore-path", fusestore,
		"-log-level", logLevel,
	}

	// we use shim-like approach to start new fusemanager process by self-invoking in the background
	// and detach it from parent
	cmd := exec.CommandContext(ctx, self, args...)
	cmd.Dir = cwd
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	if logPath == "" {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	} else {
		file, err := os.Create(logPath)
		if err != nil {
			return err
		}
		cmd.Stdout = file
		cmd.Stderr = file
	}

	if err := cmd.Start(); err != nil {
		return err
	}
	go cmd.Wait()

	if ready, err := waitUntilReady(ctx); err != nil || !ready {
		if err != nil {
			return fmt.Errorf("failed to start new fusemanager: %w", err)
		}
		if !ready {
			return fmt.Errorf("failed to start new fusemanager, fusemanager not ready")
		}
	}

	return nil
}

// waitUntilReady waits until fusemanager is ready to accept requests
func waitUntilReady(ctx context.Context) (bool, error) {
	grpcCli, err := newClient(address)
	if err != nil {
		return false, err
	}

	resp, err := grpcCli.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to call Status")
		return false, err
	}

	if resp.Status == FuseManagerNotReady {
		return false, nil
	}

	return true, nil
}

func runFuseManager(ctx context.Context) error {
	lvl, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return fmt.Errorf("failed to prepare logger: %w", err)
	}

	logrus.SetLevel(lvl)
	logrus.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: log.RFC3339NanoFixed,
	})

	golog.SetOutput(log.G(ctx).WriterLevel(logrus.DebugLevel))

	// Prepare the directory for the socket
	if err := os.MkdirAll(filepath.Dir(address), 0700); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", filepath.Dir(address), err)
	}

	// Try to remove the socket file to avoid EADDRINUSE
	if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove old socket file: %w", err)
	}

	l, err := net.Listen("unix", address)
	if err != nil {
		return fmt.Errorf("failed to listen socket: %w", err)
	}

	server := grpc.NewServer()
	fm, err := NewFuseManager(ctx, l, server, fuseStoreAddr, address)
	if err != nil {
		return fmt.Errorf("failed to configure manager server: %w", err)
	}

	pb.RegisterStargzFuseManagerServiceServer(server, fm)

	errCh := make(chan error, 1)
	go func() {
		if err := server.Serve(l); err != nil {
			errCh <- fmt.Errorf("error on serving via socket %q: %w", address, err)
		}
	}()

	var s os.Signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, unix.SIGINT, unix.SIGTERM)
	select {
	case s = <-sigCh:
		log.G(ctx).Infof("Got %v", s)
	case err := <-errCh:
		log.G(ctx).WithError(err).Warnf("error during running the server")
	}

	server.Stop()
	if err = fm.Close(ctx); err != nil {
		return fmt.Errorf("failed to close fuse manager: %w", err)
	}

	return nil
}

func StartFuseManager(ctx context.Context, executable, address, fusestore, logLevel, logPath string) (newlyStarted bool, err error) {
	// if socket exists, do not start it
	if _, err := os.Stat(address); err == nil {
		return false, nil
	} else if !os.IsNotExist(err) {
		return false, err
	}

	if _, err := os.Stat(executable); err != nil {
		return false, fmt.Errorf("failed to stat fusemanager binary: %q", executable)
	}

	args := []string{
		"-action", "start",
		"-address", address,
		"-fusestore-path", fusestore,
		"-log-level", logLevel,
		"-log-path", logPath,
	}

	cmd := exec.Command(executable, args...)
	if err := cmd.Start(); err != nil {
		return false, err
	}

	if err := cmd.Wait(); err != nil {
		return false, err
	}

	return true, nil
}
