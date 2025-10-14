package signals

import (
	"os"
	"os/signal"
	"syscall"
)

// SetupSignalHandler registers for SIGTERM and SIGINT. A stop channel is returned
// which is closed on one of these signals. If a second signal is caught, the
// program is terminated with exit code 1.
func SetupSignalHandler() <-chan struct{} {
	stop := make(chan struct{})
	sigCh := make(chan os.Signal, 2)

	// Subscribe to termination signals
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		// First signal: close stop to trigger graceful shutdown
		<-sigCh
		close(stop)

		// Second signal: force exit
		<-sigCh
		os.Exit(1)
	}()

	return stop
}
