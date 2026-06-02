package toolcheck

import (
	"testing"
	"testing/synctest"
)

// TestSynctestToolchain pins the testing/synctest API that buff's followable buffer
// is tested against, and proves the bubble scheduler actually works here — it is not
// a mere symbol-existence check.
//
// That buffer is a single-writer, many-reader log whose readers block until more
// bytes arrive; its correctness tests must deterministically observe a reader being
// woken, and a blocked reader being cancelled without leaking its goroutine. synctest
// is what makes that deterministic: it runs the closure in a "bubble" of
// deterministically scheduled goroutines, and Wait returns only once every other
// goroutine in the bubble is durably blocked or has exited.
//
// The assertion relies on exactly that guarantee. The spawned goroutine closes ran
// and exits, so a correct Wait must not return until it has, leaving the non-blocking
// receive ready; a regression that let Wait return early would take the default case
// and fail here. The callback takes *testing.T (the current API shape), whose Context
// is bubble-aware — the mechanism the buffer's cancellation test is driven by.
// Closing a channel happens-before a receive on it, so this is race-clean; running it
// under -race also confirms synctest and the race detector compose on this toolchain,
// which the buffer tests depend on.
func TestSynctestToolchain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ran := make(chan struct{})
		go func() { close(ran) }()
		synctest.Wait()
		select {
		case <-ran:
		default:
			t.Fatal("synctest.Wait returned before the bubbled goroutine ran")
		}
	})
}
