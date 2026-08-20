package redis

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestConnLeakPrevention(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{
		Addr:     "localhost:6379",
		PoolSize: 1,
	})
	defer client.Close()

	// Acquire a connection and let it go out of scope without closing.
	func() {
		conn := client.Conn(ctx)
		_ = conn.Ping(ctx).Err()
	}()

	// Force GC to trigger the finalizer.
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	// Try to acquire another connection. If leak prevention works, this succeeds.
	done := make(chan struct{})
	go func() {
		conn := client.Conn(ctx)
		_ = conn.Ping(ctx).Err()
		_ = conn.Close()
		close(done)
	}()

	select {
	case <-done:
		// Success!
	case <-time.After(2 * time.Second):
		t.Fatal("connection pool exhausted: connection was leaked")
	}
}