package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	watcher "github.com/casbin/casbin-informer-watcher"
	"github.com/casbin/casbin/v2"
)

func main() {
	// Create a SyncedEnforcer for thread-safe operations
	se, err := casbin.NewSyncedEnforcer("examples/rbac/model.conf", "examples/rbac/policy.csv")
	if err != nil {
		log.Fatalf("Failed to create synced enforcer: %v", err)
	}

	// Create and attach the watcher
	w, err := watcher.NewEnforcerWatcher(se, watcher.Options{
		Namespace:    "default", // Watch policies in the default namespace
		ResyncPeriod: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create watcher: %v", err)
	}
	defer w.Close()

	// Start watching for policy changes
	if err := w.Start(); err != nil {
		log.Fatalf("Failed to start watcher: %v", err)
	}

	log.Println("RBAC watcher started successfully")
	log.Println("The enforcer will automatically reload when CasbinPolicy CRDs change")

	// Test some authorization checks
	testAuthorization(se)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start a goroutine to periodically test authorization
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Println("\n--- Periodic Authorization Check ---")
				testAuthorization(se)
			case <-sigChan:
				return
			}
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	log.Println("\nShutting down...")
}

func testAuthorization(e *casbin.SyncedEnforcer) {
	// Test cases
	testCases := []struct {
		sub string
		obj string
		act string
	}{
		{"alice", "data1", "read"},
		{"alice", "data1", "write"},
		{"alice", "data2", "read"},
		{"bob", "data1", "read"},
		{"bob", "data2", "write"},
		{"charlie", "data1", "read"}, // Should fail
	}

	for _, tc := range testCases {
		allowed, err := e.Enforce(tc.sub, tc.obj, tc.act)
		if err != nil {
			log.Printf("Error checking authorization for %s, %s, %s: %v", tc.sub, tc.obj, tc.act, err)
			continue
		}

		result := "DENIED"
		if allowed {
			result = "ALLOWED"
		}
		log.Printf("%s: %s -> %s (%s)", result, tc.sub, tc.obj, tc.act)
	}
}
