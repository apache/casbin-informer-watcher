package main

import (
	"log"
	"time"

	watcher "github.com/casbin/casbin-informer-watcher"
)

func main() {
	// Example 1: Create a watcher with default options
	w, err := watcher.NewWatcher(watcher.Options{
		Namespace:    "default",
		ResyncPeriod: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create watcher: %v", err)
	}
	defer w.Close()

	// Set up callback
	err = w.SetUpdateCallback(func(msg string) {
		log.Printf("Policy update received: %s", msg)
	})
	if err != nil {
		log.Fatalf("Failed to set callback: %v", err)
	}

	// Start the watcher
	if err := w.Start(); err != nil {
		log.Fatalf("Failed to start watcher: %v", err)
	}

	log.Println("Watcher started successfully")

	// Example 2: Use with Casbin enforcer
	// Note: This requires a proper model and policy setup
	// e, err := casbin.NewEnforcer("path/to/model.conf", "path/to/policy.csv")
	// if err != nil {
	// 	log.Fatalf("Failed to create enforcer: %v", err)
	// }
	//
	// w2, err := watcher.NewEnforcerWatcher(e, watcher.Options{
	// 	Namespace: "default",
	// })
	// if err != nil {
	// 	log.Fatalf("Failed to create enforcer watcher: %v", err)
	// }
	// defer w2.Close()
	//
	// if err := w2.Start(); err != nil {
	// 	log.Fatalf("Failed to start enforcer watcher: %v", err)
	// }

	// Example 3: Use with SyncedEnforcer (thread-safe)
	// se, err := casbin.NewSyncedEnforcer("path/to/model.conf", "path/to/policy.csv")
	// if err != nil {
	// 	log.Fatalf("Failed to create synced enforcer: %v", err)
	// }
	//
	// w3, err := watcher.NewEnforcerWatcher(se, watcher.Options{
	// 	Namespace: "default",
	// })
	// if err != nil {
	// 	log.Fatalf("Failed to create enforcer watcher: %v", err)
	// }
	// defer w3.Close()
	//
	// if err := w3.Start(); err != nil {
	// 	log.Fatalf("Failed to start enforcer watcher: %v", err)
	// }

	// Keep the example running
	select {}
}
