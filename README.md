# Casbin Informer Watcher

[![CI](https://github.com/casbin/casbin-informer-watcher/actions/workflows/ci.yml/badge.svg)](https://github.com/casbin/casbin-informer-watcher/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/casbin/casbin-informer-watcher)](https://goreportcard.com/report/github.com/casbin/casbin-informer-watcher)
[![Go Reference](https://pkg.go.dev/badge/github.com/casbin/casbin-informer-watcher.svg)](https://pkg.go.dev/github.com/casbin/casbin-informer-watcher)
[![License](https://img.shields.io/github/license/casbin/casbin-informer-watcher)](https://github.com/casbin/casbin-informer-watcher/blob/master/LICENSE)
[![Release](https://img.shields.io/github/v/release/casbin/casbin-informer-watcher)](https://github.com/casbin/casbin-informer-watcher/releases/latest)

Casbin Informer Watcher is a Kubernetes informer-based watcher for [Casbin](https://github.com/casbin/casbin). This watcher enables real-time policy synchronization across multiple Casbin enforcer instances by watching Kubernetes Custom Resource Definitions (CRDs).

## Features

- **Real-time Updates**: Uses Kubernetes informers to watch CRD changes without periodic polling
- **Event-Driven**: Reacts immediately to create, update, and delete events
- **Concurrency-Safe**: Safe to use with `SyncedEnforcer` in multi-threaded environments
- **Graceful Reconnection**: Handles disconnections and resource version drift automatically
- **Incremental Updates**: Supports both full policy reload and incremental policy updates
- **GitOps Compatible**: Changes applied via GitOps become effective quickly across all instances

## Installation

```bash
go get github.com/casbin/casbin-informer-watcher
```

## Usage

### Basic Example

```go
package main

import (
	"log"

	"github.com/casbin/casbin/v2"
	informerwatcher "github.com/casbin/casbin-informer-watcher"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	// Load Kubernetes configuration
	config, err := clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	if err != nil {
		log.Fatalf("Failed to load kubeconfig: %v", err)
	}

	// Create dynamic client
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create dynamic client: %v", err)
	}

	// Define the GVR for your policy CRD
	gvr := schema.GroupVersionResource{
		Group:    "casbin.org",
		Version:  "v1",
		Resource: "policies",
	}

	// Create the watcher
	watcher, err := informerwatcher.NewWatcher(client, gvr, "default", informerwatcher.WatcherOptions{})
	if err != nil {
		log.Fatalf("Failed to create watcher: %v", err)
	}
	defer watcher.Close()

	// Initialize the enforcer
	e, err := casbin.NewEnforcer("examples/rbac_model.conf", "examples/rbac_policy.csv")
	if err != nil {
		log.Fatalf("Failed to create enforcer: %v", err)
	}

	// Set the watcher for the enforcer
	err = e.SetWatcher(watcher)
	if err != nil {
		log.Fatalf("Failed to set watcher: %v", err)
	}

	// By default, the watcher's callback is automatically set to the
	// enforcer's LoadPolicy() in the SetWatcher() call.
	// You can change it by explicitly setting a callback.
	err = watcher.SetUpdateCallback(informerwatcher.DefaultUpdateCallback(e))
	if err != nil {
		log.Fatalf("Failed to set callback: %v", err)
	}

	log.Println("Watcher is running and monitoring policy changes...")
	select {} // Keep the program running
}
```

### Advanced Example with Custom Options

```go
package main

import (
	"log"
	"time"

	"github.com/casbin/casbin/v2"
	informerwatcher "github.com/casbin/casbin-informer-watcher"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	// Load Kubernetes configuration
	config, err := clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	if err != nil {
		log.Fatalf("Failed to load kubeconfig: %v", err)
	}

	// Create dynamic client
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create dynamic client: %v", err)
	}

	// Define the GVR for your policy CRD
	gvr := schema.GroupVersionResource{
		Group:    "casbin.org",
		Version:  "v1",
		Resource: "policies",
	}

	// Create watcher with custom options
	options := informerwatcher.WatcherOptions{
		LocalID:      "instance-1",          // Custom instance identifier
		IgnoreSelf:   true,                  // Ignore updates from this instance
		ResyncPeriod: 30 * time.Second,      // Resync period with API server
	}

	watcher, err := informerwatcher.NewWatcher(client, gvr, "default", options)
	if err != nil {
		log.Fatalf("Failed to create watcher: %v", err)
	}
	defer watcher.Close()

	// Initialize the enforcer
	e, err := casbin.NewEnforcer("examples/rbac_model.conf", "examples/rbac_policy.csv")
	if err != nil {
		log.Fatalf("Failed to create enforcer: %v", err)
	}

	// Set the watcher
	err = e.SetWatcher(watcher)
	if err != nil {
		log.Fatalf("Failed to set watcher: %v", err)
	}

	// Custom callback that logs updates
	customCallback := func(msg string) {
		log.Printf("Policy update received: %s\n", msg)
		informerwatcher.DefaultUpdateCallback(e)(msg)
	}

	err = watcher.SetUpdateCallback(customCallback)
	if err != nil {
		log.Fatalf("Failed to set callback: %v", err)
	}

	log.Println("Watcher is running with custom configuration...")
	select {} // Keep the program running
}
```

### With SyncedEnforcer

```go
package main

import (
	"log"

	"github.com/casbin/casbin/v2"
	informerwatcher "github.com/casbin/casbin-informer-watcher"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	// Setup client and GVR (same as basic example)
	config, _ := clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	client, _ := dynamic.NewForConfig(config)

	gvr := schema.GroupVersionResource{
		Group:    "casbin.org",
		Version:  "v1",
		Resource: "policies",
	}

	watcher, _ := informerwatcher.NewWatcher(client, gvr, "default", informerwatcher.WatcherOptions{})
	defer watcher.Close()

	// Use SyncedEnforcer for concurrency-safe operations
	e, err := casbin.NewSyncedEnforcer("examples/rbac_model.conf", "examples/rbac_policy.csv")
	if err != nil {
		log.Fatalf("Failed to create synced enforcer: %v", err)
	}

	err = e.SetWatcher(watcher)
	if err != nil {
		log.Fatalf("Failed to set watcher: %v", err)
	}

	log.Println("SyncedEnforcer is running with watcher...")
	select {}
}
```

## Configuration Options

### WatcherOptions

- **LocalID** (string): Unique identifier for this watcher instance. Auto-generated if not provided.
- **IgnoreSelf** (bool): If true, ignores updates triggered by this watcher instance. Default: false.
- **ResyncPeriod** (time.Duration): Period for the informer to resync with the API server. Default: 30 seconds.
- **OptionalUpdateCallback** (func(string)): Optional callback function set during initialization.

## How It Works

1. The watcher uses Kubernetes informers to monitor Custom Resource Definitions (CRDs) that represent Casbin policies.
2. When a CRD is created, updated, or deleted, the informer triggers the corresponding event handler.
3. The event handler processes the change and invokes the registered callback function.
4. The callback typically triggers the enforcer to reload its policy or apply incremental updates.
5. All running instances with the same watcher configuration receive the same updates, keeping policies synchronized.

## Supported Update Types

The watcher supports all standard Casbin policy update operations:

- `Update`: Full policy reload
- `UpdateForAddPolicy`: Add a single policy rule
- `UpdateForRemovePolicy`: Remove a single policy rule
- `UpdateForAddPolicies`: Add multiple policy rules
- `UpdateForRemovePolicies`: Remove multiple policy rules
- `UpdateForRemoveFilteredPolicy`: Remove filtered policy rules
- `UpdateForUpdatePolicy`: Update a single policy rule
- `UpdateForUpdatePolicies`: Update multiple policy rules
- `UpdateForSavePolicy`: Save policy to storage

## Custom Resource Definition (CRD) Example

You'll need to define a CRD for your Casbin policies. Here's a basic example:

```yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: policies.casbin.org
spec:
  group: casbin.org
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                policy:
                  type: string
  scope: Namespaced
  names:
    plural: policies
    singular: policy
    kind: Policy
```

## Testing

Run the test suite:

```bash
go test -v ./...
```

Run tests with coverage:

```bash
go test -v -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## See Also

- [Casbin](https://github.com/casbin/casbin) - An authorization library that supports access control models like ACL, RBAC, ABAC
- [Redis Watcher](https://github.com/casbin/redis-watcher) - Redis-based watcher for Casbin
- [Etcd Watcher](https://github.com/casbin/etcd-watcher) - Etcd-based watcher for Casbin
- [GORM Adapter](https://github.com/casbin/gorm-adapter) - GORM adapter for Casbin
- [Ent Adapter](https://github.com/casbin/ent-adapter) - Ent adapter for Casbin
