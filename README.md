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
		LocalID:           "instance-1",     // Custom instance identifier
		IgnoreSelf:        true,             // Ignore updates from this instance
		ResyncPeriod:      30 * time.Second, // Resync period with API server
		IncrementalUpdate: true,             // Report the rules that changed instead of reloading
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
- **OptionalUpdateCallback** (func(string)): Optional callback function set during initialization. Equivalent to calling `SetUpdateCallback` right after `NewWatcher`, but it is in place before the first event arrives.
- **IncrementalUpdate** (bool): If true, the watcher reports the exact rules that changed instead of asking the enforcer to reload its whole policy. Default: false. See [Update Modes](#update-modes).

## How It Works

1. The watcher uses Kubernetes informers to monitor Custom Resource Definitions (CRDs) that represent Casbin policies.
2. When a policy resource is created, updated, or deleted, the informer triggers the corresponding event handler.
3. The event handler turns the change into one or more watcher messages and invokes the registered callback function.
4. `DefaultUpdateCallback` applies each message to the enforcer, either by reloading the policy or by applying the incremental change.
5. All running instances with the same watcher configuration receive the same updates, keeping policies synchronized.

Three kinds of event carry no change and are dropped rather than reported:

- The resources that already exist when the watcher starts. The informer replays them as create events while it builds its cache, but they describe the state the enforcer loaded through its own adapter.
- The periodic resync, which re-delivers every cached resource. Kubernetes only bumps `resourceVersion` on a real write, so a resource that comes back with the same one has not changed.
- Updates from this instance itself, when `IgnoreSelf` is enabled and the resource carries the `casbin.org/source-id` annotation with this watcher's `LocalID`.

Deletions missed while the watch was down arrive as a tombstone rather than the resource itself; the watcher unwraps it and reports the deletion normally.

## Update Modes

By default, every observed change asks the enforcer for a full reload (the `Update` message). Reloading is idempotent, so instances cannot drift apart no matter how a resource was edited. This is the recommended mode when the enforcer's adapter reads the same resources the watcher observes.

With `IncrementalUpdate` enabled, the watcher reads the rules out of the resource and reports only what changed:

- A create or delete becomes an add or remove of the rules the resource carries, batched per section and policy type.
- An edit is diffed: the rules that disappeared are removed and the rules that appeared are added. A resource that gains or loses a line is handled the same way as one whose lines were rewritten.
- Anything that cannot be described this way — a resource that does not follow the layout below, or a malformed policy line — falls back to a full reload, so an unrecognized change is never silently dropped.

Incremental mode requires policy resources to follow the [policy resource layout](#policy-resource-layout). Note that Casbin applies these messages through its `Self*` APIs, which write through to the adapter when `AutoSave` is enabled: if your adapter is backed by the same resources the watcher observes, keep the default reload mode so the observed change is not written straight back.

## Policy Resource Layout

In incremental mode the watcher reads rules from two `spec` fields. Both hold Casbin policy lines in the same format used in a policy CSV file, so the policy type comes first and the section is its first character (`p` and `p2` belong to section `p`, `g` and `g2` to section `g`). Blank lines and `#` comments are skipped.

A single rule, or several newline-separated ones:

```yaml
apiVersion: casbin.org/v1
kind: Policy
metadata:
  name: alice-can-read
spec:
  policy: "p, alice, data1, read"
```

Or a list:

```yaml
apiVersion: casbin.org/v1
kind: Policy
metadata:
  name: team-policies
spec:
  policies:
    - "p, alice, data1, read"
    - "p, bob, data2, write"
    - "g, alice, admin"
```

The default reload mode does not read these fields, and works with any resource layout.

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
                policies:
                  type: array
                  items:
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
