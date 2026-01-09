# casbin-informer-watcher

A Kubernetes informer-based watcher for Casbin that monitors CRD policy updates and keeps in-memory Casbin state synchronized without periodic polling.

## Features

- 🚀 **Real-time Policy Updates**: Reacts to CRD changes instantly using Kubernetes informers
- 🔄 **Automatic Synchronization**: Keeps in-memory Casbin state up to date without polling
- 🔒 **Concurrency-Safe**: Compatible with `SyncedEnforcer` for thread-safe operations
- 🔌 **Pluggable**: Implements Casbin's watcher interface for easy integration
- 🛡️ **Resilient**: Handles reconnects and resource version drift gracefully
- 📦 **GitOps-Friendly**: CRD updates become effective quickly across all running instances

## Installation

```bash
go get github.com/casbin/casbin-informer-watcher
```

## Quick Start

### 1. Deploy the CRD

First, apply the CasbinPolicy CRD to your Kubernetes cluster:

```bash
kubectl apply -f config/crd/casbinpolicy.yaml
```

### 2. Create Policy Resources

Create CasbinPolicy resources in your cluster:

```yaml
apiVersion: casbin.org/v1alpha1
kind: CasbinPolicy
metadata:
  name: alice-data1-read
  namespace: default
spec:
  ptype: p
  rule:
    - alice
    - data1
    - read
---
apiVersion: casbin.org/v1alpha1
kind: CasbinPolicy
metadata:
  name: alice-admin-role
  namespace: default
spec:
  ptype: g
  rule:
    - alice
    - admin
```

Apply the policies:

```bash
kubectl apply -f examples/policies.yaml
```

### 3. Use the Watcher in Your Application

```go
package main

import (
    "log"
    "time"

    "github.com/casbin/casbin/v2"
    watcher "github.com/casbin/casbin-informer-watcher"
)

func main() {
    // Create a Casbin enforcer
    e, err := casbin.NewEnforcer("model.conf", "policy.csv")
    if err != nil {
        log.Fatal(err)
    }

    // Create and attach the watcher
    w, err := watcher.NewEnforcerWatcher(e, watcher.Options{
        Namespace:    "default",
        ResyncPeriod: 30 * time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer w.Close()

    // Start watching for policy changes
    if err := w.Start(); err != nil {
        log.Fatal(err)
    }

    log.Println("Watcher started, policies will be auto-reloaded on CRD changes")

    // Your application logic here
    // The enforcer will automatically reload when CRDs change
    select {}
}
```

## Usage with SyncedEnforcer

For concurrent environments, use `SyncedEnforcer`:

```go
package main

import (
    "log"
    "time"

    "github.com/casbin/casbin/v2"
    watcher "github.com/casbin/casbin-informer-watcher"
)

func main() {
    // Create a thread-safe SyncedEnforcer
    se, err := casbin.NewSyncedEnforcer("model.conf", "policy.csv")
    if err != nil {
        log.Fatal(err)
    }

    // Create and attach the watcher
    w, err := watcher.NewEnforcerWatcher(se, watcher.Options{
        Namespace:    "default",
        ResyncPeriod: 30 * time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer w.Close()

    // Start watching
    if err := w.Start(); err != nil {
        log.Fatal(err)
    }

    // Now safe for concurrent use
    // Policy updates from CRDs are automatically synchronized
    select {}
}
```

## Configuration Options

The `Options` struct allows you to configure the watcher:

```go
type Options struct {
    // Namespace to watch (empty string for all namespaces)
    Namespace string

    // KubeConfig path (empty for in-cluster config)
    KubeConfig string

    // ResyncPeriod for the informer (default: 30s)
    ResyncPeriod time.Duration
}
```

## Custom Callback

You can set a custom callback to handle policy updates:

```go
w, err := watcher.NewWatcher(watcher.Options{
    Namespace: "default",
})
if err != nil {
    log.Fatal(err)
}

err = w.SetUpdateCallback(func(msg string) {
    log.Printf("Policy update received: %s", msg)
    // Your custom logic here
})
if err != nil {
    log.Fatal(err)
}

if err := w.Start(); err != nil {
    log.Fatal(err)
}
```

## CRD Schema

The `CasbinPolicy` CRD has the following structure:

```yaml
apiVersion: casbin.org/v1alpha1
kind: CasbinPolicy
metadata:
  name: <policy-name>
  namespace: <namespace>
spec:
  ptype: <policy-type>  # p, p2, g, g2, etc.
  rule:                  # Policy rule as array of strings
    - <subject>
    - <object>
    - <action>
status:
  synced: <boolean>
  lastSyncTime: <timestamp>
  message: <status-message>
```

### Policy Types

- **p, p2, ...**: Permission policies (subject, object, action)
- **g, g2, ...**: Role inheritance/grouping policies (user, role)

## How It Works

1. The watcher creates a Kubernetes informer that watches `CasbinPolicy` CRDs
2. When a CRD is created, updated, or deleted, the informer triggers an event
3. The watcher invokes the registered callback (typically to reload the enforcer's policy)
4. For `SyncedEnforcer`, the reload is thread-safe and atomic
5. The informer automatically handles reconnections and maintains resource versions

## GitOps Integration

This watcher is designed to work seamlessly with GitOps workflows:

1. Store your `CasbinPolicy` CRDs in Git
2. Use tools like ArgoCD or Flux to sync them to your cluster
3. The watcher automatically picks up changes and updates all running instances
4. No manual intervention or polling required

## Requirements

- Kubernetes cluster (v1.19+)
- Casbin v2
- Go 1.19+

## Testing

Run the tests:

```bash
go test -v ./...
```

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.