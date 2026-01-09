# RBAC Example

This example demonstrates how to use the casbin-informer-watcher with a Role-Based Access Control (RBAC) model.

## Overview

The example shows:
- Creating a Casbin enforcer with RBAC model
- Attaching the Kubernetes informer-based watcher
- Automatic policy reloading when CRDs change
- Thread-safe enforcement using SyncedEnforcer

## Files

- `model.conf`: Casbin RBAC model definition
- `policy.csv`: Initial policies (loaded at startup)
- `policies.yaml`: Kubernetes CRD policies (watched for changes)
- `main.go`: Example application

## Setup

### 1. Deploy the CRD

First, deploy the CasbinPolicy CRD to your cluster:

```bash
kubectl apply -f ../../config/crd/casbinpolicy.yaml
```

### 2. Apply the Policies

Apply the RBAC policies as Kubernetes CRDs:

```bash
kubectl apply -f policies.yaml
```

This creates the following policies:
- `admin` role has read/write access to `data1` and `data2`
- `alice` is assigned the `admin` role
- `bob` is assigned the `admin` role

### 3. Run the Application

```bash
cd examples/rbac
go run main.go
```

The application will:
- Start the enforcer with the RBAC model
- Attach the watcher to monitor CRD changes
- Periodically test authorization decisions
- Automatically reload policies when CRDs change

## Testing Policy Updates

### Add a New Role Mapping

Create a new CRD to grant `charlie` the `admin` role:

```bash
kubectl apply -f - <<EOF
apiVersion: casbin.org/v1alpha1
kind: CasbinPolicy
metadata:
  name: charlie-admin-role
  namespace: default
spec:
  ptype: g
  rule:
    - charlie
    - admin
EOF
```

The watcher will detect this change and reload the policy. Charlie will now have admin access.

### Add a New Permission

Create a new permission policy:

```bash
kubectl apply -f - <<EOF
apiVersion: casbin.org/v1alpha1
kind: CasbinPolicy
metadata:
  name: admin-data3-read
  namespace: default
spec:
  ptype: p
  rule:
    - admin
    - data3
    - read
EOF
```

The admin role (and alice, bob) will now have read access to data3.

### Remove a Policy

Remove a policy:

```bash
kubectl delete casbinpolicy alice-admin-role
```

Alice will immediately lose admin access.

### Update a Policy

To update a policy, delete and recreate it, or use `kubectl edit`:

```bash
kubectl edit casbinpolicy admin-data1-write
```

## How It Works

1. **Initialization**: The application starts with policies from `policy.csv`
2. **Watcher Setup**: The informer-based watcher connects to Kubernetes API
3. **CRD Monitoring**: The watcher listens for CasbinPolicy create/update/delete events
4. **Automatic Reload**: When a CRD changes, the watcher triggers `LoadPolicy()` on the enforcer
5. **Thread Safety**: `SyncedEnforcer` ensures concurrent enforcement is safe during reloads

## Key Features Demonstrated

- ✅ Real-time policy updates without restart
- ✅ Thread-safe enforcement with SyncedEnforcer
- ✅ GitOps-friendly CRD-based policy management
- ✅ Graceful shutdown handling
- ✅ Periodic authorization testing

## Cleanup

Remove the policies:

```bash
kubectl delete -f policies.yaml
```

## Notes

- The watcher uses a resync period of 30 seconds to handle missed events
- The enforcer reloads the entire policy on any CRD change
- For production, consider adding error handling and logging
- CRDs can be managed through GitOps tools like ArgoCD or Flux
