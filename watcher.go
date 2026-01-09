package watcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1alpha1 "github.com/casbin/casbin-informer-watcher/pkg/apis/casbin/v1alpha1"
	"github.com/casbin/casbin/v2"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// Watcher implements the Casbin watcher interface using Kubernetes informers
type Watcher struct {
	// callback is the function to call when a policy update is detected
	callback func(string)

	// client is the Kubernetes dynamic client
	client dynamic.Interface

	// informer is the shared informer for CasbinPolicy resources
	informer cache.SharedIndexInformer

	// factory is the dynamic informer factory
	factory dynamicinformer.DynamicSharedInformerFactory

	// stopCh is used to stop the informer
	stopCh chan struct{}

	// running indicates if the watcher is currently running
	running bool

	// mu protects the running state and callback
	mu sync.RWMutex

	// closeOnce ensures Close is only called once
	closeOnce sync.Once

	// namespace to watch (empty for all namespaces)
	namespace string

	// ctx is the context for the watcher
	ctx context.Context

	// cancel is the cancel function for the context
	cancel context.CancelFunc
}

// Options contains configuration options for the watcher
type Options struct {
	// Namespace to watch (empty for all namespaces)
	Namespace string

	// KubeConfig path (empty for in-cluster config)
	KubeConfig string

	// ResyncPeriod for the informer
	ResyncPeriod time.Duration
}

// NewWatcher creates a new Kubernetes informer-based watcher
func NewWatcher(opts Options) (*Watcher, error) {
	var config *rest.Config
	var err error

	// Create Kubernetes client config
	if opts.KubeConfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", opts.KubeConfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes config: %w", err)
	}

	// Create dynamic client
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Set default resync period
	if opts.ResyncPeriod == 0 {
		opts.ResyncPeriod = 30 * time.Second
	}

	w := &Watcher{
		client:    client,
		stopCh:    make(chan struct{}),
		namespace: opts.Namespace,
		ctx:       ctx,
		cancel:    cancel,
	}

	// Create the scheme and add our types
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to add scheme: %w", err)
	}

	// Create dynamic informer factory
	if opts.Namespace != "" {
		w.factory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(
			client,
			opts.ResyncPeriod,
			opts.Namespace,
			nil,
		)
	} else {
		w.factory = dynamicinformer.NewDynamicSharedInformerFactory(
			client,
			opts.ResyncPeriod,
		)
	}

	// Get informer for CasbinPolicy
	gvr := v1alpha1.SchemeGroupVersion.WithResource("casbinpolicies")
	w.informer = w.factory.ForResource(gvr).Informer()

	// Add event handlers
	_, err = w.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.onAdd,
		UpdateFunc: w.onUpdate,
		DeleteFunc: w.onDelete,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to add event handlers: %w", err)
	}

	return w, nil
}

// SetUpdateCallback sets the callback function to be called on policy updates
func (w *Watcher) SetUpdateCallback(callback func(string)) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.callback = callback
	return nil
}

// Update triggers the update callback with a message
func (w *Watcher) Update(msg string) error {
	w.mu.RLock()
	callback := w.callback
	w.mu.RUnlock()

	if callback != nil {
		callback(msg)
	}
	return nil
}

// Close stops the watcher
func (w *Watcher) Close() {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		defer w.mu.Unlock()

		if !w.running {
			return
		}

		w.running = false
		close(w.stopCh)
		w.cancel()
	})
}

// Start begins watching for CRD changes
func (w *Watcher) Start() error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("watcher is already running")
	}
	w.running = true
	w.mu.Unlock()

	// Start the informer
	w.factory.Start(w.stopCh)

	// Wait for cache sync
	if !cache.WaitForCacheSync(w.stopCh, w.informer.HasSynced) {
		w.mu.Lock()
		w.running = false
		w.mu.Unlock()
		return fmt.Errorf("failed to sync cache")
	}

	return nil
}

// onAdd handles the addition of a new CasbinPolicy
func (w *Watcher) onAdd(obj interface{}) {
	w.handleEvent("add", obj)
}

// onUpdate handles the update of an existing CasbinPolicy
func (w *Watcher) onUpdate(oldObj, newObj interface{}) {
	w.handleEvent("update", newObj)
}

// onDelete handles the deletion of a CasbinPolicy
func (w *Watcher) onDelete(obj interface{}) {
	w.handleEvent("delete", obj)
}

// handleEvent processes policy events and triggers the callback
func (w *Watcher) handleEvent(eventType string, obj interface{}) {
	w.mu.RLock()
	callback := w.callback
	w.mu.RUnlock()

	if callback == nil {
		return
	}

	// Convert the unstructured object to CasbinPolicy if needed
	// For now, we just trigger a reload
	msg := fmt.Sprintf("policy:%s", eventType)
	callback(msg)
}

// NewEnforcerWatcher creates a watcher and attaches it to an enforcer
func NewEnforcerWatcher(e casbin.IEnforcer, opts Options) (*Watcher, error) {
	w, err := NewWatcher(opts)
	if err != nil {
		return nil, err
	}

	// Set the update callback to reload the enforcer's policy
	err = w.SetUpdateCallback(func(msg string) {
		// For SyncedEnforcer, LoadPolicy is thread-safe
		if syncedEnforcer, ok := e.(*casbin.SyncedEnforcer); ok {
			_ = syncedEnforcer.LoadPolicy()
		} else if enforcer, ok := e.(*casbin.Enforcer); ok {
			_ = enforcer.LoadPolicy()
		}
	})
	if err != nil {
		return nil, err
	}

	return w, nil
}
