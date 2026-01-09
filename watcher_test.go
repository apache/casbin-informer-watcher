package watcher

import (
	"context"
	"sync"
	"testing"
	"time"

	v1alpha1 "github.com/casbin/casbin-informer-watcher/pkg/apis/casbin/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestWatcher_SetUpdateCallback(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	client := dynamicfake.NewSimpleDynamicClient(scheme)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{
		client: client,
		stopCh: make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}

	called := false
	err := w.SetUpdateCallback(func(msg string) {
		called = true
	})
	if err != nil {
		t.Errorf("SetUpdateCallback failed: %v", err)
	}

	// Trigger the callback
	w.Update("test")

	// Give some time for the callback to execute
	time.Sleep(100 * time.Millisecond)

	if !called {
		t.Error("Callback was not called")
	}
}

func TestWatcher_Update(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	client := dynamicfake.NewSimpleDynamicClient(scheme)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{
		client: client,
		stopCh: make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}

	var mu sync.Mutex
	messages := []string{}

	err := w.SetUpdateCallback(func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		messages = append(messages, msg)
	})
	if err != nil {
		t.Errorf("SetUpdateCallback failed: %v", err)
	}

	// Send multiple updates
	testMessages := []string{"update1", "update2", "update3"}
	for _, msg := range testMessages {
		if err := w.Update(msg); err != nil {
			t.Errorf("Update failed: %v", err)
		}
	}

	// Give some time for callbacks to execute
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(messages) != len(testMessages) {
		t.Errorf("Expected %d messages, got %d", len(testMessages), len(messages))
	}

	for i, expected := range testMessages {
		if i >= len(messages) {
			break
		}
		if messages[i] != expected {
			t.Errorf("Message %d: expected %q, got %q", i, expected, messages[i])
		}
	}
}

func TestWatcher_Close(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	client := dynamicfake.NewSimpleDynamicClient(scheme)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{
		client:  client,
		stopCh:  make(chan struct{}),
		running: true,
		ctx:     ctx,
		cancel:  cancel,
	}

	w.Close()

	if w.running {
		t.Error("Watcher should not be running after Close()")
	}

	// Check that stopCh is closed
	select {
	case <-w.stopCh:
		// Expected: channel should be closed
	default:
		t.Error("stopCh should be closed after Close()")
	}
}

func TestWatcher_ConcurrentAccess(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	client := dynamicfake.NewSimpleDynamicClient(scheme)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{
		client: client,
		stopCh: make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}

	var wg sync.WaitGroup
	callCount := 0
	var mu sync.Mutex

	// Set a callback
	err := w.SetUpdateCallback(func(msg string) {
		mu.Lock()
		callCount++
		mu.Unlock()
	})
	if err != nil {
		t.Errorf("SetUpdateCallback failed: %v", err)
	}

	// Simulate concurrent updates
	numGoroutines := 10
	updatesPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < updatesPerGoroutine; j++ {
				w.Update("test")
			}
		}(i)
	}

	wg.Wait()

	// Give time for all callbacks to execute
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	expectedCalls := numGoroutines * updatesPerGoroutine
	if callCount != expectedCalls {
		t.Errorf("Expected %d callback calls, got %d", expectedCalls, callCount)
	}
}

func TestWatcher_HandleEvent(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	client := dynamicfake.NewSimpleDynamicClient(scheme)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{
		client: client,
		stopCh: make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}

	var mu sync.Mutex
	events := []string{}

	err := w.SetUpdateCallback(func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, msg)
	})
	if err != nil {
		t.Errorf("SetUpdateCallback failed: %v", err)
	}

	// Create a sample CasbinPolicy object
	policy := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "casbin.org/v1alpha1",
			"kind":       "CasbinPolicy",
			"metadata": map[string]interface{}{
				"name":      "test-policy",
				"namespace": "default",
			},
			"spec": map[string]interface{}{
				"ptype": "p",
				"rule":  []interface{}{"alice", "data1", "read"},
			},
		},
	}

	// Test different event types
	testCases := []struct {
		name      string
		eventType string
		handler   func(interface{})
	}{
		{"Add Event", "add", w.onAdd},
		{"Delete Event", "delete", w.onDelete},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			events = []string{} // Reset events
			mu.Unlock()

			tc.handler(policy)

			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()

			if len(events) != 1 {
				t.Errorf("Expected 1 event, got %d", len(events))
			}

			if len(events) > 0 && events[0] != "policy:"+tc.eventType {
				t.Errorf("Expected event 'policy:%s', got %q", tc.eventType, events[0])
			}
		})
	}
}

func TestWatcher_OnUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	client := dynamicfake.NewSimpleDynamicClient(scheme)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{
		client: client,
		stopCh: make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}

	var mu sync.Mutex
	events := []string{}

	err := w.SetUpdateCallback(func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, msg)
	})
	if err != nil {
		t.Errorf("SetUpdateCallback failed: %v", err)
	}

	oldPolicy := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "casbin.org/v1alpha1",
			"kind":       "CasbinPolicy",
			"metadata": map[string]interface{}{
				"name":      "test-policy",
				"namespace": "default",
			},
			"spec": map[string]interface{}{
				"ptype": "p",
				"rule":  []interface{}{"alice", "data1", "read"},
			},
		},
	}

	newPolicy := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "casbin.org/v1alpha1",
			"kind":       "CasbinPolicy",
			"metadata": map[string]interface{}{
				"name":      "test-policy",
				"namespace": "default",
			},
			"spec": map[string]interface{}{
				"ptype": "p",
				"rule":  []interface{}{"alice", "data1", "write"},
			},
		},
	}

	w.onUpdate(oldPolicy, newPolicy)

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(events))
	}

	if len(events) > 0 && events[0] != "policy:update" {
		t.Errorf("Expected event 'policy:update', got %q", events[0])
	}
}

func TestWatcher_NoCallback(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	client := dynamicfake.NewSimpleDynamicClient(scheme)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{
		client: client,
		stopCh: make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}

	// Don't set a callback, just ensure no panic
	policy := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "casbin.org/v1alpha1",
			"kind":       "CasbinPolicy",
		},
	}

	// These should not panic
	w.onAdd(policy)
	w.onUpdate(policy, policy)
	w.onDelete(policy)
	w.Update("test")
}
