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

// MockEnforcer is a simple mock for testing
type MockEnforcer struct {
	mu            sync.Mutex
	loadPolicyCnt int
}

func (m *MockEnforcer) LoadPolicy() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadPolicyCnt++
	return nil
}

func (m *MockEnforcer) GetLoadPolicyCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadPolicyCnt
}

// TestIntegration_WatcherLifecycle tests the full lifecycle of the watcher
func TestIntegration_WatcherLifecycle(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	client := dynamicfake.NewSimpleDynamicClient(scheme)

	// Create a watcher manually for testing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{
		client:    client,
		stopCh:    make(chan struct{}),
		namespace: "default",
		ctx:       ctx,
		cancel:    cancel,
	}

	var mu sync.Mutex
	eventCount := 0

	err := w.SetUpdateCallback(func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		eventCount++
		t.Logf("Received update: %s", msg)
	})
	if err != nil {
		t.Fatalf("Failed to set callback: %v", err)
	}

	// Simulate policy events
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

	// Test add event
	w.onAdd(policy)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if eventCount != 1 {
		t.Errorf("Expected 1 event after add, got %d", eventCount)
	}
	mu.Unlock()

	// Test update event
	w.onUpdate(policy, policy)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if eventCount != 2 {
		t.Errorf("Expected 2 events after update, got %d", eventCount)
	}
	mu.Unlock()

	// Test delete event
	w.onDelete(policy)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if eventCount != 3 {
		t.Errorf("Expected 3 events after delete, got %d", eventCount)
	}
	mu.Unlock()

	// Test close
	w.running = true
	w.Close()
	if w.running {
		t.Error("Watcher should not be running after Close()")
	}
}

// TestIntegration_PolicySync tests that policy changes trigger enforcer reloads
func TestIntegration_PolicySync(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	client := dynamicfake.NewSimpleDynamicClient(scheme)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockEnforcer := &MockEnforcer{}

	w := &Watcher{
		client:    client,
		stopCh:    make(chan struct{}),
		namespace: "default",
		ctx:       ctx,
		cancel:    cancel,
	}

	// Set callback to reload enforcer
	err := w.SetUpdateCallback(func(msg string) {
		mockEnforcer.LoadPolicy()
	})
	if err != nil {
		t.Fatalf("Failed to set callback: %v", err)
	}

	// Create multiple policy objects
	policies := []*unstructured.Unstructured{
		{
			Object: map[string]interface{}{
				"apiVersion": "casbin.org/v1alpha1",
				"kind":       "CasbinPolicy",
				"metadata": map[string]interface{}{
					"name":      "policy-1",
					"namespace": "default",
				},
				"spec": map[string]interface{}{
					"ptype": "p",
					"rule":  []interface{}{"alice", "data1", "read"},
				},
			},
		},
		{
			Object: map[string]interface{}{
				"apiVersion": "casbin.org/v1alpha1",
				"kind":       "CasbinPolicy",
				"metadata": map[string]interface{}{
					"name":      "policy-2",
					"namespace": "default",
				},
				"spec": map[string]interface{}{
					"ptype": "p",
					"rule":  []interface{}{"bob", "data2", "write"},
				},
			},
		},
		{
			Object: map[string]interface{}{
				"apiVersion": "casbin.org/v1alpha1",
				"kind":       "CasbinPolicy",
				"metadata": map[string]interface{}{
					"name":      "role-mapping",
					"namespace": "default",
				},
				"spec": map[string]interface{}{
					"ptype": "g",
					"rule":  []interface{}{"alice", "admin"},
				},
			},
		},
	}

	// Simulate adding policies
	for _, policy := range policies {
		w.onAdd(policy)
	}

	// Give time for callbacks to execute
	time.Sleep(200 * time.Millisecond)

	loadCount := mockEnforcer.GetLoadPolicyCount()
	if loadCount != len(policies) {
		t.Errorf("Expected %d policy loads, got %d", len(policies), loadCount)
	}

	// Simulate updates
	for _, policy := range policies {
		w.onUpdate(policy, policy)
	}

	time.Sleep(200 * time.Millisecond)

	loadCount = mockEnforcer.GetLoadPolicyCount()
	if loadCount != len(policies)*2 {
		t.Errorf("Expected %d policy loads after updates, got %d", len(policies)*2, loadCount)
	}

	// Simulate deletions
	for _, policy := range policies {
		w.onDelete(policy)
	}

	time.Sleep(200 * time.Millisecond)

	loadCount = mockEnforcer.GetLoadPolicyCount()
	if loadCount != len(policies)*3 {
		t.Errorf("Expected %d policy loads after deletes, got %d", len(policies)*3, loadCount)
	}
}

// TestIntegration_ConcurrentPolicyUpdates tests concurrent policy updates
func TestIntegration_ConcurrentPolicyUpdates(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	client := dynamicfake.NewSimpleDynamicClient(scheme)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockEnforcer := &MockEnforcer{}

	w := &Watcher{
		client:    client,
		stopCh:    make(chan struct{}),
		namespace: "default",
		ctx:       ctx,
		cancel:    cancel,
	}

	err := w.SetUpdateCallback(func(msg string) {
		mockEnforcer.LoadPolicy()
	})
	if err != nil {
		t.Fatalf("Failed to set callback: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	policiesPerGoroutine := 5

	// Simulate concurrent policy additions from multiple sources
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < policiesPerGoroutine; j++ {
				policy := &unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "casbin.org/v1alpha1",
						"kind":       "CasbinPolicy",
						"metadata": map[string]interface{}{
							"name":      "policy-" + string(rune(goroutineID)) + "-" + string(rune(j)),
							"namespace": "default",
						},
						"spec": map[string]interface{}{
							"ptype": "p",
							"rule":  []interface{}{"user", "resource", "action"},
						},
					},
				}
				w.onAdd(policy)
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(300 * time.Millisecond)

	expectedLoads := numGoroutines * policiesPerGoroutine
	loadCount := mockEnforcer.GetLoadPolicyCount()
	if loadCount != expectedLoads {
		t.Errorf("Expected %d policy loads, got %d", expectedLoads, loadCount)
	}
}

// TestIntegration_EventOrdering tests that events are processed in order
func TestIntegration_EventOrdering(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	client := dynamicfake.NewSimpleDynamicClient(scheme)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{
		client:    client,
		stopCh:    make(chan struct{}),
		namespace: "default",
		ctx:       ctx,
		cancel:    cancel,
	}

	var mu sync.Mutex
	events := []string{}

	err := w.SetUpdateCallback(func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, msg)
	})
	if err != nil {
		t.Fatalf("Failed to set callback: %v", err)
	}

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

	// Process events in sequence
	w.onAdd(policy)
	time.Sleep(50 * time.Millisecond)

	w.onUpdate(policy, policy)
	time.Sleep(50 * time.Millisecond)

	w.onDelete(policy)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	expectedEvents := []string{"policy:add", "policy:update", "policy:delete"}
	if len(events) != len(expectedEvents) {
		t.Errorf("Expected %d events, got %d", len(expectedEvents), len(events))
	}

	for i, expected := range expectedEvents {
		if i >= len(events) {
			break
		}
		if events[i] != expected {
			t.Errorf("Event %d: expected %q, got %q", i, expected, events[i])
		}
	}
}
