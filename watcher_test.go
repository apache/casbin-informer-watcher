// Copyright 2026 The casbin Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package informerwatcher

import (
	"sync"
	"testing"
	"time"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var (
	testGVR = schema.GroupVersionResource{
		Group:    "casbin.org",
		Version:  "v1",
		Resource: "policies",
	}
	testGVK = schema.GroupVersionKind{
		Group:   "casbin.org",
		Version: "v1",
		Kind:    "Policy",
	}
	testListGVK = schema.GroupVersionKind{
		Group:   "casbin.org",
		Version: "v1",
		Kind:    "PolicyList",
	}
)

func createTestClient() *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		testGVR: testListGVK.Kind,
	})
}

// TestNewWatcher tests the creation of a new watcher.
func TestNewWatcher(t *testing.T) {
	client := createTestClient()

	watcher, err := NewWatcher(client, testGVR, "", WatcherOptions{})
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	if watcher == nil {
		t.Fatal("Watcher is nil")
	}

	w := watcher.(*Watcher)
	if w.localID == "" {
		t.Error("LocalID should be auto-generated")
	}

	if !w.running {
		t.Error("Watcher should be running")
	}

	watcher.Close()
}

// TestNewWatcherWithOptions tests watcher creation with custom options.
func TestNewWatcherWithOptions(t *testing.T) {

	client := createTestClient()

	customID := "test-watcher-123"
	options := WatcherOptions{
		LocalID:      customID,
		IgnoreSelf:   true,
		ResyncPeriod: 10 * time.Second,
	}

	watcher, err := NewWatcher(client, testGVR, "", options)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	w := watcher.(*Watcher)
	if w.localID != customID {
		t.Errorf("Expected LocalID %s, got %s", customID, w.localID)
	}

	if !w.options.IgnoreSelf {
		t.Error("IgnoreSelf should be true")
	}

	watcher.Close()
}

// TestSetUpdateCallback tests setting the update callback.
func TestSetUpdateCallback(t *testing.T) {

	client := createTestClient()

	watcher, err := NewWatcher(client, testGVR, "", WatcherOptions{})
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	defer watcher.Close()

	callback := func(msg string) {
		// Callback function
	}

	err = watcher.SetUpdateCallback(callback)
	if err != nil {
		t.Fatalf("Failed to set callback: %v", err)
	}

	w := watcher.(*Watcher)
	if w.callback == nil {
		t.Error("Callback should be set")
	}
}

// TestUpdateMethods tests various update methods.
func TestUpdateMethods(t *testing.T) {

	client := createTestClient()

	w, err := NewWatcher(client, testGVR, "", WatcherOptions{})
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	defer w.Close()

	watcher := w.(*Watcher)

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Update", func() error { return watcher.Update() }},
		{"UpdateForAddPolicy", func() error { return watcher.UpdateForAddPolicy("p", "p", "alice", "data1", "read") }},
		{"UpdateForRemovePolicy", func() error { return watcher.UpdateForRemovePolicy("p", "p", "alice", "data1", "read") }},
		{"UpdateForSavePolicy", func() error { return watcher.UpdateForSavePolicy("p", "p") }},
		{"UpdateForAddPolicies", func() error {
			return watcher.UpdateForAddPolicies("p", "p", []string{"alice", "data1", "read"}, []string{"bob", "data2", "write"})
		}},
		{"UpdateForRemovePolicies", func() error {
			return watcher.UpdateForRemovePolicies("p", "p", []string{"alice", "data1", "read"})
		}},
		{"UpdateForRemoveFilteredPolicy", func() error {
			return watcher.UpdateForRemoveFilteredPolicy("p", "p", 0, "alice")
		}},
		{"UpdateForUpdatePolicy", func() error {
			return watcher.UpdateForUpdatePolicy("p", "p", []string{"alice", "data1", "read"}, []string{"alice", "data1", "write"})
		}},
		{"UpdateForUpdatePolicies", func() error {
			return watcher.UpdateForUpdatePolicies("p", "p",
				[][]string{{"alice", "data1", "read"}},
				[][]string{{"alice", "data1", "write"}})
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if err != nil {
				t.Errorf("%s failed: %v", tt.name, err)
			}
		})
	}
}

// TestConcurrency tests concurrent callback execution.
func TestConcurrency(t *testing.T) {

	client := createTestClient()

	w, err := NewWatcher(client, testGVR, "", WatcherOptions{})
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	defer w.Close()

	watcher := w.(*Watcher)

	var mu sync.Mutex
	count := 0

	callback := func(msg string) {
		mu.Lock()
		count++
		mu.Unlock()
	}

	err = watcher.SetUpdateCallback(callback)
	if err != nil {
		t.Fatalf("Failed to set callback: %v", err)
	}

	// Simulate concurrent updates
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = watcher.Update()
		}()
	}
	wg.Wait()
}

// TestHandleEvent tests event handling.
func TestHandleEvent(t *testing.T) {

	client := createTestClient()

	watcher, err := NewWatcher(client, testGVR, "", WatcherOptions{})
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	defer watcher.Close()

	callbackCalled := false
	var receivedMsg string

	callback := func(msg string) {
		callbackCalled = true
		receivedMsg = msg
	}

	err = watcher.SetUpdateCallback(callback)
	if err != nil {
		t.Fatalf("Failed to set callback: %v", err)
	}

	w := watcher.(*Watcher)

	// Create a test object
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "casbin.org/v1",
			"kind":       "Policy",
			"metadata": map[string]interface{}{
				"name":      "test-policy",
				"namespace": "default",
			},
			"spec": map[string]interface{}{
				"policy": "p, alice, data1, read",
			},
		},
	}

	// Handle the event
	w.handleEvent(obj, "add")

	time.Sleep(100 * time.Millisecond)

	if !callbackCalled {
		t.Error("Callback was not called")
	}

	if receivedMsg == "" {
		t.Error("Received message is empty")
	}
}

// TestIgnoreSelf tests the IgnoreSelf functionality.
func TestIgnoreSelf(t *testing.T) {

	client := createTestClient()

	localID := "test-id-123"
	watcher, err := NewWatcher(client, testGVR, "", WatcherOptions{
		LocalID:    localID,
		IgnoreSelf: true,
	})
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	defer watcher.Close()

	callbackCalled := false
	callback := func(msg string) {
		callbackCalled = true
	}

	err = watcher.SetUpdateCallback(callback)
	if err != nil {
		t.Fatalf("Failed to set callback: %v", err)
	}

	w := watcher.(*Watcher)

	// Create an object with our own ID
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "casbin.org/v1",
			"kind":       "Policy",
			"metadata": map[string]interface{}{
				"name":      "test-policy",
				"namespace": "default",
				"annotations": map[string]interface{}{
					"casbin.org/source-id": localID,
				},
			},
		},
	}

	w.handleEvent(obj, "add")
	time.Sleep(100 * time.Millisecond)

	if callbackCalled {
		t.Error("Callback should not be called for own updates when IgnoreSelf is true")
	}
}

// TestDefaultUpdateCallback tests the default update callback.
func TestDefaultUpdateCallback(t *testing.T) {
	// Create a simple test model
	m := model.NewModel()
	m.AddDef("r", "r", "sub, obj, act")
	m.AddDef("p", "p", "sub, obj, act")
	m.AddDef("e", "e", "some(where (p.eft == allow))")
	m.AddDef("m", "m", "r.sub == p.sub && r.obj == p.obj && r.act == p.act")

	e, err := casbin.NewEnforcer(m)
	if err != nil {
		t.Fatalf("Failed to create enforcer: %v", err)
	}

	callback := DefaultUpdateCallback(e)

	// Test UpdateForAddPolicy message (skip Update since it requires an adapter)
	msg := &MSG{
		Method:  UpdateForAddPolicy,
		ID:      "test-id",
		Sec:     "p",
		Ptype:   "p",
		NewRule: []string{"alice", "data1", "read"},
	}

	msgBytes, _ := msg.MarshalBinary()
	callback(string(msgBytes))

	// Verify policy was added
	has, _ := e.HasPolicy("alice", "data1", "read")
	if !has {
		t.Error("Policy was not added")
	}
}

// TestMSGMarshaling tests MSG marshaling and unmarshaling.
func TestMSGMarshaling(t *testing.T) {
	original := &MSG{
		Method:      UpdateForAddPolicy,
		ID:          "test-id-123",
		Sec:         "p",
		Ptype:       "p",
		NewRule:     []string{"alice", "data1", "read"},
		FieldIndex:  0,
		FieldValues: []string{"alice"},
	}

	// Marshal
	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	// Unmarshal
	restored := &MSG{}
	err = restored.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	// Compare
	if restored.Method != original.Method {
		t.Errorf("Method mismatch: got %v, want %v", restored.Method, original.Method)
	}
	if restored.ID != original.ID {
		t.Errorf("ID mismatch: got %v, want %v", restored.ID, original.ID)
	}
	if restored.Sec != original.Sec {
		t.Errorf("Sec mismatch: got %v, want %v", restored.Sec, original.Sec)
	}
	if restored.Ptype != original.Ptype {
		t.Errorf("Ptype mismatch: got %v, want %v", restored.Ptype, original.Ptype)
	}
}

// TestClose tests watcher cleanup.
func TestClose(t *testing.T) {

	client := createTestClient()

	watcher, err := NewWatcher(client, testGVR, "", WatcherOptions{})
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	w := watcher.(*Watcher)
	if !w.running {
		t.Error("Watcher should be running initially")
	}

	watcher.Close()

	if w.running {
		t.Error("Watcher should not be running after Close()")
	}

	// Closing again should be safe
	watcher.Close()
}

// TestUpdateAfterClose tests that updates fail after closing.
func TestUpdateAfterClose(t *testing.T) {

	client := createTestClient()

	w, err := NewWatcher(client, testGVR, "", WatcherOptions{})
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	watcher := w.(*Watcher)
	watcher.Close()

	err = watcher.Update()
	if err == nil {
		t.Error("Update should fail after Close()")
	}
}
