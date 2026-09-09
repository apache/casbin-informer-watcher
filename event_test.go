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
	"reflect"
	"sync"
	"testing"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"github.com/casbin/casbin/v2/persist"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

const testModel = `
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub) && r.obj == p.obj && r.act == p.act
`

// collector records the messages a watcher hands to its callback.
type collector struct {
	mu   sync.Mutex
	msgs []*MSG
}

func (c *collector) callback(msg string) {
	decoded := &MSG{}
	if err := decoded.UnmarshalBinary([]byte(msg)); err != nil {
		panic(err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.msgs = append(c.msgs, decoded)
}

func (c *collector) take() []*MSG {
	c.mu.Lock()
	defer c.mu.Unlock()

	msgs := c.msgs
	c.msgs = nil

	return msgs
}

func newTestWatcher(t *testing.T, options WatcherOptions) (*Watcher, *collector) {
	t.Helper()

	msgs := &collector{}
	options.OptionalUpdateCallback = msgs.callback

	watcher, err := NewWatcher(createTestClient(), testGVR, "", options)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	t.Cleanup(watcher.Close)

	return watcher.(*Watcher), msgs
}

func withVersion(obj *unstructured.Unstructured, version string) *unstructured.Unstructured {
	obj.SetResourceVersion(version)
	return obj
}

// TestOptionalUpdateCallback tests that a callback passed through the options
// is used without a separate SetUpdateCallback call.
func TestOptionalUpdateCallback(t *testing.T) {
	w, msgs := newTestWatcher(t, WatcherOptions{})

	w.handleEvent(eventAdd, nil, newPolicyResource(map[string]interface{}{
		"policy": "p, alice, data1, read",
	}))

	if got := msgs.take(); len(got) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(got))
	}
}

// TestNewWatcherWithNilClient tests that an unusable configuration is rejected.
func TestNewWatcherWithNilClient(t *testing.T) {
	if _, err := NewWatcher(nil, testGVR, "", WatcherOptions{}); err == nil {
		t.Error("Expected an error when the dynamic client is nil")
	}
}

// TestReloadOnChange tests that, by default, every change asks the enforcer for
// a full reload.
func TestReloadOnChange(t *testing.T) {
	w, msgs := newTestWatcher(t, WatcherOptions{})

	obj := withVersion(newPolicyResource(map[string]interface{}{
		"policy": "p, alice, data1, read",
	}), "1")
	updated := withVersion(newPolicyResource(map[string]interface{}{
		"policy": "p, alice, data1, write",
	}), "2")

	w.handleEvent(eventAdd, nil, obj)
	w.handleEvent(eventUpdate, obj, updated)
	w.handleEvent(eventDelete, updated, nil)

	got := msgs.take()
	if len(got) != 3 {
		t.Fatalf("Expected 3 messages, got %d", len(got))
	}

	for i, msg := range got {
		if msg.Method != Update {
			t.Errorf("Message %d: expected method %q, got %q", i, Update, msg.Method)
		}
	}
}

// TestIncrementalAddAndDelete tests that a created or removed resource is
// translated into the matching policy operations.
func TestIncrementalAddAndDelete(t *testing.T) {
	tests := []struct {
		name       string
		wantSingle UpdateType
		wantBatch  UpdateType
	}{
		{"add", UpdateForAddPolicy, UpdateForAddPolicies},
		{"delete", UpdateForRemovePolicy, UpdateForRemovePolicies},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, msgs := newTestWatcher(t, WatcherOptions{IncrementalUpdate: true})

			single := newPolicyResource(map[string]interface{}{
				"policy": "p, alice, data1, read",
			})
			batch := newPolicyResource(map[string]interface{}{
				"policies": []interface{}{
					"p, alice, data1, read",
					"p, bob, data2, write",
					"g, alice, admin",
				},
			})

			fire := func(obj *unstructured.Unstructured) {
				if tt.name == "add" {
					w.handleEvent(eventAdd, nil, obj)
					return
				}
				w.handleEvent(eventDelete, obj, nil)
			}

			fire(single)

			got := msgs.take()
			if len(got) != 1 {
				t.Fatalf("Expected 1 message, got %d", len(got))
			}
			if got[0].Method != tt.wantSingle {
				t.Errorf("Expected method %q, got %q", tt.wantSingle, got[0].Method)
			}
			if !reflect.DeepEqual(got[0].NewRule, []string{"alice", "data1", "read"}) {
				t.Errorf("Unexpected rule: %v", got[0].NewRule)
			}

			fire(batch)

			// The p rules batch together; the g rule is its own section.
			got = msgs.take()
			if len(got) != 2 {
				t.Fatalf("Expected 2 messages, got %d", len(got))
			}

			if got[0].Method != tt.wantBatch || got[0].Sec != "p" || got[0].Ptype != "p" {
				t.Errorf("Unexpected p message: %+v", got[0])
			}
			if len(got[0].NewRules) != 2 {
				t.Errorf("Expected 2 batched p rules, got %v", got[0].NewRules)
			}

			if got[1].Method != tt.wantSingle || got[1].Sec != "g" || got[1].Ptype != "g" {
				t.Errorf("Unexpected g message: %+v", got[1])
			}
		})
	}
}

// TestIncrementalUpdate tests that an edited resource is translated into the
// rules that were actually removed and added.
func TestIncrementalUpdate(t *testing.T) {
	tests := []struct {
		name string
		old  []interface{}
		new  []interface{}
		want []*MSG
	}{
		{
			name: "rule replaced",
			old:  []interface{}{"p, alice, data1, read"},
			new:  []interface{}{"p, alice, data1, write"},
			want: []*MSG{
				{Method: UpdateForRemovePolicy, Sec: "p", Ptype: "p", NewRule: []string{"alice", "data1", "read"}},
				{Method: UpdateForAddPolicy, Sec: "p", Ptype: "p", NewRule: []string{"alice", "data1", "write"}},
			},
		},
		{
			name: "rule added",
			old:  []interface{}{"p, alice, data1, read"},
			new:  []interface{}{"p, alice, data1, read", "p, bob, data2, write"},
			want: []*MSG{
				{Method: UpdateForAddPolicy, Sec: "p", Ptype: "p", NewRule: []string{"bob", "data2", "write"}},
			},
		},
		{
			name: "rule removed",
			old:  []interface{}{"p, alice, data1, read", "p, bob, data2, write"},
			new:  []interface{}{"p, alice, data1, read"},
			want: []*MSG{
				{Method: UpdateForRemovePolicy, Sec: "p", Ptype: "p", NewRule: []string{"bob", "data2", "write"}},
			},
		},
		{
			name: "resource emptied",
			old:  []interface{}{"p, alice, data1, read", "p, bob, data2, write"},
			new:  []interface{}{},
			want: []*MSG{
				{Method: UpdateForRemovePolicies, Sec: "p", Ptype: "p", NewRules: [][]string{
					{"alice", "data1", "read"},
					{"bob", "data2", "write"},
				}},
			},
		},
		{
			name: "rules reordered only",
			old:  []interface{}{"p, alice, data1, read", "p, bob, data2, write"},
			new:  []interface{}{"p, bob, data2, write", "p, alice, data1, read"},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, msgs := newTestWatcher(t, WatcherOptions{IncrementalUpdate: true})

			oldObj := withVersion(newPolicyResource(map[string]interface{}{"policies": tt.old}), "1")
			newObj := withVersion(newPolicyResource(map[string]interface{}{"policies": tt.new}), "2")

			w.handleEvent(eventUpdate, oldObj, newObj)

			got := msgs.take()
			if len(got) != len(tt.want) {
				t.Fatalf("Expected %d messages, got %d: %+v", len(tt.want), len(got), got)
			}

			for i, want := range tt.want {
				want.ID = w.localID

				if !reflect.DeepEqual(got[i], want) {
					t.Errorf("Message %d: expected %+v, got %+v", i, want, got[i])
				}
			}
		})
	}
}

// TestResyncIsIgnored tests that the periodic resync, which re-delivers every
// cached resource unchanged, produces no messages.
func TestResyncIsIgnored(t *testing.T) {
	for _, incremental := range []bool{false, true} {
		w, msgs := newTestWatcher(t, WatcherOptions{IncrementalUpdate: incremental})

		obj := withVersion(newPolicyResource(map[string]interface{}{
			"policy": "p, alice, data1, read",
		}), "42")

		w.handleEvent(eventUpdate, obj, obj.DeepCopy())

		if got := msgs.take(); len(got) != 0 {
			t.Errorf("IncrementalUpdate=%v: expected no messages for a resync, got %+v", incremental, got)
		}
	}
}

// TestDeleteTombstone tests the tombstone Kubernetes delivers when a deletion
// was missed while the watch was down.
func TestDeleteTombstone(t *testing.T) {
	w, msgs := newTestWatcher(t, WatcherOptions{IncrementalUpdate: true})

	tombstone := cache.DeletedFinalStateUnknown{
		Key: "default/test-policy",
		Obj: newPolicyResource(map[string]interface{}{"policy": "p, alice, data1, read"}),
	}

	w.handleEvent(eventDelete, tombstone, nil)

	got := msgs.take()
	if len(got) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(got))
	}

	if got[0].Method != UpdateForRemovePolicy {
		t.Errorf("Expected method %q, got %q", UpdateForRemovePolicy, got[0].Method)
	}

	w.handleEvent(eventDelete, cache.DeletedFinalStateUnknown{Key: "default/gone"}, nil)

	if got := msgs.take(); len(got) != 0 {
		t.Errorf("Expected no messages for an empty tombstone, got %+v", got)
	}
}

// TestIncrementalFallsBackToReload tests that a change which cannot be
// described incrementally still reaches the enforcer as a full reload.
func TestIncrementalFallsBackToReload(t *testing.T) {
	tests := []struct {
		name string
		spec map[string]interface{}
	}{
		{"unknown layout", map[string]interface{}{"unexpected": "field"}},
		{"malformed policy line", map[string]interface{}{"policy": "p"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, msgs := newTestWatcher(t, WatcherOptions{IncrementalUpdate: true})

			w.handleEvent(eventAdd, nil, newPolicyResource(tt.spec))

			got := msgs.take()
			if len(got) != 1 {
				t.Fatalf("Expected 1 message, got %d", len(got))
			}

			if got[0].Method != Update {
				t.Errorf("Expected method %q, got %q", Update, got[0].Method)
			}
		})
	}
}

// TestNoEventsAfterClose tests that a closed watcher stops reporting changes.
func TestNoEventsAfterClose(t *testing.T) {
	w, msgs := newTestWatcher(t, WatcherOptions{})

	w.Close()
	w.handleEvent(eventAdd, nil, newPolicyResource(map[string]interface{}{
		"policy": "p, alice, data1, read",
	}))

	if got := msgs.take(); len(got) != 0 {
		t.Errorf("Expected no messages after Close, got %+v", got)
	}
}

// memoryAdapter stands in for an adapter backed by the same resources the
// watcher observes: a test changes its contents to mimic a GitOps apply, then
// delivers the matching informer event.
type memoryAdapter struct {
	mu    sync.Mutex
	lines []string
}

func (a *memoryAdapter) setLines(lines ...string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.lines = lines
}

func (a *memoryAdapter) LoadPolicy(m model.Model) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, line := range a.lines {
		if err := persist.LoadPolicyLine(line, m); err != nil {
			return err
		}
	}

	return nil
}

func (a *memoryAdapter) SavePolicy(model.Model) error                { return nil }
func (a *memoryAdapter) AddPolicy(string, string, []string) error    { return nil }
func (a *memoryAdapter) RemovePolicy(string, string, []string) error { return nil }
func (a *memoryAdapter) RemoveFilteredPolicy(string, string, int, ...string) error {
	return nil
}

// TestEnforcementFollowsReload tests that create, update and delete events
// reach enforcement decisions in the default reload mode.
func TestEnforcementFollowsReload(t *testing.T) {
	adapter := &memoryAdapter{lines: []string{"p, alice, data1, read"}}

	m, err := model.NewModelFromString(testModel)
	if err != nil {
		t.Fatalf("Failed to build model: %v", err)
	}

	e, err := casbin.NewEnforcer(m, adapter)
	if err != nil {
		t.Fatalf("Failed to create enforcer: %v", err)
	}

	w, _ := newTestWatcher(t, WatcherOptions{})
	if err := w.SetUpdateCallback(DefaultUpdateCallback(e)); err != nil {
		t.Fatalf("Failed to set callback: %v", err)
	}

	assertEnforce(t, e, true, "alice", "data1", "read")

	adapter.setLines("p, alice, data1, write")
	w.handleEvent(eventUpdate,
		withVersion(newPolicyResource(map[string]interface{}{"policy": "p, alice, data1, read"}), "1"),
		withVersion(newPolicyResource(map[string]interface{}{"policy": "p, alice, data1, write"}), "2"))

	assertEnforce(t, e, false, "alice", "data1", "read")
	assertEnforce(t, e, true, "alice", "data1", "write")

	adapter.setLines()
	w.handleEvent(eventDelete,
		withVersion(newPolicyResource(map[string]interface{}{"policy": "p, alice, data1, write"}), "2"), nil)

	assertEnforce(t, e, false, "alice", "data1", "write")
}

// TestEnforcementFollowsIncrementalUpdates tests that create, update and delete
// events reach enforcement decisions in incremental mode, including the role
// links a grouping rule brings with it.
func TestEnforcementFollowsIncrementalUpdates(t *testing.T) {
	m, err := model.NewModelFromString(testModel)
	if err != nil {
		t.Fatalf("Failed to build model: %v", err)
	}

	e, err := casbin.NewEnforcer(m)
	if err != nil {
		t.Fatalf("Failed to create enforcer: %v", err)
	}

	w, _ := newTestWatcher(t, WatcherOptions{IncrementalUpdate: true})
	if err := w.SetUpdateCallback(DefaultUpdateCallback(e)); err != nil {
		t.Fatalf("Failed to set callback: %v", err)
	}

	direct := map[string]interface{}{"policy": "p, alice, data1, read"}
	w.handleEvent(eventAdd, nil, withVersion(newPolicyResource(direct), "1"))
	assertEnforce(t, e, true, "alice", "data1", "read")

	// Access granted indirectly, so the grouping rule has to rebuild the
	// enforcer's role links.
	roles := map[string]interface{}{
		"policies": []interface{}{"g, alice, admin", "p, admin, data2, write"},
	}
	w.handleEvent(eventAdd, nil, withVersion(newPolicyResource(roles), "1"))
	assertEnforce(t, e, true, "alice", "data2", "write")

	swapped := map[string]interface{}{"policy": "p, alice, data1, write"}
	w.handleEvent(eventUpdate,
		withVersion(newPolicyResource(direct), "1"),
		withVersion(newPolicyResource(swapped), "2"))

	assertEnforce(t, e, false, "alice", "data1", "read")
	assertEnforce(t, e, true, "alice", "data1", "write")

	w.handleEvent(eventDelete, withVersion(newPolicyResource(roles), "1"), nil)
	assertEnforce(t, e, false, "alice", "data2", "write")
	assertEnforce(t, e, true, "alice", "data1", "write")
}

// TestConcurrentEventsWithSyncedEnforcer tests that concurrent events stay safe
// when the callback drives a SyncedEnforcer. Run with -race.
func TestConcurrentEventsWithSyncedEnforcer(t *testing.T) {
	m, err := model.NewModelFromString(testModel)
	if err != nil {
		t.Fatalf("Failed to build model: %v", err)
	}

	e, err := casbin.NewSyncedEnforcer(m)
	if err != nil {
		t.Fatalf("Failed to create enforcer: %v", err)
	}

	w, _ := newTestWatcher(t, WatcherOptions{IncrementalUpdate: true})
	if err := w.SetUpdateCallback(DefaultUpdateCallback(e)); err != nil {
		t.Fatalf("Failed to set callback: %v", err)
	}

	const users = 20

	var wg sync.WaitGroup
	for i := 0; i < users; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			obj := newPolicyResource(map[string]interface{}{
				"policy": "p, user" + string(rune('a'+i%26)) + ", data1, read",
			})

			w.handleEvent(eventAdd, nil, withVersion(obj, "1"))
			_, _ = e.Enforce("alice", "data1", "read")
		}(i)
	}

	wg.Wait()

	assertEnforce(t, e, true, "usera", "data1", "read")
}

func assertEnforce(t *testing.T, e casbin.IEnforcer, want bool, request ...interface{}) {
	t.Helper()

	got, err := e.Enforce(request...)
	if err != nil {
		t.Fatalf("Enforce%v failed: %v", request, err)
	}

	if got != want {
		t.Errorf("Enforce%v: expected %v, got %v", request, want, got)
	}
}
