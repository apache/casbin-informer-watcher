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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/persist"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

// Watcher implements the persist.Watcher interface for Kubernetes CRD-based policy updates.
type Watcher struct {
	lock     sync.RWMutex
	callback func(string)
	running  bool
	localID  string
	options  WatcherOptions
	informer cache.SharedIndexInformer
	stopCh   chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc

	// callbackMu serializes callback invocations so the enforcer sees changes
	// in the order the informer reported them.
	callbackMu sync.Mutex
}

// sourceIDAnnotation marks the watcher instance that wrote a policy resource.
const sourceIDAnnotation = "casbin.org/source-id"

// eventKind identifies the informer event being translated.
type eventKind int

const (
	eventAdd eventKind = iota
	eventUpdate
	eventDelete
)

// UpdateType represents the type of policy update.
type UpdateType string

const (
	Update                        UpdateType = "Update"
	UpdateForAddPolicy            UpdateType = "UpdateForAddPolicy"
	UpdateForRemovePolicy         UpdateType = "UpdateForRemovePolicy"
	UpdateForRemoveFilteredPolicy UpdateType = "UpdateForRemoveFilteredPolicy"
	UpdateForSavePolicy           UpdateType = "UpdateForSavePolicy"
	UpdateForAddPolicies          UpdateType = "UpdateForAddPolicies"
	UpdateForRemovePolicies       UpdateType = "UpdateForRemovePolicies"
	UpdateForUpdatePolicy         UpdateType = "UpdateForUpdatePolicy"
	UpdateForUpdatePolicies       UpdateType = "UpdateForUpdatePolicies"
)

// MSG represents a policy update message.
type MSG struct {
	Method      UpdateType `json:"method"`
	ID          string     `json:"id"`
	Sec         string     `json:"sec,omitempty"`
	Ptype       string     `json:"ptype,omitempty"`
	OldRule     []string   `json:"oldRule,omitempty"`
	OldRules    [][]string `json:"oldRules,omitempty"`
	NewRule     []string   `json:"newRule,omitempty"`
	NewRules    [][]string `json:"newRules,omitempty"`
	FieldIndex  int        `json:"fieldIndex,omitempty"`
	FieldValues []string   `json:"fieldValues,omitempty"`
}

// MarshalBinary implements binary marshaling for MSG.
func (m *MSG) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalBinary implements binary unmarshaling for MSG.
func (m *MSG) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}

// DefaultUpdateCallback returns the default callback function for policy updates.
func DefaultUpdateCallback(e casbin.IEnforcer) func(string) {
	return func(msg string) {
		msgStruct := &MSG{}
		err := msgStruct.UnmarshalBinary([]byte(msg))
		if err != nil {
			log.Println("Error unmarshaling message:", err)
			return
		}

		var res bool
		switch msgStruct.Method {
		case Update, UpdateForSavePolicy:
			err = e.LoadPolicy()
			res = err == nil
		case UpdateForAddPolicy:
			res, err = e.SelfAddPolicy(msgStruct.Sec, msgStruct.Ptype, msgStruct.NewRule)
		case UpdateForAddPolicies:
			res, err = e.SelfAddPolicies(msgStruct.Sec, msgStruct.Ptype, msgStruct.NewRules)
		case UpdateForRemovePolicy:
			res, err = e.SelfRemovePolicy(msgStruct.Sec, msgStruct.Ptype, msgStruct.NewRule)
		case UpdateForRemoveFilteredPolicy:
			res, err = e.SelfRemoveFilteredPolicy(msgStruct.Sec, msgStruct.Ptype, msgStruct.FieldIndex, msgStruct.FieldValues...)
		case UpdateForRemovePolicies:
			res, err = e.SelfRemovePolicies(msgStruct.Sec, msgStruct.Ptype, msgStruct.NewRules)
		case UpdateForUpdatePolicy:
			res, err = e.SelfUpdatePolicy(msgStruct.Sec, msgStruct.Ptype, msgStruct.OldRule, msgStruct.NewRule)
		case UpdateForUpdatePolicies:
			res, err = e.SelfUpdatePolicies(msgStruct.Sec, msgStruct.Ptype, msgStruct.OldRules, msgStruct.NewRules)
		default:
			err = errors.New("unknown update type")
		}

		if err != nil {
			log.Println("Error updating policy:", err)
			return
		}
		if !res {
			log.Println("Callback update policy had no effect")
		}
	}
}

// finalizer is the destructor for Watcher.
func finalizer(w *Watcher) {
	w.Close()
}

// NewWatcher creates a new Watcher instance.
func NewWatcher(client dynamic.Interface, gvr schema.GroupVersionResource, namespace string, options WatcherOptions) (persist.Watcher, error) {
	if client == nil {
		return nil, errors.New("dynamic client cannot be nil")
	}

	initConfig(&options)

	ctx, cancel := context.WithCancel(context.Background())
	w := &Watcher{
		running:  true,
		localID:  options.LocalID,
		options:  options,
		callback: options.OptionalUpdateCallback,
		stopCh:   make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}

	// Create dynamic informer factory
	var factory dynamicinformer.DynamicSharedInformerFactory
	if namespace != "" {
		factory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, options.ResyncPeriod, namespace, nil)
	} else {
		factory = dynamicinformer.NewDynamicSharedInformerFactory(client, options.ResyncPeriod)
	}

	// Get informer for the specified GVR
	w.informer = factory.ForResource(gvr).Informer()

	// Add event handlers
	_, err := w.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			// Resources that already exist are replayed as adds while the
			// cache is built; they are state the enforcer already loaded, not
			// a change. HasSynced turns true just before the last replay, so a
			// stray one may slip through - cheaper than gating any later and
			// dropping a resource created moments after startup.
			if !w.informer.HasSynced() {
				return
			}
			w.handleEvent(eventAdd, nil, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			w.handleEvent(eventUpdate, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			w.handleEvent(eventDelete, obj, nil)
		},
	})
	if err != nil {
		cancel()
		return nil, err
	}

	// Start informer
	go w.informer.Run(w.stopCh)

	// Wait for cache sync
	go func() {
		if !cache.WaitForCacheSync(w.stopCh, w.informer.HasSynced) {
			log.Println("Failed to sync informer cache")
		}
	}()

	runtime.SetFinalizer(w, finalizer)

	return w, nil
}

// handleEvent processes CRD events and triggers the callback. oldObj holds the
// resource before the event and newObj after it, so an add has no oldObj and a
// delete has no newObj.
func (w *Watcher) handleEvent(kind eventKind, oldObj, newObj interface{}) {
	w.lock.RLock()
	running := w.running
	callback := w.callback
	w.lock.RUnlock()

	if !running || callback == nil {
		return
	}

	old, err := asPolicyResource(oldObj)
	if err != nil {
		log.Println("Skipping event:", err)
		return
	}

	current, err := asPolicyResource(newObj)
	if err != nil {
		log.Println("Skipping event:", err)
		return
	}

	// A delete carries only the previous state of the resource.
	subject := current
	if subject == nil {
		subject = old
	}
	if subject == nil {
		return
	}

	// Check if this is our own update (if IgnoreSelf is enabled)
	if w.options.IgnoreSelf && subject.GetAnnotations()[sourceIDAnnotation] == w.localID {
		return
	}

	// A resync re-delivers every cached resource as an update, and Kubernetes
	// only bumps resourceVersion on a real write.
	if kind == eventUpdate && old != nil && current != nil &&
		old.GetResourceVersion() == current.GetResourceVersion() {
		return
	}

	for _, msg := range w.buildMessages(kind, old, current) {
		w.notify(callback, msg)
	}
}

// asPolicyResource converts an object delivered by the informer, unwrapping the
// tombstone Kubernetes sends when a deletion was missed while the watch was
// down. A nil object stays nil: that side of the event does not exist.
func asPolicyResource(obj interface{}) (*unstructured.Unstructured, error) {
	if obj == nil {
		return nil, nil
	}

	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}

	resource, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("unexpected object type %T in informer event", obj)
	}

	return resource, nil
}

// buildMessages translates an observed change into the messages that describe
// it: a full reload unless IncrementalUpdate is enabled, otherwise the rules
// that actually changed.
func (w *Watcher) buildMessages(kind eventKind, oldObj, newObj *unstructured.Unstructured) []*MSG {
	if !w.options.IncrementalUpdate {
		return []*MSG{w.newMessage(Update)}
	}

	switch kind {
	case eventAdd:
		rules, err := extractRules(newObj)
		if err != nil {
			return w.reload(err)
		}

		return w.ruleMessages(rules, UpdateForAddPolicy, UpdateForAddPolicies)

	case eventDelete:
		rules, err := extractRules(oldObj)
		if err != nil {
			return w.reload(err)
		}

		return w.ruleMessages(rules, UpdateForRemovePolicy, UpdateForRemovePolicies)

	case eventUpdate:
		oldRules, err := extractRules(oldObj)
		if err != nil {
			return w.reload(err)
		}

		newRules, err := extractRules(newObj)
		if err != nil {
			return w.reload(err)
		}

		// Diffed rather than paired up: SelfUpdatePolicies needs both sides to
		// hold the same number of rules, which a resource that gained or lost
		// a line does not.
		removed := w.ruleMessages(diffRules(newRules, oldRules), UpdateForRemovePolicy, UpdateForRemovePolicies)
		added := w.ruleMessages(diffRules(oldRules, newRules), UpdateForAddPolicy, UpdateForAddPolicies)

		return append(removed, added...)

	default:
		return nil
	}
}

// reload reports why the change could not be described incrementally.
func (w *Watcher) reload(err error) []*MSG {
	log.Println("Falling back to a full policy reload:", err)
	return []*MSG{w.newMessage(Update)}
}

// ruleMessages builds one message per section and policy type.
func (w *Watcher) ruleMessages(rules []rule, single, batch UpdateType) []*MSG {
	groups := groupRules(rules)
	msgs := make([]*MSG, 0, len(groups))

	for _, group := range groups {
		msg := w.newMessage(single)
		msg.Sec = group[0].sec
		msg.Ptype = group[0].ptype

		if len(group) == 1 {
			msg.NewRule = group[0].fields
		} else {
			msg.Method = batch
			msg.NewRules = ruleFields(group)
		}

		msgs = append(msgs, msg)
	}

	return msgs
}

func (w *Watcher) newMessage(method UpdateType) *MSG {
	return &MSG{Method: method, ID: w.localID}
}

// notify hands one message to the callback. Calls are serialized so a change
// made of several messages, such as a rule replacement, is applied in order.
func (w *Watcher) notify(callback func(string), msg *MSG) {
	msgBytes, err := msg.MarshalBinary()
	if err != nil {
		log.Println("Error marshaling message:", err)
		return
	}

	w.callbackMu.Lock()
	defer w.callbackMu.Unlock()

	callback(string(msgBytes))
}

// SetUpdateCallback sets the callback function to be called when a policy update is detected.
func (w *Watcher) SetUpdateCallback(callback func(string)) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.callback = callback
	return nil
}

// Update triggers an update notification to other instances.
func (w *Watcher) Update() error {
	return w.UpdateForPolicy(Update, "", "", nil, nil, 0)
}

// UpdateForAddPolicy triggers an update for AddPolicy.
func (w *Watcher) UpdateForAddPolicy(sec string, ptype string, params ...string) error {
	return w.UpdateForPolicy(UpdateForAddPolicy, sec, ptype, params, nil, 0)
}

// UpdateForRemovePolicy triggers an update for RemovePolicy.
func (w *Watcher) UpdateForRemovePolicy(sec string, ptype string, params ...string) error {
	return w.UpdateForPolicy(UpdateForRemovePolicy, sec, ptype, params, nil, 0)
}

// UpdateForRemoveFilteredPolicy triggers an update for RemoveFilteredPolicy.
func (w *Watcher) UpdateForRemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	return w.UpdateForPolicyWithFieldIndex(UpdateForRemoveFilteredPolicy, sec, ptype, fieldIndex, fieldValues...)
}

// UpdateForSavePolicy triggers an update for SavePolicy.
func (w *Watcher) UpdateForSavePolicy(sec string, ptype string, params ...string) error {
	return w.UpdateForPolicy(UpdateForSavePolicy, sec, ptype, params, nil, 0)
}

// UpdateForAddPolicies triggers an update for AddPolicies.
func (w *Watcher) UpdateForAddPolicies(sec string, ptype string, rules ...[]string) error {
	return w.UpdateForPolicy(UpdateForAddPolicies, sec, ptype, nil, rules, 0)
}

// UpdateForRemovePolicies triggers an update for RemovePolicies.
func (w *Watcher) UpdateForRemovePolicies(sec string, ptype string, rules ...[]string) error {
	return w.UpdateForPolicy(UpdateForRemovePolicies, sec, ptype, nil, rules, 0)
}

// UpdateForUpdatePolicy triggers an update for UpdatePolicy.
func (w *Watcher) UpdateForUpdatePolicy(sec string, ptype string, oldRule, newRule []string) error {
	msg := &MSG{
		Method:  UpdateForUpdatePolicy,
		ID:      w.localID,
		Sec:     sec,
		Ptype:   ptype,
		OldRule: oldRule,
		NewRule: newRule,
	}
	return w.publishMessage(msg)
}

// UpdateForUpdatePolicies triggers an update for UpdatePolicies.
func (w *Watcher) UpdateForUpdatePolicies(sec string, ptype string, oldRules, newRules [][]string) error {
	msg := &MSG{
		Method:   UpdateForUpdatePolicies,
		ID:       w.localID,
		Sec:      sec,
		Ptype:    ptype,
		OldRules: oldRules,
		NewRules: newRules,
	}
	return w.publishMessage(msg)
}

// UpdateForPolicy is a helper method for triggering policy updates.
func (w *Watcher) UpdateForPolicy(method UpdateType, sec string, ptype string, params []string, rules [][]string, fieldIndex int) error {
	msg := &MSG{
		Method: method,
		ID:     w.localID,
		Sec:    sec,
		Ptype:  ptype,
	}

	if params != nil {
		msg.NewRule = params
	}
	if rules != nil {
		msg.NewRules = rules
	}

	return w.publishMessage(msg)
}

// UpdateForPolicyWithFieldIndex is a helper method for filtered policy updates.
func (w *Watcher) UpdateForPolicyWithFieldIndex(method UpdateType, sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	msg := &MSG{
		Method:      method,
		ID:          w.localID,
		Sec:         sec,
		Ptype:       ptype,
		FieldIndex:  fieldIndex,
		FieldValues: fieldValues,
	}
	return w.publishMessage(msg)
}

// publishMessage publishes a message (in this implementation, it just logs).
// In a real implementation, this would update the CRD or send notifications.
func (w *Watcher) publishMessage(msg *MSG) error {
	w.lock.RLock()
	running := w.running
	w.lock.RUnlock()

	if !running {
		return errors.New("watcher is not running")
	}

	// In a real implementation, this would update a Kubernetes resource
	// For now, we just log the message
	msgBytes, err := msg.MarshalBinary()
	if err != nil {
		return err
	}

	log.Printf("Publishing message: %s\n", string(msgBytes))
	return nil
}

// Close stops the watcher and releases resources.
func (w *Watcher) Close() {
	w.lock.Lock()
	defer w.lock.Unlock()

	if !w.running {
		return
	}

	w.running = false
	close(w.stopCh)
	if w.cancel != nil {
		w.cancel()
	}
}
