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
	"strings"
	"sync"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/persist"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

// Watcher implements the persist.Watcher interface for Kubernetes CRD-based
// policy updates.
type Watcher struct {
	lock sync.RWMutex

	callbackMu sync.Mutex

	callback func(string)

	running bool

	localID string
	options WatcherOptions

	informer cache.SharedIndexInformer

	stopCh chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
}

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

		if err := msgStruct.UnmarshalBinary([]byte(msg)); err != nil {
			log.Println("Error unmarshaling message:", err)
			return
		}

		var (
			res bool
			err error
		)

		switch msgStruct.Method {
		case Update, UpdateForSavePolicy:
			err = e.LoadPolicy()
			res = err == nil

		case UpdateForAddPolicy:
			res, err = e.SelfAddPolicy(
				msgStruct.Sec,
				msgStruct.Ptype,
				msgStruct.NewRule,
			)

		case UpdateForAddPolicies:
			res, err = e.SelfAddPolicies(
				msgStruct.Sec,
				msgStruct.Ptype,
				msgStruct.NewRules,
			)

		case UpdateForRemovePolicy:
			res, err = e.SelfRemovePolicy(
				msgStruct.Sec,
				msgStruct.Ptype,
				msgStruct.NewRule,
			)

		case UpdateForRemoveFilteredPolicy:
			res, err = e.SelfRemoveFilteredPolicy(
				msgStruct.Sec,
				msgStruct.Ptype,
				msgStruct.FieldIndex,
				msgStruct.FieldValues...,
			)

		case UpdateForRemovePolicies:
			res, err = e.SelfRemovePolicies(
				msgStruct.Sec,
				msgStruct.Ptype,
				msgStruct.NewRules,
			)

		case UpdateForUpdatePolicy:
			res, err = e.SelfUpdatePolicy(
				msgStruct.Sec,
				msgStruct.Ptype,
				msgStruct.OldRule,
				msgStruct.NewRule,
			)

		case UpdateForUpdatePolicies:
			res, err = e.SelfUpdatePolicies(
				msgStruct.Sec,
				msgStruct.Ptype,
				msgStruct.OldRules,
				msgStruct.NewRules,
			)

		default:
			err = errors.New("unknown update type")
		}

		if err != nil {
			log.Println("Error updating policy:", err)
		}

		if !res {
			log.Println("Callback update policy failed")
		}
	}
}

// finalizer is the destructor for Watcher.
func finalizer(w *Watcher) {
	w.Close()
}

// NewWatcher creates a new Watcher instance.
func NewWatcher(
	client dynamic.Interface,
	gvr schema.GroupVersionResource,
	namespace string,
	options WatcherOptions,
) (persist.Watcher, error) {
	if client == nil {
		return nil, errors.New("kubernetes dynamic client cannot be nil")
	}

	initConfig(&options)

	ctx, cancel := context.WithCancel(context.Background())

	w := &Watcher{
		running: true,
		localID: options.LocalID,
		options: options,
		stopCh:  make(chan struct{}),
		ctx:     ctx,
		cancel:  cancel,
	}

	if options.OptionalUpdateCallback != nil {
		w.callback = options.OptionalUpdateCallback
	}

	var factory dynamicinformer.DynamicSharedInformerFactory

	if namespace != "" {
		factory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(
			client,
			options.ResyncPeriod,
			namespace,
			nil,
		)
	} else {
		factory = dynamicinformer.NewDynamicSharedInformerFactory(
			client,
			options.ResyncPeriod,
		)
	}

	w.informer = factory.ForResource(gvr).Informer()

	_, err := w.informer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				w.handleEvent(obj, nil, "add")
			},

			UpdateFunc: func(oldObj, newObj interface{}) {
				w.handleEvent(newObj, oldObj, "update")
			},

			DeleteFunc: func(obj interface{}) {
				w.handleEvent(obj, nil, "delete")
			},
		},
	)

	if err != nil {
		cancel()
		return nil, err
	}

	go w.informer.Run(w.stopCh)

	go func() {
		if !cache.WaitForCacheSync(
			w.stopCh,
			w.informer.HasSynced,
		) {
			log.Println("Failed to sync informer cache")
		}
	}()

	runtime.SetFinalizer(w, finalizer)

	return w, nil
}

// handleEvent processes Kubernetes CRD events.
func (w *Watcher) handleEvent(
	obj interface{},
	oldObj interface{},
	eventType string,
) {
	w.lock.RLock()

	if !w.running {
		w.lock.RUnlock()
		return
	}

	callback := w.callback
	ignoreSelf := w.options.IgnoreSelf
	localID := w.localID

	w.lock.RUnlock()

	if callback == nil {
		return
	}

	current, ok := obj.(*unstructured.Unstructured)
	if !ok {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			current, ok = tombstone.Obj.(*unstructured.Unstructured)
		}

		if !ok {
			log.Println("Failed to convert Kubernetes object to unstructured")
			return
		}
	}

	if ignoreSelf && isSelfUpdate(current, localID) {
		return
	}

	var old *unstructured.Unstructured

	if oldObj != nil {
		old, _ = oldObj.(*unstructured.Unstructured)
	}

	messages := buildEventMessages(eventType, old, current, localID)

	if len(messages) == 0 {
		return
	}

	for _, msg := range messages {
		data, err := msg.MarshalBinary()
		if err != nil {
			log.Println("Error marshaling watcher message:", err)
			continue
		}

		w.invokeCallback(callback, string(data))
	}
}

// invokeCallback serializes callback execution so Kubernetes events are
// applied in the same order in which the watcher observes them.
func (w *Watcher) invokeCallback(callback func(string), msg string) {
	w.callbackMu.Lock()
	defer w.callbackMu.Unlock()

	callback(msg)
}

// isSelfUpdate checks whether an object originated from this watcher.
func isSelfUpdate(obj *unstructured.Unstructured, localID string) bool {
	if obj == nil {
		return false
	}

	annotations := obj.GetAnnotations()

	if annotations == nil {
		return false
	}

	return annotations["casbin.org/source-id"] == localID
}

// buildEventMessages converts a Kubernetes event into Casbin watcher
// messages.
func buildEventMessages(
	eventType string,
	oldObj *unstructured.Unstructured,
	newObj *unstructured.Unstructured,
	localID string,
) []*MSG {
	switch eventType {
	case "add":
		rules, sec, ptype, ok := extractPolicy(newObj)

		if !ok {
			return []*MSG{
				{
					Method: Update,
					ID:     localID,
				},
			}
		}

		if len(rules) == 1 {
			return []*MSG{
				{
					Method:  UpdateForAddPolicy,
					ID:      localID,
					Sec:     sec,
					Ptype:   ptype,
					NewRule: rules[0],
				},
			}
		}

		return []*MSG{
			{
				Method:   UpdateForAddPolicies,
				ID:       localID,
				Sec:      sec,
				Ptype:    ptype,
				NewRules: rules,
			},
		}

	case "delete":
		rules, sec, ptype, ok := extractPolicy(newObj)

		if !ok {
			return []*MSG{
				{
					Method: Update,
					ID:     localID,
				},
			}
		}

		if len(rules) == 1 {
			return []*MSG{
				{
					Method:  UpdateForRemovePolicy,
					ID:      localID,
					Sec:     sec,
					Ptype:   ptype,
					NewRule: rules[0],
				},
			}
		}

		return []*MSG{
			{
				Method:   UpdateForRemovePolicies,
				ID:       localID,
				Sec:      sec,
				Ptype:    ptype,
				NewRules: rules,
			},
		}

	case "update":
		oldRules, oldSec, oldPtype, oldOK := extractPolicy(oldObj)
		newRules, newSec, newPtype, newOK := extractPolicy(newObj)

		if !oldOK || !newOK {
			return []*MSG{
				{
					Method: Update,
					ID:     localID,
				},
			}
		}

		if oldSec != newSec || oldPtype != newPtype {
			return []*MSG{
				{
					Method: Update,
					ID:     localID,
				},
			}
		}

		if len(oldRules) == 1 && len(newRules) == 1 {
			return []*MSG{
				{
					Method:  UpdateForUpdatePolicy,
					ID:      localID,
					Sec:     newSec,
					Ptype:   newPtype,
					OldRule: oldRules[0],
					NewRule: newRules[0],
				},
			}
		}

		return []*MSG{
			{
				Method:   UpdateForUpdatePolicies,
				ID:       localID,
				Sec:      newSec,
				Ptype:    newPtype,
				OldRules: oldRules,
				NewRules: newRules,
			},
		}

	default:
		return []*MSG{
			{
				Method: Update,
				ID:     localID,
			},
		}
	}
}

// extractPolicy extracts Casbin policy information from a CRD.
//
// Supported formats:
//
// spec:
//
//	policy: "p, alice, data1, read"
//
// and:
//
// spec:
//
//	sec: p
//	ptype: p
//	rules:
//	  - "alice, data1, read"
//	  - "bob, data2, write"
//
// A rule may also be represented as a list:
//
// rules:
//   - ["alice", "data1", "read"]
func extractPolicy(obj *unstructured.Unstructured) (
	[][]string,
	string,
	string,
	bool,
) {
	if obj == nil {
		return nil, "", "", false
	}

	spec, ok := obj.Object["spec"].(map[string]interface{})
	if !ok {
		return nil, "", "", false
	}

	sec := "p"
	ptype := "p"

	if value, ok := spec["sec"].(string); ok && value != "" {
		sec = value
	}

	if value, ok := spec["ptype"].(string); ok && value != "" {
		ptype = value
	}

	if policy, ok := spec["policy"].(string); ok {
		ruleSec, rulePtype, rule, valid := parsePolicyString(policy)

		if !valid {
			return nil, "", "", false
		}

		if ruleSec != "" {
			sec = ruleSec
		}

		if rulePtype != "" {
			ptype = rulePtype
		}

		return [][]string{rule}, sec, ptype, true
	}

	rawRules, ok := spec["rules"].([]interface{})
	if !ok {
		return nil, "", "", false
	}

	rules := make([][]string, 0, len(rawRules))

	for _, raw := range rawRules {
		switch value := raw.(type) {
		case string:
			_, _, rule, valid := parsePolicyString(value)

			if !valid {
				return nil, "", "", false
			}

			rules = append(rules, rule)

		case []interface{}:
			rule := make([]string, 0, len(value))

			for _, field := range value {
				fieldString, ok := field.(string)

				if !ok {
					return nil, "", "", false
				}

				rule = append(rule, fieldString)
			}

			if len(rule) == 0 {
				return nil, "", "", false
			}

			rules = append(rules, rule)

		default:
			return nil, "", "", false
		}
	}

	if len(rules) == 0 {
		return nil, "", "", false
	}

	return rules, sec, ptype, true
}

// parsePolicyString parses:
//
// p, alice, data1, read
//
// into:
//
// sec = p
// ptype = p
// rule = [alice data1 read]
func parsePolicyString(policy string) (
	string,
	string,
	[]string,
	bool,
) {
	parts := splitRule(policy)

	if len(parts) < 2 {
		return "", "", nil, false
	}

	sec := parts[0]
	ptype := parts[0]

	if strings.Contains(sec, ".") {
		segments := strings.SplitN(sec, ".", 2)

		if len(segments) == 2 {
			sec = segments[0]
			ptype = segments[1]
			parts = parts[1:]
		}
	} else {
		parts = parts[1:]
	}

	if sec == "" || ptype == "" || len(parts) == 0 {
		return "", "", nil, false
	}

	return sec, ptype, parts, true
}

// splitRule splits a comma-separated Casbin rule.
func splitRule(value string) []string {
	parts := strings.Split(value, ",")

	result := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)

		if part != "" {
			result = append(result, part)
		}
	}

	return result
}

// SetUpdateCallback sets the callback function to be called when a policy
// update is detected.
func (w *Watcher) SetUpdateCallback(callback func(string)) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if !w.running {
		return errors.New("watcher is not running")
	}

	w.callback = callback

	return nil
}

// Update triggers a full update notification.
func (w *Watcher) Update() error {
	return w.UpdateForPolicy(
		Update,
		"",
		"",
		nil,
		nil,
		0,
	)
}

// UpdateForAddPolicy triggers an update for AddPolicy.
func (w *Watcher) UpdateForAddPolicy(
	sec string,
	ptype string,
	params ...string,
) error {
	return w.UpdateForPolicy(
		UpdateForAddPolicy,
		sec,
		ptype,
		params,
		nil,
		0,
	)
}

// UpdateForRemovePolicy triggers an update for RemovePolicy.
func (w *Watcher) UpdateForRemovePolicy(
	sec string,
	ptype string,
	params ...string,
) error {
	return w.UpdateForPolicy(
		UpdateForRemovePolicy,
		sec,
		ptype,
		params,
		nil,
		0,
	)
}

// UpdateForRemoveFilteredPolicy triggers an update for
// RemoveFilteredPolicy.
func (w *Watcher) UpdateForRemoveFilteredPolicy(
	sec string,
	ptype string,
	fieldIndex int,
	fieldValues ...string,
) error {
	return w.UpdateForPolicyWithFieldIndex(
		UpdateForRemoveFilteredPolicy,
		sec,
		ptype,
		fieldIndex,
		fieldValues...,
	)
}

// UpdateForSavePolicy triggers an update for SavePolicy.
func (w *Watcher) UpdateForSavePolicy(
	sec string,
	ptype string,
	params ...string,
) error {
	return w.UpdateForPolicy(
		UpdateForSavePolicy,
		sec,
		ptype,
		params,
		nil,
		0,
	)
}

// UpdateForAddPolicies triggers an update for AddPolicies.
func (w *Watcher) UpdateForAddPolicies(
	sec string,
	ptype string,
	rules ...[]string,
) error {
	return w.UpdateForPolicy(
		UpdateForAddPolicies,
		sec,
		ptype,
		nil,
		rules,
		0,
	)
}

// UpdateForRemovePolicies triggers an update for RemovePolicies.
func (w *Watcher) UpdateForRemovePolicies(
	sec string,
	ptype string,
	rules ...[]string,
) error {
	return w.UpdateForPolicy(
		UpdateForRemovePolicies,
		sec,
		ptype,
		nil,
		rules,
		0,
	)
}

// UpdateForUpdatePolicy triggers an update for UpdatePolicy.
func (w *Watcher) UpdateForUpdatePolicy(
	sec string,
	ptype string,
	oldRule []string,
	newRule []string,
) error {
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
func (w *Watcher) UpdateForUpdatePolicies(
	sec string,
	ptype string,
	oldRules [][]string,
	newRules [][]string,
) error {
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
func (w *Watcher) UpdateForPolicy(
	method UpdateType,
	sec string,
	ptype string,
	params []string,
	rules [][]string,
	fieldIndex int,
) error {
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

// UpdateForPolicyWithFieldIndex is a helper method for filtered policy
// updates.
func (w *Watcher) UpdateForPolicyWithFieldIndex(
	method UpdateType,
	sec string,
	ptype string,
	fieldIndex int,
	fieldValues ...string,
) error {
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

// publishMessage validates the watcher state and logs the outgoing message.
//
// Kubernetes CRD changes are consumed through the informer event stream.
// The Update* methods are retained to satisfy persist.Watcher and to provide
// a consistent serialized representation for future CRD persistence
// integration.
func (w *Watcher) publishMessage(msg *MSG) error {
	w.lock.RLock()
	running := w.running
	w.lock.RUnlock()

	if !running {
		return errors.New("watcher is not running")
	}

	if msg == nil {
		return errors.New("watcher message cannot be nil")
	}

	msgBytes, err := msg.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal watcher message: %w", err)
	}

	log.Printf("Publishing message: %s", string(msgBytes))

	return nil
}

// Close stops the watcher and releases resources.
func (w *Watcher) Close() {
	w.closeOnce.Do(func() {
		w.lock.Lock()

		if !w.running {
			w.lock.Unlock()
			return
		}

		w.running = false
		close(w.stopCh)

		cancel := w.cancel

		w.lock.Unlock()

		if cancel != nil {
			cancel()
		}
	})
}
