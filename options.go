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
	"time"

	"github.com/google/uuid"
)

// WatcherOptions configures the Watcher behavior.
type WatcherOptions struct {
	// LocalID is a unique identifier for this watcher instance.
	// If empty, a UUID will be generated.
	LocalID string

	// IgnoreSelf determines whether to ignore updates triggered by this watcher instance.
	IgnoreSelf bool

	// ResyncPeriod is the period for the informer to resync with the API server.
	// Default is 30 seconds.
	ResyncPeriod time.Duration

	// IncrementalUpdate makes the watcher report the exact rules that changed
	// instead of asking the enforcer to reload its whole policy. Leave it off
	// when the enforcer's adapter is backed by the same resources the watcher
	// observes: Casbin applies these messages through its Self* APIs, which
	// write through to the adapter when AutoSave is enabled.
	IncrementalUpdate bool

	// OptionalUpdateCallback is an optional callback function that can be set during initialization.
	OptionalUpdateCallback func(string)
}

// initConfig initializes default values for WatcherOptions.
func initConfig(options *WatcherOptions) {
	if options.LocalID == "" {
		options.LocalID = uuid.New().String()
	}
	if options.ResyncPeriod == 0 {
		options.ResyncPeriod = 30 * time.Second
	}
}
