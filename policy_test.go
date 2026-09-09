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
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// newPolicyResource builds a policy resource carrying the given spec.
func newPolicyResource(spec map[string]interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "casbin.org/v1",
			"kind":       "Policy",
			"metadata": map[string]interface{}{
				"name":      "test-policy",
				"namespace": "default",
			},
			"spec": spec,
		},
	}
}

// TestParsePolicyLine tests parsing of individual Casbin policy lines.
func TestParsePolicyLine(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		want    rule
		wantOK  bool
		wantErr bool
	}{
		{
			name:   "policy rule",
			line:   "p, alice, data1, read",
			want:   rule{sec: "p", ptype: "p", fields: []string{"alice", "data1", "read"}},
			wantOK: true,
		},
		{
			name:   "grouping rule",
			line:   "g, alice, admin",
			want:   rule{sec: "g", ptype: "g", fields: []string{"alice", "admin"}},
			wantOK: true,
		},
		{
			// The section is the first character of the policy type.
			name:   "numbered grouping rule",
			line:   "g2, alice, admin, domain1",
			want:   rule{sec: "g", ptype: "g2", fields: []string{"alice", "admin", "domain1"}},
			wantOK: true,
		},
		{
			name:   "numbered policy rule",
			line:   "p2, alice, data1, read",
			want:   rule{sec: "p", ptype: "p2", fields: []string{"alice", "data1", "read"}},
			wantOK: true,
		},
		{
			name:   "quoted field holding a comma",
			line:   `p, alice, "data1,data2", read`,
			want:   rule{sec: "p", ptype: "p", fields: []string{"alice", "data1,data2", "read"}},
			wantOK: true,
		},
		{
			name:   "untrimmed line",
			line:   "   p ,  alice ,  data1 ,  read   ",
			want:   rule{sec: "p", ptype: "p", fields: []string{"alice", "data1", "read"}},
			wantOK: true,
		},
		{
			name:   "blank line",
			line:   "   ",
			wantOK: false,
		},
		{
			name:   "comment",
			line:   "# p, alice, data1, read",
			wantOK: false,
		},
		{
			name:    "policy type without fields",
			line:    "p",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := parsePolicyLine(tt.line)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("Expected an error for %q", tt.line)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error for %q: %v", tt.line, err)
			}

			if ok != tt.wantOK {
				t.Fatalf("Expected ok %v, got %v", tt.wantOK, ok)
			}

			if ok && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Expected rule %+v, got %+v", tt.want, got)
			}
		})
	}
}

// TestExtractRules tests reading rules out of a policy resource.
func TestExtractRules(t *testing.T) {
	tests := []struct {
		name    string
		spec    map[string]interface{}
		want    []rule
		wantErr bool
	}{
		{
			name: "single policy",
			spec: map[string]interface{}{"policy": "p, alice, data1, read"},
			want: []rule{{sec: "p", ptype: "p", fields: []string{"alice", "data1", "read"}}},
		},
		{
			name: "multi-line policy",
			spec: map[string]interface{}{"policy": "p, alice, data1, read\ng, alice, admin\n"},
			want: []rule{
				{sec: "p", ptype: "p", fields: []string{"alice", "data1", "read"}},
				{sec: "g", ptype: "g", fields: []string{"alice", "admin"}},
			},
		},
		{
			name: "policy list",
			spec: map[string]interface{}{
				"policies": []interface{}{"p, alice, data1, read", "p, bob, data2, write"},
			},
			want: []rule{
				{sec: "p", ptype: "p", fields: []string{"alice", "data1", "read"}},
				{sec: "p", ptype: "p", fields: []string{"bob", "data2", "write"}},
			},
		},
		{
			name: "empty policy list",
			spec: map[string]interface{}{"policies": []interface{}{}},
			want: []rule{},
		},
		{
			name:    "unknown layout",
			spec:    map[string]interface{}{"unexpected": "field"},
			wantErr: true,
		},
		{
			name:    "policy of the wrong type",
			spec:    map[string]interface{}{"policy": 42},
			wantErr: true,
		},
		{
			name:    "policy list holding a non-string",
			spec:    map[string]interface{}{"policies": []interface{}{42}},
			wantErr: true,
		},
		{
			name:    "malformed policy line",
			spec:    map[string]interface{}{"policy": "p"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractRules(newPolicyResource(tt.spec))

			if tt.wantErr {
				if err == nil {
					t.Fatalf("Expected an error, got rules %+v", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Expected rules %+v, got %+v", tt.want, got)
			}
		})
	}
}

// TestExtractRulesWithoutSpec tests a resource that carries no spec at all.
func TestExtractRulesWithoutSpec(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{"kind": "Policy"}}

	if _, err := extractRules(obj); err == nil {
		t.Error("Expected an error for a resource without a spec")
	}

	if _, err := extractRules(nil); err == nil {
		t.Error("Expected an error for a nil resource")
	}
}

// TestGroupRules tests that rules are grouped by section and policy type.
func TestGroupRules(t *testing.T) {
	rules := []rule{
		{sec: "p", ptype: "p", fields: []string{"alice", "data1", "read"}},
		{sec: "g", ptype: "g", fields: []string{"alice", "admin"}},
		{sec: "p", ptype: "p", fields: []string{"bob", "data2", "write"}},
		{sec: "g", ptype: "g2", fields: []string{"admin", "root"}},
	}

	groups := groupRules(rules)

	if len(groups) != 3 {
		t.Fatalf("Expected 3 groups, got %d", len(groups))
	}

	if len(groups[0]) != 2 || groups[0][0].ptype != "p" {
		t.Errorf("Expected the two p rules in the first group, got %+v", groups[0])
	}

	if len(groups[1]) != 1 || groups[1][0].ptype != "g" {
		t.Errorf("Expected the g rule in the second group, got %+v", groups[1])
	}

	if len(groups[2]) != 1 || groups[2][0].ptype != "g2" {
		t.Errorf("Expected the g2 rule in the third group, got %+v", groups[2])
	}
}

// TestDiffRules tests the rule diff used to translate update events.
func TestDiffRules(t *testing.T) {
	alice := rule{sec: "p", ptype: "p", fields: []string{"alice", "data1", "read"}}
	bob := rule{sec: "p", ptype: "p", fields: []string{"bob", "data2", "write"}}
	// Same fields as alice but a different policy type.
	alice2 := rule{sec: "p", ptype: "p2", fields: []string{"alice", "data1", "read"}}

	missing := diffRules([]rule{alice}, []rule{alice, bob, alice2})

	if len(missing) != 2 {
		t.Fatalf("Expected 2 missing rules, got %d: %+v", len(missing), missing)
	}

	if !reflect.DeepEqual(missing[0], bob) || !reflect.DeepEqual(missing[1], alice2) {
		t.Errorf("Unexpected missing rules: %+v", missing)
	}

	if got := diffRules([]rule{alice, bob}, []rule{alice, bob}); len(got) != 0 {
		t.Errorf("Expected no missing rules, got %+v", got)
	}
}
