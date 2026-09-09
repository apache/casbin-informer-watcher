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
	"encoding/csv"
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// fieldSeparator joins rule fields into a comparable key. It cannot appear in a
// policy line, so two rules share a key only if they are the same rule.
const fieldSeparator = "\x00"

// rule is a single Casbin policy rule carried by a policy resource.
type rule struct {
	sec    string
	ptype  string
	fields []string
}

func (r rule) key() string {
	return strings.Join(append([]string{r.ptype}, r.fields...), fieldSeparator)
}

// extractRules reads the rules held by spec.policy and spec.policies. An error
// means the resource does not follow that layout, and the caller falls back to
// a full policy reload.
func extractRules(obj *unstructured.Unstructured) ([]rule, error) {
	if obj == nil {
		return nil, errors.New("policy resource is nil")
	}

	spec, ok := obj.Object["spec"].(map[string]interface{})
	if !ok {
		return nil, errors.New("policy resource has no spec object")
	}

	lines, err := policyLines(spec)
	if err != nil {
		return nil, err
	}

	rules := make([]rule, 0, len(lines))
	for _, line := range lines {
		r, ok, err := parsePolicyLine(line)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		rules = append(rules, r)
	}

	return rules, nil
}

// policyLines collects the raw policy lines held by spec.policy and
// spec.policies. At least one of the two must be present.
func policyLines(spec map[string]interface{}) ([]string, error) {
	var lines []string
	found := false

	if raw, ok := spec["policy"]; ok {
		text, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("spec.policy must be a string, got %T", raw)
		}

		lines = append(lines, strings.Split(text, "\n")...)
		found = true
	}

	if raw, ok := spec["policies"]; ok {
		items, ok := raw.([]interface{})
		if !ok {
			return nil, fmt.Errorf("spec.policies must be a list, got %T", raw)
		}

		for _, item := range items {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("spec.policies must hold strings, got %T", item)
			}

			lines = append(lines, text)
		}

		found = true
	}

	if !found {
		return nil, errors.New("policy resource has neither spec.policy nor spec.policies")
	}

	return lines, nil
}

// parsePolicyLine parses one line such as "p, alice, data1, read". It mirrors
// persist.LoadPolicyLine so a resource and a policy CSV file are read the same
// way. A blank line or a comment yields ok == false and no error.
func parsePolicyLine(line string) (rule, bool, error) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return rule{}, false, nil
	}

	reader := csv.NewReader(strings.NewReader(line))
	reader.Comma = ','
	reader.Comment = '#'
	reader.TrimLeadingSpace = true

	tokens, err := reader.Read()
	if err != nil {
		return rule{}, false, fmt.Errorf("parse policy line %q: %w", line, err)
	}

	for i := range tokens {
		tokens[i] = strings.TrimSpace(tokens[i])
	}

	ptype := tokens[0]
	if ptype == "" || len(tokens) < 2 {
		return rule{}, false, fmt.Errorf("policy line %q carries no rule fields", line)
	}

	// Casbin takes the section from the first character of the policy type, so
	// "p2" belongs to section "p" and "g2" to section "g".
	return rule{sec: ptype[:1], ptype: ptype, fields: tokens[1:]}, true, nil
}

// groupRules collects rules by section and policy type, preserving the order in
// which each group first appears.
func groupRules(rules []rule) [][]rule {
	groups := make([][]rule, 0, len(rules))
	index := make(map[string]int, len(rules))

	for _, r := range rules {
		key := r.sec + fieldSeparator + r.ptype

		if i, ok := index[key]; ok {
			groups[i] = append(groups[i], r)
			continue
		}

		index[key] = len(groups)
		groups = append(groups, []rule{r})
	}

	return groups
}

// diffRules returns the rules present in want but missing from have.
func diffRules(have, want []rule) []rule {
	seen := make(map[string]struct{}, len(have))
	for _, r := range have {
		seen[r.key()] = struct{}{}
	}

	var missing []rule
	for _, r := range want {
		if _, ok := seen[r.key()]; ok {
			continue
		}

		missing = append(missing, r)
	}

	return missing
}

func ruleFields(rules []rule) [][]string {
	fields := make([][]string, 0, len(rules))
	for _, r := range rules {
		fields = append(fields, r.fields)
	}

	return fields
}
