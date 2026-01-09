package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CasbinPolicy represents a Casbin policy rule stored as a CRD
type CasbinPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CasbinPolicySpec   `json:"spec"`
	Status CasbinPolicyStatus `json:"status,omitempty"`
}

// CasbinPolicySpec defines the desired state of CasbinPolicy
type CasbinPolicySpec struct {
	// PType is the policy type (p, p2, g, g2, etc.)
	PType string `json:"ptype"`

	// Rule is the policy rule as a list of strings
	// For example: ["alice", "data1", "read"] for a permission
	// or ["alice", "admin"] for a role mapping
	Rule []string `json:"rule"`
}

// CasbinPolicyStatus defines the observed state of CasbinPolicy
type CasbinPolicyStatus struct {
	// Synced indicates whether this policy has been synced to enforcers
	Synced bool `json:"synced,omitempty"`

	// LastSyncTime is the timestamp of the last successful sync
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`

	// Message contains any status message
	Message string `json:"message,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CasbinPolicyList contains a list of CasbinPolicy
type CasbinPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CasbinPolicy `json:"items"`
}
