/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type TaskPhase string

const (
	TaskPhaseInPending  TaskPhase = "Pending"
	TaskPhaseInProgress TaskPhase = "Progress"
	TaskPhaseFailed     TaskPhase = "Failed"
	TaskPhaseSuccessful TaskPhase = "Successful"
)

// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// TaskQueue is the Schema for the taskqueues API.
type TaskQueue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TaskQueueSpec   `json:"spec,omitempty"`
	Status TaskQueueStatus `json:"status,omitempty"`
}

// TaskQueueSpec defines the desired state of TaskQueue.
type TaskQueueSpec struct {
	// MaxConcurrentTasks specifies the maximum number of tasks can run concurrently.
	// Defaults to 10 if not set.
	// +kubebuilder:default=10
	// +optional
	MaxConcurrentTasks int `json:"maxConcurrentTasks,omitempty"`

	// Tasks represents the lists of tasks that this queue is responsible for processing.
	Tasks []UnitTask `json:"tasks,omitempty"`
}

// ObjectPhaseRules defines three identification rules of successful phase of the object,
// progressing phase of the object & failed execution of the object.
// To specifies any field of the Operation object, the rule must start with the word `self`.
// Example:
//
//	.status.phase -> self.status.phase
//	.status.observedGeneration -> self.status.observedGeneration
//
// The rules can be any valid expression supported by CEL(Common Expression Language).
// Ref: https://github.com/google/cel-spec
type ObjectPhaseRules struct {
	// Success defines a rule to identify the successful execution of the operation.
	// Example:
	//   success: `has(self.status.phase) && self.status.phase == 'Successful'`
	// Here self.status.phase is pointing to .status.phase field of the Operation object.
	// When .status.phase field presents and becomes `Successful`, the Success rule will satisfy.
	Success string `json:"success"`

	// InProgress defines a rule to identify that applied operation is progressing.
	// Example:
	//   inProgress: `has(self.status.phase) && self.status.phase == 'Progressing'`
	// Here self.status.phase is pointing to .status.phase field of the Operation object.
	// When .status.phase field presents and becomes `Progressing`, the InProgress rule will satisfy.
	InProgress string `json:"inProgress"`

	// Failed defines a rule to identify that applied operation is failed.
	// Example:
	//   inProgress: `has(self.status.phase) && self.status.phase == 'Failed'`
	// Here self.status.phase is pointing to .status.phase field of the Operation object.
	// When .status.phase field presents and becomes `Failed`, the Failed rule will satisfy.
	Failed string `json:"failed"`
}

// TaskQueueStatus defines the observed state of TaskQueue.
type TaskQueueStatus struct {
	// TriggeredTasksPhase contains the status of the triggered tasks.
	// +optional
	TriggeredTasksStatus map[string]map[string]TaskPhase `json:"triggeredTasksStatus,omitempty"`
}
type UnitTask struct {
	Type metav1.GroupKind `json:"type,omitempty"`
	// Rules defines ObjectPhaseRules. It contains three identification rules of successful phase of the object,
	// progressing phase of the object & failed phase of the object.
	// Example:
	// rules:
	//   success:    `has(self.status.phase) && self.status.phase == 'Successful'`
	//   inProgress: `has(self.status.phase) && self.status.phase == 'Progressing'`
	//   failed:     `has(self.status.phase) && self.status.phase == 'Failed'`
	Rules ObjectPhaseRules `json:"rules"`
}

// +kubebuilder:object:root=true
// TaskQueueList contains a list of TaskQueue.
type TaskQueueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TaskQueue `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TaskQueue{}, &TaskQueueList{})
}
