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
	"k8s.io/apimachinery/pkg/runtime"
)

// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// PendingTask is the Schema for the pendingtasks API.
type PendingTask struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PendingTaskSpec   `json:"spec,omitempty"`
	Status PendingTaskStatus `json:"status,omitempty"`
}

// PendingTaskSpec defines the desired state of PendingTask.
type PendingTaskSpec struct {
	// TaskType identifies the resource type that the taskQueue is responsible for triggering.
	TaskType metav1.GroupKind `json:"taskType,omitempty"`

	// Resource contains the raw YAML/JSON representation of the Kubernetes resource
	// to be triggered by the task queue.
	Resource runtime.RawExtension `json:"resource,omitempty"`
}

// PendingTaskStatus defines the observed state of PendingTask.
type PendingTaskStatus struct {
	// TaskQueueName is the name of the taskQueue that is responsible for triggering this task.
	TaskQueueName string `json:"taskQueueName,omitempty"`
}

// +kubebuilder:object:root=true

// PendingTaskList contains a list of PendingTask.
type PendingTaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PendingTask `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PendingTask{}, &PendingTaskList{})
}
