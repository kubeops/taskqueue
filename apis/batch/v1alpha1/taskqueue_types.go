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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type UnitTask struct {
	Kind     string `json:"kind,omitempty" protobuf:"bytes,2,opt,name=kind"`
	APIGroup string `json:"apiGroup,omitempty" protobuf:"bytes,1,opt,name=apiGroup"`
}

// TaskQueueSpec defines the desired configuration of a TaskQueue.
type TaskQueueSpec struct {
	// NumberOfConcurrentTasks specifies how many tasks can run concurrently.
	// Defaults to 20 if not set.
	// +kubebuilder:default=20
	// +optional
	NumberOfConcurrentTasks int `json:"numberOfConcurrentTasks,omitempty"`

	// Tasks represents the lists of tasks that this queue is responsible for processing.
	Tasks []UnitTask `json:"tasks,omitempty"`
}

// TaskQueueStatus represents the observed state of a TaskQueue.
type TaskQueueStatus struct {
	// LastTriggeredTasks contains references to the most recently triggered tasks.
	// +optional
	LastTriggeredTasks []v1.TypedObjectReference `json:"lastTriggeredTasks,omitempty"`
}

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
