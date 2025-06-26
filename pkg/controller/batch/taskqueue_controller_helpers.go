/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Community License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Community-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package batch

import (
	"context"
	"fmt"

	queueapi "kubeops.dev/taskqueue/apis/batch/v1alpha1"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	cu "kmodules.xyz/client-go/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Getter

func (r *TaskQueueReconciler) getTaskQueue(ctx context.Context, namespace, name string) (*queueapi.TaskQueue, error) {
	var err error
	tq := &queueapi.TaskQueue{}
	if err = r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, tq); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return tq, nil
}

func (r *TaskQueueReconciler) getRuleSetFromTaskQueue(obj *unstructured.Unstructured, tq *queueapi.TaskQueue) queueapi.ObjectPhaseRules {
	for _, task := range tq.Spec.Tasks {
		if task.Type.Kind == obj.GetKind() && task.Type.Group == obj.GroupVersionKind().Group {
			return task.Rules
		}
	}
	return queueapi.ObjectPhaseRules{}
}

func (r *TaskQueueReconciler) getInProgressTaskCount(tq *queueapi.TaskQueue) int {
	count := 0
	for _, statusMap := range tq.Status.TriggeredTasksStatus {
		for _, phase := range statusMap {
			if phase == queueapi.TaskPhaseInProgress ||
				phase == queueapi.TaskPhasePending {
				count++
			}
		}
	}
	return count
}

func (r *TaskQueueReconciler) findMatchingTaskQueue(ctx context.Context, obj *unstructured.Unstructured) (string, error) {
	var taskQueueList queueapi.TaskQueueList
	if err := r.List(ctx, &taskQueueList); err != nil {
		return "", fmt.Errorf("failed to list TaskQueues: %w", err)
	}
	for _, tq := range taskQueueList.Items {
		for _, task := range tq.Spec.Tasks {
			if task.Type.Kind == obj.GetKind() && task.Type.Group == obj.GroupVersionKind().Group {
				return tq.Name, nil
			}
		}
	}
	return "", nil
}

// Finalizer

func (r *TaskQueueReconciler) handleDeletion(ctx context.Context, logger logr.Logger, tq *queueapi.TaskQueue) (ctrl.Result, error) {
	logger.Info("Handling deletion")
	r.QueuePool.Remove(tq.Name)

	if controllerutil.ContainsFinalizer(tq, taskQueueFinalizer) {
		_, err := cu.CreateOrPatch(ctx, r.Client, tq, func(obj client.Object, createOp bool) client.Object {
			controllerutil.RemoveFinalizer(obj, taskQueueFinalizer)
			return obj
		})
		return ctrl.Result{}, err
	}
	logger.Info("TaskQueue deleted successfully", "name", tq.Name)
	return ctrl.Result{}, nil
}

func (r *TaskQueueReconciler) ensureFinalizer(ctx context.Context, logger logr.Logger, tq *queueapi.TaskQueue) error {
	if !controllerutil.ContainsFinalizer(tq, taskQueueFinalizer) {
		logger.Info("Adding finalizer for TaskQueue")
		_, err := cu.CreateOrPatch(ctx, r.Client, tq, func(obj client.Object, createOp bool) client.Object {
			controllerutil.AddFinalizer(obj, taskQueueFinalizer)
			return obj
		})
		return err
	}
	return nil
}

// Phase or status

func (r *TaskQueueReconciler) updateTasksPhase(tq *queueapi.TaskQueue, gvk string, key string, phase queueapi.TaskPhase) {
	r.QueuePool.ExecuteFunc(func() {
		tq.Status.TriggeredTasksStatus[gvk][key] = phase
	})
}

func (r *TaskQueueReconciler) deleteKeyFromTasksPhase(tq *queueapi.TaskQueue, typeRefKey string, statusKey string) {
	r.QueuePool.ExecuteFunc(func() {
		delete(tq.Status.TriggeredTasksStatus[typeRefKey], statusKey)
	})
}

func (r *TaskQueueReconciler) updateStatus(ctx context.Context, tq *queueapi.TaskQueue) error {
	_, err := cu.PatchStatus(
		ctx,
		r.Client,
		tq,
		func(obj client.Object) client.Object {
			in := obj.(*queueapi.TaskQueue)
			in.Status = tq.Status
			return in
		},
	)
	return client.IgnoreNotFound(err)
}
