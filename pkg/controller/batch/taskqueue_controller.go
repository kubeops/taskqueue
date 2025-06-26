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
	"encoding/json"
	"fmt"
	"sync"

	queueapi "kubeops.dev/taskqueue/apis/batch/v1alpha1"
	"kubeops.dev/taskqueue/pkg/queue"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	cu "kmodules.xyz/client-go/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type TaskQueueReconciler struct {
	client.Client
	once                   sync.Once
	DiscoveryClient        *discovery.DiscoveryClient
	DynamicInformerFactory dynamicinformer.DynamicSharedInformerFactory
	QueuePool              *queue.SharedQueuePool
	StartedWatchersByGVR   map[schema.GroupVersionResource]struct{}
}

func (r *TaskQueueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling TaskQueue", "name", req.Name, "namespace", req.Namespace)

	tq, err := r.getTaskQueue(ctx, req.Namespace, req.Name)
	if err != nil || tq == nil {
		return ctrl.Result{}, err
	}
	if !tq.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, logger, tq)
	}

	if err := r.ensureFinalizer(ctx, logger, tq); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to ensure finalizer: %w", err)
	}

	needsRequeue, err := r.syncTaskQueueStatus(ctx, tq, func(key string) bool { return true })
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to sync task statuses: %w", err)
	}

	if !needsRequeue {
		if err := r.processPendingTasks(ctx, logger, tq); err != nil {
			return ctrl.Result{}, fmt.Errorf("process pending: %w", err)
		}
	}

	if err := r.updateStatus(ctx, tq); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}

	return ctrl.Result{Requeue: needsRequeue}, nil
}

func (r *TaskQueueReconciler) syncTaskQueueStatus(ctx context.Context, tq *queueapi.TaskQueue, keyFilter func(key string) bool) (bool, error) {
	if tq.Status.TriggeredTasksStatus == nil {
		tq.Status.TriggeredTasksStatus = make(map[string]map[string]queueapi.TaskPhase)
	}
	var errs []error
	var shouldRequeue bool
	for typRef, statusMap := range tq.Status.TriggeredTasksStatus {
		gvk := parseGVKFromKey(typRef)
		if tq.Status.TriggeredTasksStatus[typRef] == nil {
			tq.Status.TriggeredTasksStatus[typRef] = make(map[string]queueapi.TaskPhase)
		}
		for statusKey := range statusMap {
			if !keyFilter(statusKey) {
				continue
			}
			ns, name, err := cache.SplitMetaNamespaceKey(statusKey)
			if err != nil {
				errs = append(errs, err)
			}
			obj, err := getObject(ctx, r.Client, schema.GroupVersionKind(gvk), types.NamespacedName{Namespace: ns, Name: name})
			if err != nil {
				if errors.IsNotFound(err) {
					r.deleteKeyFromTasksPhase(tq, typRef, statusKey)
					continue
				}
				errs = append(errs, err)
				continue
			}

			shouldKeep, phase, err := r.evaluateObjectPhase(obj, tq)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if !shouldKeep {
				shouldRequeue = true
				r.deleteKeyFromTasksPhase(tq, typRef, statusKey)
			} else if phase != "" {
				shouldRequeue = true
				r.updateTasksPhase(tq, typRef, statusKey, phase)
			}
		}
	}
	return shouldRequeue, utilerrors.NewAggregate(errs)
}

func (r *TaskQueueReconciler) evaluateObjectPhase(obj *unstructured.Unstructured, tq *queueapi.TaskQueue) (bool, queueapi.TaskPhase, error) {
	ruleSet := r.getRuleSetFromTaskQueue(obj, tq)

	if ok, err := evalCEL(obj, ruleSet.Success); err != nil || ok {
		return false, "", err
	}

	if ok, err := evalCEL(obj, ruleSet.Failed); err != nil || ok {
		return false, "", err
	}

	if ok, err := evalCEL(obj, ruleSet.InProgress); err != nil || ok { // If the object is in progress, we keep it in the status map
		return true, queueapi.TaskPhaseInProgress, err
	}

	return true, "", nil
}

func (r *TaskQueueReconciler) processPendingTasks(ctx context.Context, logger logr.Logger, tq *queueapi.TaskQueue) error {
	if !r.QueuePool.Exists(tq.Name) {
		logger.Info("TaskQueue not found in queue pool, skipping processing", "taskQueue", tq.Name)
		return nil
	}
	if err := r.syncWatchingResourcesOnce(ctx, logger, tq); err != nil {
		return fmt.Errorf("failed to sync watching resources: %w", err)
	}
	inProgress := r.getInProgressTaskCount(tq)
	for ; inProgress < tq.Spec.MaxConcurrentTasks && r.QueuePool.QueueLength(tq.Name) > 0; inProgress++ {
		pt, err := r.getPendingTask(ctx, tq)
		if err != nil {
			return client.IgnoreNotFound(err)
		}
		gvr, err := getGVR(r.DiscoveryClient, pt.Spec.TaskType)
		if err != nil {
			return fmt.Errorf("failed to get GVR: %w", err)
		}
		if err = r.createTasksObject(ctx, gvr.GroupVersion().String(), tq, pt); err != nil {
			return fmt.Errorf("failed to create pendingTasks object: %w", err)
		}
		r.startWatchingResource(ctx, gvr)
	}
	return nil
}

func (r *TaskQueueReconciler) getPendingTask(ctx context.Context, tq *queueapi.TaskQueue) (*queueapi.PendingTask, error) {
	key, shutting := r.QueuePool.Dequeue(tq.Name)
	if shutting {
		return nil, nil
	}

	pendingTask := &queueapi.PendingTask{}
	if err := r.Get(ctx, types.NamespacedName{Name: key}, pendingTask); err != nil {
		return nil, err
	}
	return pendingTask, nil
}

func (r *TaskQueueReconciler) createTasksObject(ctx context.Context, grpVersion string, tq *queueapi.TaskQueue, pt *queueapi.PendingTask) error {
	var obj unstructured.Unstructured
	if err := json.Unmarshal(pt.Spec.Resource.Raw, &obj.Object); err != nil {
		return fmt.Errorf("failed to unmarshal task resource: %w", err)
	}
	obj.SetKind(pt.Spec.TaskType.Kind)
	obj.SetAPIVersion(grpVersion)
	if _, err := cu.CreateOrPatch(ctx, r.Client, &obj,
		func(obj client.Object, createOp bool) client.Object {
			in := obj.(*unstructured.Unstructured)
			return in
		},
	); err != nil {
		return fmt.Errorf("failed to create or patch task: %w", err)
	}
	typeRef := getGVKFromObj(obj)
	if tq.Status.TriggeredTasksStatus[typeRef] == nil {
		tq.Status.TriggeredTasksStatus[typeRef] = make(map[string]queueapi.TaskPhase)
	}
	key, err := cache.MetaNamespaceKeyFunc(&obj)
	if err != nil {
		return fmt.Errorf("failed to get key for object: %w", err)
	}
	r.updateTasksPhase(tq, typeRef, key, queueapi.TaskPhasePending)
	if err := r.Delete(ctx, pt); err != nil {
		return fmt.Errorf("failed to delete task object: %w", err)
	}
	return nil
}
