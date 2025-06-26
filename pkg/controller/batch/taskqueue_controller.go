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
	"strings"
	"sync"

	queueapi "kubeops.dev/taskqueue/apis/batch/v1alpha1"
	"kubeops.dev/taskqueue/pkg/queue"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	cu "kmodules.xyz/client-go/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
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

func (r *TaskQueueReconciler) getTaskQueue(ctx context.Context, namespace, name string) (*queueapi.TaskQueue, error) {
	var err error
	tq := &queueapi.TaskQueue{}
	if err = r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, tq); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return tq, nil
}

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

func (r *TaskQueueReconciler) startWatchingResource(ctx context.Context, gvr schema.GroupVersionResource) {
	var shouldStart = true
	if r.QueuePool.ExecuteFunc(func() {
		if _, exist := r.StartedWatchersByGVR[gvr]; exist {
			shouldStart = false
		}
		r.StartedWatchersByGVR[gvr] = struct{}{}
	}); !shouldStart {
		return
	}

	_, _ = r.DynamicInformerFactory.ForResource(gvr).Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			if err := r.handleResourcesUpdate(ctx, new); err != nil {
				klog.Errorf("failed to handle resources update: %v", err)
			}
		},
		DeleteFunc: func(delObj interface{}) {
			if err := r.handleResourcesUpdate(ctx, delObj); err != nil {
				klog.Errorf("failed to handle resources update: %v", err)
			}
		},
	})

	r.DynamicInformerFactory.Start(make(chan struct{}))
}

func (r *TaskQueueReconciler) handleResourcesUpdate(ctx context.Context, obj interface{}) error {
	newObj := obj.(*unstructured.Unstructured)
	taskQueueName, err := r.findMatchingTaskQueue(ctx, newObj)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to find matching TaskQueue: %w", err)
	}
	tq, err := r.getTaskQueue(ctx, "", taskQueueName)
	if err != nil {
		return fmt.Errorf("failed to get TaskQueue: %w", err)
	}
	if tq == nil {
		return nil
	}
	if _, err := r.syncTaskQueueStatus(ctx, tq, func(key string) bool {
		return strings.HasSuffix(key, newObj.GetName())
	}); err != nil {
		return fmt.Errorf("failed to sync task status: %w", err)
	}

	if err := r.updateStatus(ctx, tq); err != nil {
		return fmt.Errorf("failed to update TaskQueue status: %w", err)
	}
	return err
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

func (r *TaskQueueReconciler) deleteKeyFromTasksPhase(tq *queueapi.TaskQueue, typeRefKey string, statusKey string) {
	r.QueuePool.ExecuteFunc(func() {
		delete(tq.Status.TriggeredTasksStatus[typeRefKey], statusKey)
	})
}

func (r *TaskQueueReconciler) updateTasksPhase(tq *queueapi.TaskQueue, gvk string, key string, phase queueapi.TaskPhase) {
	r.QueuePool.ExecuteFunc(func() {
		tq.Status.TriggeredTasksStatus[gvk][key] = phase
	})
}

func (r *TaskQueueReconciler) processPendingTasks(ctx context.Context, logger logr.Logger, tq *queueapi.TaskQueue) error {
	if !r.QueuePool.Exists(tq.Name) {
		logger.Info("TaskQueue not found in queue pool, skipping processing", "taskQueue", tq.Name)
		return nil
	}
	if err := r.syncWatchingResourceOnce(ctx, logger, tq); err != nil {
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

func (r *TaskQueueReconciler) syncWatchingResourceOnce(ctx context.Context, logger logr.Logger, tq *queueapi.TaskQueue) error {
	var errs []error
	r.once.Do(func() {
		logger.Info("Syncing watching resources for TaskQueue", "taskQueue", tq.Name)
		for typeRef := range tq.Status.TriggeredTasksStatus {
			gvk := parseGVKFromKey(typeRef)
			gvr, err := getGVR(r.DiscoveryClient, metav1.GroupKind{Group: gvk.Group, Kind: gvk.Kind})
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to get GVR: %w", err))
				return
			}
			r.startWatchingResource(ctx, gvr)
		}
	})
	return utilerrors.NewAggregate(errs)
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

func (r *TaskQueueReconciler) handlerForUpdatePendingTask(ctx context.Context, e event.TypedUpdateEvent[client.Object], w workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pt := e.ObjectNew.(*queueapi.PendingTask)
	if pt.Status.TaskQueueName != "" {
		log.FromContext(ctx).V(1).Info("PendingTask updated, enqueuing referenced TaskQueue", "pendingTaskName", pt.Name, "taskQueueName", pt.Status.TaskQueueName)
		w.Add(reconcile.Request{
			NamespacedName: types.NamespacedName{Name: pt.Status.TaskQueueName},
		})
	}
}

func (r *TaskQueueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&queueapi.TaskQueue{}).
		Watches(
			&queueapi.PendingTask{},
			&handler.Funcs{
				UpdateFunc: r.handlerForUpdatePendingTask,
			},
		).
		Complete(r)
}
