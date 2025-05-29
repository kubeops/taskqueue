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

	batchv1alpha1 "kubeops.dev/taskqueue/apis/batch/v1alpha1"
	"kubeops.dev/taskqueue/pkg/queue"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	kmc "kmodules.xyz/client-go/client"
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
	Scheme                 *runtime.Scheme
	DiscoveryClient        *discovery.DiscoveryClient
	DynamicInformerFactory dynamicinformer.DynamicSharedInformerFactory
	QueuePool              *queue.SharedQueuePool
	MapOfWatchResources    map[schema.GroupVersionResource]struct{}
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

	if err := r.syncTaskQueueStatus(ctx, tq, func(key string) bool {
		return true
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to sync task statuses: %w", err)
	}

	if err := r.processPendingTasks(ctx, logger, tq); err != nil {
		return ctrl.Result{}, fmt.Errorf("process pending: %w", err)
	}

	if err := r.updateStatus(ctx, tq); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *TaskQueueReconciler) getTaskQueue(ctx context.Context, namespace, name string) (*batchv1alpha1.TaskQueue, error) {
	var err error
	tq := &batchv1alpha1.TaskQueue{}
	if err = r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, tq); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return tq, nil
}

func (r *TaskQueueReconciler) handleDeletion(ctx context.Context, logger logr.Logger, tq *batchv1alpha1.TaskQueue) (ctrl.Result, error) {
	logger.Info("Handling deletion")
	r.QueuePool.Remove(tq.Name)

	if controllerutil.ContainsFinalizer(tq, taskQueueFinalizer) {
		_, err := kmc.CreateOrPatch(ctx, r.Client, tq, func(obj client.Object, createOp bool) client.Object {
			controllerutil.RemoveFinalizer(obj, taskQueueFinalizer)
			return obj
		})
		return ctrl.Result{}, err
	}
	logger.Info("TaskQueue deleted successfully", "name", tq.Name)
	return ctrl.Result{}, nil
}

func (r *TaskQueueReconciler) startWatchingResource(ctx context.Context, gvr schema.GroupVersionResource) {
	var shouldStart bool = true
	if r.QueuePool.WithMutexLock(func() {
		if _, exist := r.MapOfWatchResources[gvr]; exist {
			shouldStart = false
		}
		r.MapOfWatchResources[gvr] = struct{}{}
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
	if err := r.syncTaskQueueStatus(ctx, tq, func(key string) bool {
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
	var taskQueueList batchv1alpha1.TaskQueueList
	if err := r.List(ctx, &taskQueueList); err != nil {
		return "", fmt.Errorf("failed to list TaskQueues: %w", err)
	}
	for _, tq := range taskQueueList.Items {
		for _, task := range tq.Spec.Tasks {
			if task.Type.Kind == obj.GetKind() && task.Type.APIGroup == obj.GroupVersionKind().Group {
				return tq.Name, nil
			}
		}
	}
	return "", nil
}

func (r *TaskQueueReconciler) ensureFinalizer(ctx context.Context, logger logr.Logger, tq *batchv1alpha1.TaskQueue) error {
	if !controllerutil.ContainsFinalizer(tq, taskQueueFinalizer) {
		logger.Info("Adding finalizer for TaskQueue")
		_, err := kmc.CreateOrPatch(ctx, r.Client, tq, func(obj client.Object, createOp bool) client.Object {
			controllerutil.AddFinalizer(obj, taskQueueFinalizer)
			return obj
		})
		return err
	}
	return nil
}

func (r *TaskQueueReconciler) syncTaskQueueStatus(ctx context.Context, tq *batchv1alpha1.TaskQueue, keyFilter func(key string) bool) error {
	if tq.Status.TriggeredTasksPhase == nil {
		tq.Status.TriggeredTasksPhase = make(map[string]batchv1alpha1.TaskPhase)
	}
	var errs []error
	for key := range tq.Status.TriggeredTasksPhase {
		if !keyFilter(key) {
			continue
		}
		gvk, ns, name := parseStatusKey(key)
		obj, err := getObject(ctx, r.Client, gvk, types.NamespacedName{Namespace: ns, Name: name})
		if err != nil {
			if errors.IsNotFound(err) {
				r.deleteKeyFromTasksPhase(tq, key)
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
			r.deleteKeyFromTasksPhase(tq, key)
		} else if phase != "" {
			r.updateTasksPhase(tq, key, phase)
		}
	}

	return utilerrors.NewAggregate(errs)
}

func (r *TaskQueueReconciler) evaluateObjectPhase(obj *unstructured.Unstructured, tq *batchv1alpha1.TaskQueue) (bool, batchv1alpha1.TaskPhase, error) {
	ruleSet := r.getRuleSetFromTaskQueue(obj, tq)

	if ok, err := evalCEL(obj, ruleSet.Success); err != nil || ok {
		return false, "", err
	}

	if ok, err := evalCEL(obj, ruleSet.Failed); err != nil || ok {
		return false, "", err
	}

	if ok, err := evalCEL(obj, ruleSet.InProgress); err != nil || ok { // If the object is in progress, we keep it in the status map
		return true, batchv1alpha1.TaskPhaseInProgress, err
	}

	return true, "", nil
}

func (r *TaskQueueReconciler) deleteKeyFromTasksPhase(tq *batchv1alpha1.TaskQueue, key string) {
	r.QueuePool.WithMutexLock(func() {
		delete(tq.Status.TriggeredTasksPhase, key)
	})
}

func (r *TaskQueueReconciler) updateTasksPhase(tq *batchv1alpha1.TaskQueue, key string, phase batchv1alpha1.TaskPhase) {
	r.QueuePool.WithMutexLock(func() {
		tq.Status.TriggeredTasksPhase[key] = phase
	})
}

func (r *TaskQueueReconciler) processPendingTasks(ctx context.Context, logger logr.Logger, tq *batchv1alpha1.TaskQueue) error {
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
		if err = r.createTasksObject(ctx, tq, pt); err != nil {
			return fmt.Errorf("failed to create pendingTasks object: %w", err)
		}
		gvr, err := getGVR(r.DiscoveryClient, pt.Spec.TaskType)
		if err != nil {
			return fmt.Errorf("failed to get GVR: %w", err)
		}
		r.startWatchingResource(ctx, gvr)
	}
	return nil
}

func (r *TaskQueueReconciler) syncWatchingResourceOnce(ctx context.Context, logger logr.Logger, tq *batchv1alpha1.TaskQueue) error {
	var errs []error
	r.once.Do(func() {
		logger.Info("Syncing watching resources for TaskQueue", "taskQueue", tq.Name)
		for key := range tq.Status.TriggeredTasksPhase {
			gvk, _, _ := parseStatusKey(key)
			gvr, err := getGVR(r.DiscoveryClient, batchv1alpha1.TypedResourceReference{
				APIGroup: gvk.Group,
				Kind:     gvk.Kind,
			})
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to get GVR: %w", err))
				return
			}
			r.startWatchingResource(ctx, gvr)
		}
	})
	return utilerrors.NewAggregate(errs)
}

func (r *TaskQueueReconciler) createTasksObject(ctx context.Context, tq *batchv1alpha1.TaskQueue, pt *batchv1alpha1.PendingTask) error {
	var obj unstructured.Unstructured
	if err := json.Unmarshal(pt.Spec.Resource.Raw, &obj.Object); err != nil {
		return fmt.Errorf("failed to unmarshal task resource: %w", err)
	}
	if _, err := kmc.CreateOrPatch(ctx, r.Client, &obj,
		func(obj client.Object, createOp bool) client.Object {
			in := obj.(*unstructured.Unstructured)
			return in
		},
	); err != nil {
		return fmt.Errorf("failed to create or patch task: %w", err)
	}
	r.updateTasksPhase(tq, getKeyFromObj(&obj), batchv1alpha1.TaskPhaseInPending)
	if err := r.Delete(ctx, pt); err != nil {
		return fmt.Errorf("failed to delete task object: %w", err)
	}
	return nil
}

func (r *TaskQueueReconciler) updateStatus(ctx context.Context, tq *batchv1alpha1.TaskQueue) error {
	_, err := kmc.PatchStatus(
		ctx,
		r.Client,
		tq,
		func(obj client.Object) client.Object {
			in := obj.(*batchv1alpha1.TaskQueue)
			in.Status = tq.Status
			return in
		},
	)
	return client.IgnoreNotFound(err)
}

func (r *TaskQueueReconciler) getRuleSetFromTaskQueue(obj *unstructured.Unstructured, tq *batchv1alpha1.TaskQueue) batchv1alpha1.ObjectPhaseRules {
	for _, task := range tq.Spec.Tasks {
		if task.Type.Kind == obj.GetKind() && task.Type.APIGroup == obj.GroupVersionKind().Group {
			return task.Rules
		}
	}
	return batchv1alpha1.ObjectPhaseRules{}
}

func (r *TaskQueueReconciler) getInProgressTaskCount(tq *batchv1alpha1.TaskQueue) int {
	count := 0
	for _, phase := range tq.Status.TriggeredTasksPhase {
		if phase == batchv1alpha1.TaskPhaseInProgress || phase == batchv1alpha1.TaskPhaseInPending {
			count++
		}
	}
	return count
}

func (r *TaskQueueReconciler) getPendingTask(ctx context.Context, tq *batchv1alpha1.TaskQueue) (*batchv1alpha1.PendingTask, error) {
	key, shutting := r.QueuePool.Dequeue(tq.Name)
	if shutting {
		return nil, nil
	}

	pendingTask := &batchv1alpha1.PendingTask{}
	if err := r.Get(ctx, types.NamespacedName{Name: key}, pendingTask); err != nil {
		return nil, err
	}
	return pendingTask, nil
}

func (r *TaskQueueReconciler) handlerForUpdatePendingTask(ctx context.Context, e event.TypedUpdateEvent[client.Object], w workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pt := e.ObjectNew.(*batchv1alpha1.PendingTask)
	if pt.Status.TaskQueueName != "" {
		log.FromContext(ctx).V(1).Info("PendingTask updated, enqueuing referenced TaskQueue", "pendingTaskName", pt.Name, "taskQueueName", pt.Status.TaskQueueName)
		w.Add(reconcile.Request{
			NamespacedName: types.NamespacedName{Name: pt.Status.TaskQueueName},
		})
	}
}

func (r *TaskQueueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1alpha1.TaskQueue{}).
		Watches(
			&batchv1alpha1.PendingTask{},
			&handler.Funcs{
				UpdateFunc: r.handlerForUpdatePendingTask,
			},
		).
		Complete(r)
}
