package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"strings"
	"time"

	"github.com/go-logr/logr"

	_ "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	_ "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	batchv1alpha1 "kubeops.dev/taskqueue/api/batch/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const taskQueueFinalizer = "batch.kubeops.dev/finalizer"

type TaskQueueReconciler struct {
	client.Client
	taskQueue *batchv1alpha1.TaskQueue

	MapOfQueues map[string]*workqueue.Typed[string]
	Scheme      *runtime.Scheme
}

func (r *TaskQueueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("taskqueue", req.Name)

	r.taskQueue = &batchv1alpha1.TaskQueue{}
	if err := r.Get(ctx, types.NamespacedName{Name: req.Name}, r.taskQueue); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("TaskQueue resource not found. Ignoring since object must be deleted.")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Unable to fetch TaskQueue")
		return ctrl.Result{}, err
	}

	if !r.taskQueue.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, logger)
	}

	if !controllerutil.ContainsFinalizer(r.taskQueue, taskQueueFinalizer) {
		logger.Info("Adding finalizer for TaskQueue")
		controllerutil.AddFinalizer(r.taskQueue, taskQueueFinalizer)
		if err := r.Update(ctx, r.taskQueue); err != nil {
			logger.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if r.taskQueue.Status.TriggeredTasksPhase == nil {
		r.taskQueue.Status.TriggeredTasksPhase = make(map[string]batchv1alpha1.TaskPhase)
	}

	for key := range r.taskQueue.Status.TriggeredTasksPhase {
		gvk, ns, name := parseKey(key)
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, obj); err != nil {
			continue
		}

		ruleSet := r.getRuleSetFromTaskQueue(gvk)
		if ok, _ := evaluateCEL(obj, ruleSet.Success); ok {
			r.taskQueue.Status.TriggeredTasksPhase[key] = batchv1alpha1.TaskPhaseSuccessful
			continue
		}
		if ok, _ := evaluateCEL(obj, ruleSet.InProgress); ok {
			r.taskQueue.Status.TriggeredTasksPhase[key] = batchv1alpha1.TaskPhaseInProgress
			continue
		}
		if ok, _ := evaluateCEL(obj, ruleSet.Failed); ok {
			r.taskQueue.Status.TriggeredTasksPhase[key] = batchv1alpha1.TaskPhaseFailed
			continue
		}
	}

	inProgressTasksCount := r.getInProgressTaskCount()
	var errs []error
	for ; inProgressTasksCount < r.taskQueue.Spec.MaxConcurrentTasks; inProgressTasksCount += 1 {
		key, _ := r.MapOfQueues[r.taskQueue.Name].Get()
		defer r.MapOfQueues[r.taskQueue.Name].Done(key)
		pt, err := r.getPendingTaskFromKey(key)
		if err != nil {
			if errors.IsNotFound(err) {
				if err := r.Delete(ctx, pt); err != nil {
					errs = append(errs, err)
				}
			} else {
				errs = append(errs, err)
			}
			continue
		}

		defer func() {
			if err := r.Delete(ctx, pt); err != nil {
				errs = append(errs, err)
			}
		}()

		var obj unstructured.Unstructured
		if err := json.Unmarshal(pt.Spec.Resource.Raw, &obj.Object); err != nil {
			errs = append(errs, err)
			continue
		}

		if err := r.Create(ctx, &obj); err != nil {
			errs = append(errs, err)
		}
		r.taskQueue.Status.TriggeredTasksPhase[getKeyFromTaskObject(obj)] = batchv1alpha1.TaskPhaseInPending
	}
	if err := r.Update(ctx, r.taskQueue); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return ctrl.Result{}, fmt.Errorf("failed to reconcile TaskQueue: %v", errs)
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *TaskQueueReconciler) getRuleSetFromTaskQueue(gvk schema.GroupVersionKind) batchv1alpha1.ObjectPhaseRules {
	var rules batchv1alpha1.ObjectPhaseRules
	for _, task := range r.taskQueue.Spec.Tasks {
		if task.Type.Kind == gvk.Kind && task.Type.APIGroup == gvk.Group {
			rules = task.Rules
			break
		}
	}
	return rules
}

func getKeyFromTaskObject(obj unstructured.Unstructured) string {
	gvk := obj.GroupVersionKind()
	return fmt.Sprintf("%s/%s/%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind, obj.GetNamespace(), obj.GetName())
}

func parseKey(key string) (schema.GroupVersionKind, string, string) {
	// Format: group/version/kind/namespace/name
	parts := strings.Split(key, "/")
	return schema.GroupVersionKind{
		Group:   parts[0],
		Version: parts[1],
		Kind:    parts[2],
	}, parts[3], parts[4]
}

func evaluateCEL(obj *unstructured.Unstructured, rule string) (bool, error) {
	status := obj.Object["status"]
	if status == nil {
		return false, nil
	}

	// Prepare CEL environment
	env, err := cel.NewEnv(
		cel.Declarations(decls.NewVar("self", decls.NewMapType(decls.String, decls.Dyn))),
	)
	if err != nil {
		return false, err
	}

	ast, issues := env.Compile(rule)
	if issues != nil && issues.Err() != nil {
		return false, issues.Err()
	}

	// Create a CEL program
	program, err := env.Program(ast)
	if err != nil {
		return false, err
	}

	// Evaluate the CEL program
	out, _, err := program.Eval(map[string]any{
		"self": status,
	})
	if err != nil {
		return false, err
	}

	result, ok := out.Value().(bool)
	if !ok {
		return false, nil
	}
	return result, nil
}

func (r *TaskQueueReconciler) getPendingTaskFromKey(taskName string) (*batchv1alpha1.PendingTask, error) {
	key := types.NamespacedName{
		Name: taskName,
	}
	pendingTask := &batchv1alpha1.PendingTask{}
	if err := r.Get(context.TODO(), key, pendingTask); err != nil {
		return &batchv1alpha1.PendingTask{}, err
	}
	return pendingTask, nil
}

func (r *TaskQueueReconciler) getInProgressTaskCount() int {
	count := 0
	for key := range r.taskQueue.Status.TriggeredTasksPhase {
		if r.taskQueue.Status.TriggeredTasksPhase[key] == batchv1alpha1.TaskPhaseInProgress {
			count++
		}
	}
	return count
}

func (r *TaskQueueReconciler) reconcileDelete(ctx context.Context, logger logr.Logger) (ctrl.Result, error) {
	logger.Info("Reconciling deletion for TaskQueue")
	if controllerutil.ContainsFinalizer(r.taskQueue, taskQueueFinalizer) {
		logger.Info("Removing finalizer for TaskQueue")
		controllerutil.RemoveFinalizer(r.taskQueue, taskQueueFinalizer)
		if err := r.Update(ctx, r.taskQueue); err != nil {
			logger.Error(err, "Failed to remove finalizer")
			return ctrl.Result{}, err
		}
	}
	logger.Info("TaskQueue deleted successfully")
	return ctrl.Result{}, nil
}

func (r *TaskQueueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1alpha1.TaskQueue{}).
		Named("batch-taskqueue").
		Complete(r)
}
