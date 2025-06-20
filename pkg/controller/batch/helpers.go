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
	"strings"

	queueapi "kubeops.dev/taskqueue/apis/batch/v1alpha1"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	runtime_client "sigs.k8s.io/controller-runtime/pkg/client"
)

func evalCEL(obj *unstructured.Unstructured, expr string) (bool, error) {
	if obj == nil || obj.Object == nil {
		return false, fmt.Errorf("object is nil or empty")
	}
	status, found, err := unstructured.NestedMap(obj.Object, "status")
	if err != nil {
		return false, fmt.Errorf("failed to access status field: %v", err)
	}
	if !found {
		return false, nil
	}
	phase, ok := status["phase"]
	if !ok {
		return false, nil
	}
	if _, ok := phase.(string); !ok {
		return false, fmt.Errorf("status.phase is not a string: got %T", phase)
	}

	env, err := cel.NewEnv(cel.Declarations(decls.NewVar("self", decls.NewMapType(decls.String, decls.Dyn))))
	if err != nil {
		return false, fmt.Errorf("failed to create CEL environment: %v", err)
	}
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return false, fmt.Errorf("failed to compile CEL expression %q: %v", expr, issues.Err())
	}
	program, err := env.Program(ast)
	if err != nil {
		return false, fmt.Errorf("failed to create CEL program: %v", err)
	}
	out, _, err := program.Eval(map[string]any{
		"self": obj.Object,
	})
	if err != nil {
		return false, fmt.Errorf("failed to evaluate CEL expression: %v", err)
	}

	result, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("CEL expression %q did not return a boolean: got %T", expr, out.Value())
	}

	return result, nil
}

func getKeyFromObj(u unstructured.Unstructured) string {
	ns := u.GetNamespace()
	return fmt.Sprintf("%s/%s", ns, u.GetName())
}

func getGVKFromObj(u unstructured.Unstructured) string {
	group := u.GroupVersionKind().Group
	version := u.GroupVersionKind().Version
	kind := u.GroupVersionKind().Kind
	if version == "" {
		version = "_"
	}
	return fmt.Sprintf("%s/%s/%s", group, version, kind)
}

func parseTypeRefKey(key string) schema.GroupVersionKind {
	parts := strings.Split(key, keySeparator)
	if parts[1] == "_" {
		parts[1] = ""
	}
	return schema.GroupVersionKind{
		Group:   parts[0],
		Version: parts[1],
		Kind:    parts[2],
	}
}

func parseStatusKey(key string) types.NamespacedName {
	parts := strings.Split(key, keySeparator)
	if parts[0] == "_" {
		parts[0] = ""
	}
	return types.NamespacedName{
		Namespace: parts[0],
		Name:      parts[1],
	}
}

func getObject(ctx context.Context, client runtime_client.Client, gvk schema.GroupVersionKind,
	objRef types.NamespacedName,
) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	if err := client.Get(ctx, objRef, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func getPreferredResourceVersion(discoveryClient *discovery.DiscoveryClient, gk schema.GroupKind) (string, string, error) {
	groups, err := discoveryClient.ServerGroups()
	if err != nil {
		return "", "", fmt.Errorf("list API groups: %w", err)
	}
	for _, group := range groups.Groups {
		if group.Name != gk.Group {
			continue
		}
		for _, ver := range group.Versions {
			resList, err := discoveryClient.ServerResourcesForGroupVersion(ver.GroupVersion)
			if err != nil {
				continue
			}
			for _, r := range resList.APIResources {
				if r.Kind == gk.Kind {
					return r.Name, ver.Version, nil
				}
			}
		}
	}
	return "", "", fmt.Errorf("resource %s/%s not found in discovery", gk.Group, gk.Kind)
}

func getGVR(discoveryClient *discovery.DiscoveryClient, typeRef queueapi.TypedResourceReference) (schema.GroupVersionResource, error) {
	apiResource, version, err := getPreferredResourceVersion(discoveryClient, schema.GroupKind{
		Group: typeRef.APIGroup,
		Kind:  typeRef.Kind,
	})
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("failed to get preferred version for %s/%s: %w", typeRef.APIGroup, typeRef.Kind, err)
	}
	return schema.GroupVersionResource{Group: typeRef.APIGroup, Version: version, Resource: apiResource}, nil
}
