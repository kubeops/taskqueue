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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func getGVKFromObj(u unstructured.Unstructured) string {
	group := u.GroupVersionKind().Group
	version := u.GroupVersionKind().Version
	kind := u.GroupVersionKind().Kind
	if version == "" {
		version = "_"
	}
	return fmt.Sprintf("%s/%s/%s", group, version, kind)
}

func parseGVKFromKey(key string) metav1.GroupVersionKind {
	parts := strings.Split(key, keySeparator)
	if parts[1] == "_" {
		parts[1] = ""
	}
	return metav1.GroupVersionKind{
		Group:   parts[0],
		Version: parts[1],
		Kind:    parts[2],
	}
}

func getObject(ctx context.Context, kbClient client.Client, gvk schema.GroupVersionKind,
	objRef types.NamespacedName,
) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	if err := kbClient.Get(ctx, objRef, obj); err != nil {
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

func getGVR(discoveryClient *discovery.DiscoveryClient, typeRef metav1.GroupKind) (schema.GroupVersionResource, error) {
	apiResource, version, err := getPreferredResourceVersion(discoveryClient, schema.GroupKind{
		Group: typeRef.Group,
		Kind:  typeRef.Kind,
	})
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("failed to get preferred version for %s/%s: %w", typeRef.Group, typeRef.Kind, err)
	}
	return schema.GroupVersionResource{Group: typeRef.Group, Version: version, Resource: apiResource}, nil
}
