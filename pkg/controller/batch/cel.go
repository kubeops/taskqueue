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
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func evalCEL(obj *unstructured.Unstructured, expr string) (bool, error) {
	phase, err := extractStatusPhase(obj)
	if err != nil {
		return false, err
	}
	if phase == nil {
		return false, nil // no phase => rule doesn't match
	}
	if _, ok := phase.(string); !ok {
		return false, fmt.Errorf("status.phase is not a string: got %T", phase)
	}

	env, err := createCELEnv()
	if err != nil {
		return false, fmt.Errorf("failed to create CEL environment: %v", err)
	}

	program, err := compileExpression(env, expr)
	if err != nil {
		return false, err
	}

	return evaluateCEL(program, map[string]any{"self": obj.Object})
}

func createCELEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Declarations(
			decls.NewVar("self", decls.NewMapType(decls.String, decls.Dyn)),
		),
	)
}

func extractStatusPhase(obj *unstructured.Unstructured) (any, error) {
	if obj == nil || obj.Object == nil {
		return nil, fmt.Errorf("object is nil or empty")
	}

	status, found, err := unstructured.NestedMap(obj.Object, "status")
	if err != nil {
		return nil, fmt.Errorf("failed to access status field: %v", err)
	}
	if !found {
		return nil, nil // No status field
	}

	phase, ok := status["phase"]
	if !ok {
		return nil, nil // No phase field
	}
	return phase, nil
}

func compileExpression(env *cel.Env, expr string) (cel.Program, error) {
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to compile CEL expression %q: %v", expr, issues.Err())
	}

	program, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL program: %v", err)
	}
	return program, nil
}

func evaluateCEL(program cel.Program, input map[string]any) (bool, error) {
	out, _, err := program.Eval(input)
	if err != nil {
		return false, fmt.Errorf("failed to evaluate CEL expression: %v", err)
	}

	result, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("CEL expression did not return a boolean: got %T", out.Value())
	}
	return result, nil
}
