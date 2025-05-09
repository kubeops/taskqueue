package fuzzer

import (
	fuzz "github.com/google/gofuzz"
	"go.virtual-secrets.dev/taskqueue/apis/ops/v1alpha1"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
)

// Funcs returns the fuzzer functions for this api group.
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		// v1alpha1
		func(s *v1alpha1.PendingTask, c fuzz.Continue) {
			c.FuzzNoCustom(s) // fuzz self without calling this function again
		},
	}
}
