package install

import (
	"testing"

	identityfuzzer "go.virtual-secrets.dev/taskqueue/apis/ops/fuzzer"
	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
)

func TestRoundTripTypes(t *testing.T) {
	roundtrip.RoundTripTestForAPIGroup(t, Install, identityfuzzer.Funcs)
	// TODO: enable protobuf generation for the sample-apiserver
	// roundtrip.RoundTripProtobufTestForAPIGroup(t, Install, identityfuzzer.Funcs)
}
