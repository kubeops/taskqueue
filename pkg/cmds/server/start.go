/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Free Trial License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Free-Trial-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"context"
	"fmt"
	"io"
	"net"

	opsapi "kubeops.dev/taskqueue/apis/ops/v1alpha1"
	"kubeops.dev/taskqueue/pkg/apiserver"

	"github.com/spf13/pflag"
	v "gomodules.xyz/x/version"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	utilversion "k8s.io/component-base/version"
	ou "kmodules.xyz/client-go/openapi"
	"kmodules.xyz/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const defaultEtcdPathPrefix = "/registry/k8s.appscode.com"

// PendingTaskOptions contains state for master/api server
type PendingTaskOptions struct {
	RecommendedOptions *genericoptions.RecommendedOptions
	ExtraOptions       *ExtraOptions

	StdOut io.Writer
	StdErr io.Writer
}

// NewPendingTaskOptions returns a new PendingTaskOptions
func NewPendingTaskOptions(out, errOut io.Writer) *PendingTaskOptions {
	o := &PendingTaskOptions{
		RecommendedOptions: genericoptions.NewRecommendedOptions(
			defaultEtcdPathPrefix,
			apiserver.Codecs.LegacyCodec(
				opsapi.SchemeGroupVersion,
			),
		),
		ExtraOptions: NewExtraOptions(),
		StdOut:       out,
		StdErr:       errOut,
	}
	o.RecommendedOptions.Etcd = nil
	o.RecommendedOptions.Admission = nil
	return o
}

func (o PendingTaskOptions) AddFlags(fs *pflag.FlagSet) {
	o.RecommendedOptions.AddFlags(fs)
}

// Validate validates PendingTaskOptions
func (o PendingTaskOptions) Validate(args []string) error {
	var errors []error
	errors = append(errors, o.RecommendedOptions.Validate()...)
	return utilerrors.NewAggregate(errors)
}

// Complete fills in fields required to have valid data
func (o *PendingTaskOptions) Complete() error {
	return nil
}

// Config returns config for the api server given PendingTaskOptions
func (o *PendingTaskOptions) Config() (*apiserver.Config, error) {
	// TODO have a "real" external address
	if err := o.RecommendedOptions.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{net.ParseIP("127.0.0.1")}); err != nil {
		return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
	}
	serverConfig := genericapiserver.NewRecommendedConfig(apiserver.Codecs)
	if err := o.RecommendedOptions.ApplyTo(serverConfig); err != nil {
		return nil, err
	}
	// Fixes https://github.com/Azure/AKS/issues/522
	clientcmd.Fix(serverConfig.ClientConfig)

	ignorePrefixes := []string{
		"/swaggerapi",

		fmt.Sprintf("/apis/%s/%s", opsapi.SchemeGroupVersion, opsapi.ResourcePendingTasks),
	}

	serverConfig.EffectiveVersion = utilversion.NewEffectiveVersion("v1.0.0")

	serverConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(
		ou.GetDefinitions(
			opsapi.GetOpenAPIDefinitions,
		),
		openapi.NewDefinitionNamer(apiserver.Scheme))

	serverConfig.OpenAPIConfig.Info.Title = "secrets-store-reader"
	serverConfig.OpenAPIConfig.Info.Version = v.Version.Version
	serverConfig.OpenAPIConfig.IgnorePrefixes = ignorePrefixes

	serverConfig.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(
		ou.GetDefinitions(
			opsapi.GetOpenAPIDefinitions,
		),
		openapi.NewDefinitionNamer(apiserver.Scheme))

	serverConfig.OpenAPIConfig.Info.Title = "secrets-store-reader"
	serverConfig.OpenAPIConfig.Info.Version = v.Version.Version
	serverConfig.OpenAPIV3Config.IgnorePrefixes = ignorePrefixes

	if err := o.ExtraOptions.ApplyTo(serverConfig.ClientConfig); err != nil {
		return nil, err
	}

	config := &apiserver.Config{
		GenericConfig: serverConfig,
		ExtraConfig: apiserver.ExtraConfig{
			ClientConfig: serverConfig.ClientConfig,
		},
	}
	return config, nil
}

// RunServer starts a new TaskQueue given PendingTaskOptions
func (o PendingTaskOptions) RunServer(ctx context.Context) error {
	config, err := o.Config()
	if err != nil {
		return err
	}
	server, err := config.Complete().New(ctx)
	if err != nil {
		return err
	}
	server.GenericAPIServer.AddPostStartHookOrDie("task-queue-informers", func(context genericapiserver.PostStartHookContext) error {
		config.GenericConfig.SharedInformerFactory.Start(ctx.Done())
		return nil
	})

	err = server.Manager.Add(manager.RunnableFunc(func(ctx context.Context) error {
		return server.GenericAPIServer.PrepareRun().RunWithContext(ctx)
	}))
	if err != nil {
		return err
	}

	setupLog := log.Log.WithName("setup")
	setupLog.Info("starting manager")
	return server.Manager.Start(ctx)
}
