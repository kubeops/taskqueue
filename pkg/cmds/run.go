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

package cmds

import (
	"crypto/tls"
	"flag"
	"os"
	"path/filepath"

	// +kubebuilder:scaffold:imports

	batchv1alpha1 "kubeops.dev/taskqueue/apis/batch/v1alpha1"
	batchcontroller "kubeops.dev/taskqueue/pkg/controller/batch"
	queue "kubeops.dev/taskqueue/pkg/queue"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(batchv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func NewCmdRun() *cobra.Command {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	opts := zap.Options{
		Development: true,
	}

	cmd := &cobra.Command{
		Use:               "run",
		Short:             "Launch the TaskQueue controller manager",
		DisableAutoGenTag: true,
		Run: func(cmd *cobra.Command, args []string) {
			opts.BindFlags(flag.CommandLine)
			ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

			// if the enable-http2 flag is false (the default), http/2 should be disabled
			// due to its vulnerabilities. More specifically, disabling http/2 will
			// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
			// Rapid Reset CVEs. For more information see:
			// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
			// - https://github.com/advisories/GHSA-4374-p667-p6c8
			disableHTTP2 := func(c *tls.Config) {
				setupLog.Info("disabling http/2")
				c.NextProtos = []string{"http/1.1"}
			}

			if !enableHTTP2 {
				tlsOpts = append(tlsOpts, disableHTTP2)
			}

			// Create watchers for metrics certificates
			var metricsCertWatcher *certwatcher.CertWatcher

			// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
			// More info:
			// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.4/pkg/metrics/server
			// - https://book.kubebuilder.io/reference/metrics.html
			metricsServerOptions := metricsserver.Options{
				BindAddress:   metricsAddr,
				SecureServing: secureMetrics,
				TLSOpts:       tlsOpts,
			}

			if secureMetrics {
				// FilterProvider is used to protect the metrics endpoint with authn/authz.
				// These configurations ensure that only authorized users and service accounts
				// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
				// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.4/pkg/metrics/filters#WithAuthenticationAndAuthorization
				metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
			}

			// If the certificate is not specified, controller-runtime will automatically
			// generate self-signed certificates for the metrics server. While convenient for development and testing,
			// this setup is not recommended for production.
			//
			// TODO(user): If you enable certManager, uncomment the following lines:
			// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
			// managed by cert-manager for the metrics server.
			// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
			if len(metricsCertPath) > 0 {
				setupLog.Info("Initializing metrics certificate watcher using provided certificates",
					"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

				var err error
				metricsCertWatcher, err = certwatcher.New(
					filepath.Join(metricsCertPath, metricsCertName),
					filepath.Join(metricsCertPath, metricsCertKey),
				)
				if err != nil {
					setupLog.Error(err, "to initialize metrics certificate watcher", "error", err)
					os.Exit(1)
				}

				metricsServerOptions.TLSOpts = append(metricsServerOptions.TLSOpts, func(config *tls.Config) {
					config.GetCertificate = metricsCertWatcher.GetCertificate
				})
			}

			mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
				Scheme:                 scheme,
				Metrics:                metricsServerOptions,
				HealthProbeBindAddress: probeAddr,
				LeaderElection:         enableLeaderElection,
				LeaderElectionID:       "3afd8152.k8s.appscode.com",
				// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
				// when the Manager ends. This requires the binary to immediately end when the
				// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
				// speeds up voluntary leader transitions as the new leader don't have to wait
				// LeaseDuration time first.
				//
				// In the default scaffold provided, the program ends immediately after
				// the manager stops, so would be fine to enable this option. However,
				// if you are doing or is intended to do any operation such as perform cleanups
				// after the manager stops then its usage might be unsafe.
				// LeaderElectionReleaseOnCancel: true,
			})
			if err != nil {
				setupLog.Error(err, "unable to start manager")
				os.Exit(1)
			}
			mapOfWatchResources := make(map[schema.GroupVersionResource]struct{})
			cfg := mgr.GetConfig()
			cfg.QPS = 50000
			cfg.Burst = 50000
			dynClient, err := dynamic.NewForConfig(cfg)
			if err != nil {
				setupLog.Error(err, "unable to initialize dynamic client")
				os.Exit(1)
			}
			discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg)
			if err != nil {
				setupLog.Error(err, "unable to initialize discovery client")
				os.Exit(1)
			}

			uncachedClient, err := client.New(mgr.GetConfig(), client.Options{
				Scheme: mgr.GetScheme(),
				Mapper: mgr.GetRESTMapper(),
				Cache:  nil,
			})
			if err != nil {
				setupLog.Error(err, "unable to create uncached client")
				os.Exit(1)
			}

			newQueuePool := queue.NewSharedQueuePool()
			if err = (&batchcontroller.TaskQueueReconciler{
				Client:                 uncachedClient,
				Scheme:                 mgr.GetScheme(),
				MapOfWatchResources:    mapOfWatchResources,
				QueuePool:              newQueuePool,
				DiscoveryClient:        discoveryClient,
				DynamicInformerFactory: dynamicinformer.NewDynamicSharedInformerFactory(dynClient, 0),
			}).SetupWithManager(mgr); err != nil {
				setupLog.Error(err, "unable to create controller", "controller", "TaskQueue")
				os.Exit(1)
			}

			if err = (&batchcontroller.PendingTaskReconciler{
				Client:    mgr.GetClient(),
				Scheme:    mgr.GetScheme(),
				QueuePool: newQueuePool,
			}).SetupWithManager(mgr); err != nil {
				setupLog.Error(err, "unable to create controller", "controller", "pendingTask")
				os.Exit(1)
			}
			// +kubebuilder:scaffold:builder

			if metricsCertWatcher != nil {
				setupLog.Info("Adding metrics certificate watcher to manager")
				if err := mgr.Add(metricsCertWatcher); err != nil {
					setupLog.Error(err, "unable to add metrics certificate watcher to manager")
					os.Exit(1)
				}
			}

			if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
				setupLog.Error(err, "unable to set up health check")
				os.Exit(1)
			}
			if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
				setupLog.Error(err, "unable to set up ready check")
				os.Exit(1)
			}

			setupLog.Info("starting manager")
			if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
				setupLog.Error(err, "problem running manager")
				os.Exit(1)
			}
		},
	}

	cmd.Flags().StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	cmd.Flags().StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	cmd.Flags().BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	cmd.Flags().BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	cmd.Flags().StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	cmd.Flags().StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	cmd.Flags().StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	cmd.Flags().BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")

	return cmd
}
