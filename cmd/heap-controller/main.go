package main

import (
	"flag"
	"os"
	"path/filepath"
	"time"

	"github.com/bakins/test-controller/pkg/controller"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	serializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var kubeconfig *string
	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}
	config.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: scheme.Codecs}

	client, err := rest.UnversionedRESTClientFor(config)
	if err != nil {
		panic(err.Error())
	}

	cc := controller.Config{
		Resource: controller.Resource{
			APIVersion: "akins.org/v1alpha1",
			Kind:       "Heap",
			Resource:   "heaps",
		},
		SyncInterval: time.Minute,
		ChildResources: []controller.Resource{
			{
				APIVersion: "v1",
				Kind:       "Pod",
				Resource:   "Pods",
			},
		},
	}
	c, _ := controller.New(client, cc, &reconciler{})

	c.Run(make(chan struct{}))

}

type reconciler struct {
}

func (r *reconciler) Reconcile(u *unstructured.Unstructured) ([]*unstructured.Unstructured, error) {
	out := unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]interface{}{
				"name": "nginx-1234",
			},
			"spec": map[string]interface{}{
				"containers": []map[string]interface{}{
					{
						"name":  "nginx",
						"image": "nginx:1.7.9",
					},
				},
			},
		},
	}
	return []*unstructured.Unstructured{&out}, nil
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}
