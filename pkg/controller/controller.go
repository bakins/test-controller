package controller

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
)

const (
	controllerLabel = "controller-uid"
)

// Reconciler ...
type Reconciler interface {
	Reconcile(*unstructured.Unstructured) ([]*unstructured.Unstructured, error)
}

// Controller ...
type Controller struct {
	client     *rest.RESTClient
	config     Config
	reconciler Reconciler
	events     chan *unstructured.Unstructured
}

// Config ...
type Config struct {
	Resource       Resource
	SyncInterval   time.Duration
	ChildResources []Resource
}

// Resource ...
type Resource struct {
	APIVersion string
	Kind       string
	Resource   string
}

// New ...
func New(client *rest.RESTClient, config Config, reconciler Reconciler) (*Controller, error) {
	return &Controller{
		client:     client,
		config:     config,
		reconciler: reconciler,
		events:     make(chan *unstructured.Unstructured),
	}, nil
}

func boolPointer(v bool) *bool {
	return &v
}

func setOwnerReference(parent *unstructured.Unstructured, child *unstructured.Unstructured) {
	controllerRef := metav1.OwnerReference{
		APIVersion:         parent.GetAPIVersion(),
		Kind:               parent.GetKind(),
		Name:               parent.GetName(),
		UID:                parent.GetUID(),
		Controller:         boolPointer(true),
		BlockOwnerDeletion: boolPointer(true),
	}

	child.SetOwnerReferences([]metav1.OwnerReference{controllerRef})
}

func setLabel(parent *unstructured.Unstructured, child *unstructured.Unstructured) {
	u := string(parent.GetUID())
	labels := child.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[controllerLabel] = u
	child.SetLabels(labels)
}

func (c *Controller) resourceForObject(u *unstructured.Unstructured) *Resource {
	kind := u.GetKind()
	apiVersion := u.GetAPIVersion()
	for _, r := range c.config.ChildResources {
		if r.APIVersion == apiVersion && r.Kind == kind {
			return &r
		}
	}
	return nil
}

func (c *Controller) reconcile(u *unstructured.Unstructured) {
	fmt.Println("reconcile", u)
	resources, err := c.reconciler.Reconcile(u)
	if err != nil {
		fmt.Printf("got an error %v", err)
		// TODO log
		return
	}

	// loop over resources, ensure they are the expected type,
	// and add/update/delete
	for _, r := range resources {
		setOwnerReference(u, r)
		setLabel(u, r)
		r.SetNamespace(u.GetNamespace())
		res := c.resourceForObject(r)
		if res == nil {
			fmt.Println("did not find resource", r)
			// TODO: log/error
			continue
		}

		fmt.Println("getname", r.GetName())
		if err := c.ensureResource(res, r); err != nil {
			fmt.Println(err)
		}
	}
}

// Run ...
func (c *Controller) Run(stopChan <-chan struct{}) {
	go func() {
		// events channel is closed below
		for u := range c.events {
			fmt.Println("got an event")
			c.reconcile(u)
		}
	}()

	loopStop := make(chan struct{})

	stopLoop := func() {
		// don't block on stopping the watcher
		select {
		case loopStop <- struct{}{}:
		default:
		}
	}

	t := time.NewTicker(c.config.SyncInterval)
	for {
		c.doGet()
		// start new watch
		go c.doWatch(loopStop)
		select {
		case <-stopChan:
			t.Stop()
			stopLoop()
			close(c.events)
			return
		case <-t.C:
			stopLoop()
		}
	}
}

func (c *Controller) doGet() {
	req := c.client.Get().
		Prefix("apis", c.config.Resource.APIVersion, c.config.Resource.Resource).
		SetHeader("Accept", "application/json").
		Timeout(time.Second * 10)

	res := req.Do()
	if res.Error() != nil {
		// TODO: log
		return
	}
	body, err := res.Raw()
	if err != nil {
		// TODO: log
		return
	}

	var list unstructured.UnstructuredList
	if err := json.Unmarshal(body, &list); err != nil {
		// TODO: log
		return
	}
	for _, item := range list.Items {
		fmt.Println("pushing an event", item)
		c.events <- &item
	}

}
func (c *Controller) doWatch(stopChan <-chan struct{}) {
	req := c.client.Get().
		Prefix("apis", c.config.Resource.APIVersion, c.config.Resource.Resource).
		SetHeader("Accept", "application/json").
		Timeout(time.Second*10).Param("watch", "true")
	stream, err := req.Stream()
	if err != nil {
		// TODO: log
		return
	}

	d := decoder{
		reader:  stream,
		decoder: json.NewDecoder(stream),
	}

	streamWatcher := watch.NewStreamWatcher(&d)

	go func() {
		events := streamWatcher.ResultChan()
		for e := range events {
			switch string(e.Type) {
			case "ADDED", "MODIFIED":
				u, ok := e.Object.(*unstructured.Unstructured)
				if ok {
					c.events <- u
				}
			}
		}
	}()
	<-stopChan
	streamWatcher.Stop()
}

type watchEvent struct {
	Type   string                    `json:"type"`
	Object unstructured.Unstructured `json:"object"`
}

type decoder struct {
	reader  io.ReadCloser
	decoder *json.Decoder
}

func (d *decoder) Decode() (action watch.EventType, object runtime.Object, err error) {
	var e watchEvent
	if err := d.decoder.Decode(&e); err != nil {
		return "", nil, errors.Wrap(err, "json decoder failed")
	}

	return watch.EventType(e.Type), &e.Object, nil
}

func (d *decoder) Close() {
	d.reader.Close()
}

func doRequest(r *rest.Request) (*unstructured.Unstructured, int, error) {
	req := r.SetHeader("Accept", "application/json")
	res := req.Do()

	var statusCode int
	res.StatusCode(&statusCode)

	// special case for not found to avoid checking body
	if statusCode == 404 {
		return nil, statusCode, nil
	}

	if err := res.Error(); err != nil {
		return nil, statusCode, errors.Wrapf(err, "request failed %q", req.URL().String())
	}

	body, err := res.Raw()
	if err != nil {
		return nil, statusCode, errors.Wrapf(err, "failed to get response body %s", req.URL().String())
	}

	var u unstructured.Unstructured
	if err := json.Unmarshal(body, &u); err != nil {
		return nil, statusCode, errors.Wrapf(err, "failed to unmarshal response body %s", req.URL().String())
	}

	return &u, statusCode, nil
}

// these API versions will use strategic patching.
// if not listed, update is used
var usePatch map[string]bool = map[string]bool{
	"apps/v1": true,
	"v1":      true,
}

func (c *Controller) requestForResource(method string, r *Resource) *rest.Request {
	req := c.client.Verb(method).Resource(r.Resource)

	if r.APIVersion == "v1" {
		req = req.Prefix("api/v1")
	} else {
		req = req.Prefix("apis", r.APIVersion)
	}
	return req
}

func (c *Controller) ensureResource(r *Resource, u *unstructured.Unstructured) error {
	req := c.requestForResource("GET", r).
		Namespace(u.GetNamespace()).Name(u.GetName())

	fmt.Println("ensureResource", req.URL().String())

	old, code, err := doRequest(req)
	if code == 404 {
		body, err := json.Marshal(u)
		if err != nil {
			return errors.Wrapf(err, "failed to marshal %s %s", r.APIVersion, r.Kind)
		}

		fmt.Println("ensureResource", "calling POST")
		// we do not care about any error if not found
		req = c.requestForResource("POST", r).
			Namespace(u.GetNamespace()).
			Body(body).SetHeader("Content-Type", "application/json")

		_, code, err = doRequest(req)
		if err != nil {
			return errors.Wrapf(err, "failed to create %s %s", r.APIVersion, r.Kind)
		}
		if code != 201 {
			return errors.Wrapf(err, "unexpected status code %d for %s %s", code, r.APIVersion, r.Kind)
		}
		return nil
	}

	if err != nil {
		return errors.Wrapf(err, "failed to get %s %s", r.APIVersion, r.Kind)
	}

	// we shouldn't get here without an error, but just in case
	if code != 200 {
		return errors.Wrapf(err, "unexpected status code %d for %s %s", code, r.APIVersion, r.Kind)
	}

	if usePatch[r.APIVersion] {
		patch, err := createPatch(old, u)
		if err != nil {
			return errors.Wrapf(err, "failed to create patch for %s %s", code, r.APIVersion, r.Kind)
		}
		req = c.requestForResource("PATCH", r).
			Namespace(u.GetNamespace()).
			Body(patch).SetHeader("Content-Type", string(types.StrategicMergePatchType))
		_, code, err = doRequest(req)
		if err != nil {
			return errors.Wrapf(err, "failed to patch %s %s", r.APIVersion, r.Kind)
		}
		if code != 200 {
			return errors.Wrapf(err, "unexpected status code %d for %s %s", code, r.APIVersion, r.Kind)
		}
	} else {
		u.SetResourceVersion(old.GetResourceVersion())
		u.SetGeneration(old.GetGeneration())

		// todo : we need to cleanup uid, etc
		// we are not using patch, but it tells us if objects are same
		patch, err := createPatch(old, u)
		if err != nil {
			return errors.Wrapf(err, "failed to create patch for %s %s", code, r.APIVersion, r.Kind)
		}
		if patch == nil {
			// up to date
			return nil
		}
		body, err := json.Marshal(u)
		if err != nil {
			return errors.Wrapf(err, "failed to marshal %s %s", r.APIVersion, r.Kind)
		}

		req = c.requestForResource("PUT", r).
			Namespace(u.GetNamespace()).Name(u.GetName()).
			Body(body).SetHeader("Content-Type", "application/json")
		_, code, err = doRequest(req)
		if err != nil {
			return errors.Wrapf(err, "failed to update %s %s", r.APIVersion, r.Kind)
		}
		if code != 200 {
			return errors.Wrapf(err, "unexpected status code %d for %s %s", code, r.APIVersion, r.Kind)
		}
	}
	return nil
}

func createPatch(old *unstructured.Unstructured, new *unstructured.Unstructured) ([]byte, error) {
	oldData, err := json.Marshal(old)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal")
	}
	newData, err := json.Marshal(new)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal")
	}

	var dataStruct map[string]interface{}
	patch, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, dataStruct)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create patch")
	}

	// if len is 2 or less, it is {}, which means no change
	if len(patch) <= 2 {
		return nil, nil
	}

	return patch, nil
}
