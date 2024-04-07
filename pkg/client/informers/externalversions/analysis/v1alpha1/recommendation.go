/*
Copyright 2022 The Koordinator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by informer-gen. DO NOT EDIT.

package v1alpha1

import (
	"context"
	time "time"

	analysisv1alpha1 "github.com/koordinator-sh/koordinator/apis/analysis/v1alpha1"
	versioned "github.com/koordinator-sh/koordinator/pkg/client/clientset/versioned"
	internalinterfaces "github.com/koordinator-sh/koordinator/pkg/client/informers/externalversions/internalinterfaces"
	v1alpha1 "github.com/koordinator-sh/koordinator/pkg/client/listers/analysis/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
)

// RecommendationInformer provides access to a shared informer and lister for
// Recommendations.
type RecommendationInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1alpha1.RecommendationLister
}

type recommendationInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewRecommendationInformer constructs a new informer for Recommendation type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewRecommendationInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredRecommendationInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredRecommendationInformer constructs a new informer for Recommendation type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredRecommendationInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.AnalysisV1alpha1().Recommendations(namespace).List(context.TODO(), options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.AnalysisV1alpha1().Recommendations(namespace).Watch(context.TODO(), options)
			},
		},
		&analysisv1alpha1.Recommendation{},
		resyncPeriod,
		indexers,
	)
}

func (f *recommendationInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredRecommendationInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *recommendationInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&analysisv1alpha1.Recommendation{}, f.defaultInformer)
}

func (f *recommendationInformer) Lister() v1alpha1.RecommendationLister {
	return v1alpha1.NewRecommendationLister(f.Informer().GetIndexer())
}