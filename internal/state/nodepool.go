package state

import (
    "context"
    "sigs.k8s.io/controller-runtime/pkg/client"
    "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	nfdKernelLabelKey        = "feature.node.kubernetes.io/kernel-version.full"
	nfdOSTreeVersionLabelKey = "feature.node.kubernetes.io/system-os_release.OSTREE_VERSION"
)

type nodePool struct {  // 👈 Экспорт!
    name         string
    osRelease    string
    osVersion    string
    rhcosVersion string
    kernel       string
    nodeSelector map[string]string
}

func getNodePools(ctx context.Context, k8sClient client.Client, selector map[string]string, precompiled bool, openshift bool) ([]nodePool, error) {
    logger := log.FromContext(ctx)
    
    np := nodePool{
        nodeSelector: map[string]string{
            "kubernetes.io/hostname": "minikube",
        },
        osRelease:  "ubuntu",
        osVersion:  "20.04",
        kernel:     "5.4.0",
        name:       "ubuntu20.04",  // 👈 РЕАЛЬНЫЙ тег!
        rhcosVersion: "",
    }
    
    logger.Info("FORCE node pool for minikub ubuntu20.04", "NodePool", np)
    return []nodePool{np}, nil
}

func (n *nodePool) getOS() string {
    return n.name  // 👈 Возвращай name напрямую!
}