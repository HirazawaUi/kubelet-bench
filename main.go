package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

const (
	namespace          = "default"
	podNamePrefix      = "test-pod"
	podReplacePeriod   = 50 * time.Millisecond
	podReplaceDuration = 60 * time.Second
)

func createPodObject(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app": "kubelet-bench",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:            "nginx",
					Image:           "nginx:stable",
					ImagePullPolicy: corev1.PullIfNotPresent,
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}
}

func getKubernetesClient() (*kubernetes.Clientset, error) {
	var kubeconfig string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	}
	if envKubeconfig := os.Getenv("KUBECONFIG"); envKubeconfig != "" {
		kubeconfig = envKubeconfig
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %v", err)
	}
	config.QPS = 1000
	config.Burst = 1000

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %v", err)
	}
	return clientset, nil
}

func createAndWaitForPods(ctx context.Context, clientset *kubernetes.Clientset) (time.Duration, error) {
	fmt.Printf("Starting to create %d pods...\n", *totalPods)
	startTime := time.Now()

	for i := 0; i < *totalPods; i++ {
		select {
		case <-ctx.Done():
			return time.Since(startTime), fmt.Errorf("pod creation cancelled: %w", ctx.Err())
		default:
		}

		podName := fmt.Sprintf("%s-%d", podNamePrefix, i)
		pod := createPodObject(podName)
		_, err := clientset.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		if err != nil && ctx.Err() == nil {
			fmt.Printf("Warning: failed to create pod %s: %v. Continuing...\n", podName, err)
		}
		if i%20 == 0 && i > 0 {
			fmt.Printf("Requested creation of %d pods...\n", i)
		}
	}
	fmt.Printf("All %d pod creation requests sent.\n", totalPods)
	fmt.Println("Waiting for all pods to enter Running state...")

	for {
		select {
		case <-ctx.Done():
			return time.Since(startTime), fmt.Errorf("waiting for pods cancelled: %w", ctx.Err())
		default:
		}
		podList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app=kubelet-bench",
		})
		if err != nil && ctx.Err() == nil {
			fmt.Printf("Warning: failed to get pod list: %v. Retrying in 2s...\n", err)
			time.Sleep(2 * time.Second)
			continue
		}
		currentRunning := 0
		for _, pod := range podList.Items {
			if pod.Status.Phase == corev1.PodRunning {
				currentRunning++
			}
		}
		if currentRunning >= *totalPods {
			elapsedTime := time.Since(startTime)
			fmt.Printf("All %d pods are Running, time elapsed: %v\n", totalPods, elapsedTime)
			return elapsedTime, nil
		}
		fmt.Printf("Waiting: %d/%d pods Running\n", currentRunning, *totalPods)
		time.Sleep(2 * time.Second)
	}
}

func waitForPodDeletion(ctx context.Context, clientset *kubernetes.Clientset, podName string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
			if errors.IsNotFound(err) {
				return nil
			}
			if err != nil && ctx.Err() == nil {
				fmt.Printf("Error checking pod %s deletion: %v. Retrying.\n", podName, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func replacePods(ctx context.Context, clientset *kubernetes.Clientset) {
	fmt.Printf("Starting pod replacement for %v...\n", podReplaceDuration)

	podIndex := 1
	ticker := time.NewTicker(podReplacePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Pod replacement phase completed or cancelled.")
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				continue
			}
			currentPodName := fmt.Sprintf("%s-%d", podNamePrefix, podIndex)
			newPodName := fmt.Sprintf("%s-replaced-%d-%s", podNamePrefix, podIndex, strconv.FormatInt(time.Now().UnixNano(), 36))

			delOpCtx, delOpCancel := context.WithTimeout(ctx, 30*time.Second)
			err := clientset.CoreV1().Pods(namespace).Delete(delOpCtx, currentPodName, metav1.DeleteOptions{})
			delOpCancel()
			if err != nil && !errors.IsNotFound(err) && ctx.Err() == nil {
				fmt.Printf("Failed to delete pod %s: %v. Skipping.\n", currentPodName, err)
				podIndex = (podIndex + 1) % *totalPods
				continue
			}

			if err == nil {
				delWaitCtx, delWaitCancel := context.WithTimeout(ctx, 45*time.Second)
				waitErr := waitForPodDeletion(delWaitCtx, clientset, currentPodName)
				delWaitCancel()
				if waitErr != nil && ctx.Err() == nil {
					fmt.Printf("Error waiting for pod %s deletion: %v. Proceeding.\n", currentPodName, waitErr)
				}
			}

			if ctx.Err() != nil {
				continue
			}
			pod := createPodObject(newPodName)
			createOpCtx, createOpCancel := context.WithTimeout(ctx, 30*time.Second)
			_, err = clientset.CoreV1().Pods(namespace).Create(createOpCtx, pod, metav1.CreateOptions{})
			createOpCancel()
			if err != nil && ctx.Err() == nil {
				fmt.Printf("Failed to recreate pod %s: %v\n", newPodName, err)
			}
			podIndex = (podIndex + 1) % *totalPods
		}
	}
}

func monitorResources(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("Starting resource monitoring...")

	kubeletFile, err := os.Create("kubelet_usage.csv")
	if err != nil {
		fmt.Printf("Error creating kubelet_usage.csv: %v\n", err)
		return
	}
	defer kubeletFile.Close()
	kubeletWriter := csv.NewWriter(kubeletFile)
	defer kubeletWriter.Flush()

	containerdFile, err := os.Create("containerd_usage.csv")
	if err != nil {
		fmt.Printf("Error creating containerd_usage.csv: %v\n", err)
		return
	}
	defer containerdFile.Close()
	containerdWriter := csv.NewWriter(containerdFile)
	defer containerdWriter.Flush()

	header := []string{"timestamp", "cpu_usage", "memory_usage_kb"}
	kubeletWriter.Write(header)
	containerdWriter.Write(header)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping resource monitoring.")
			return
		case <-ticker.C:
			timestamp := time.Now().Format(time.RFC3339)

			// Monitor kubelet using top
			cmdTop := exec.Command("top", "-b", "-n", "1", "-u", "root")
			outputTop, err := cmdTop.Output()
			if err != nil {
				fmt.Printf("Error getting stats from top: %v\n", err)
			} else {
				scanner := bufio.NewScanner(bytes.NewReader(outputTop))
				for scanner.Scan() {
					line := scanner.Text()
					fields := strings.Fields(line)
					if len(fields) < 12 {
						continue
					}
					processName := fields[11]
					if strings.Contains(processName, "kubelet") {
						cpu := fields[8]  // %CPU
						mem := fields[5]  // RES in KB
						rowData := []string{timestamp, cpu, mem}
						if err := kubeletWriter.Write(rowData); err != nil {
							fmt.Printf("Error writing to kubelet_usage.csv: %v\n", err)
						}
						break // Assume only one kubelet process
					}
				}
			}

			// Monitor containerd using pidstat
			pidCmd := exec.Command("pgrep", "-x", "containerd")
			pidOutput, err := pidCmd.Output()
			if err != nil {
				fmt.Printf("Error finding containerd PID: %v\n", err)
				continue
			}
			pid := strings.TrimSpace(string(pidOutput))
			if pid == "" {
				fmt.Println("containerd process not found")
				continue
			}

			pidstatCmd := exec.Command("pidstat", "-p", pid, "-ru", "-h", "1", "1")
			pidstatOutput, err := pidstatCmd.Output()
			if err != nil {
				fmt.Printf("Error getting stats from pidstat: %v\n", err)
				continue
			}

			pidstatScanner := bufio.NewScanner(bytes.NewReader(pidstatOutput))
			var cpu, mem string
			for pidstatScanner.Scan() {
				line := pidstatScanner.Text()
				if strings.Contains(line, "Average") || !strings.Contains(line, pid) {
					continue
				}
				fields := strings.Fields(line)
				if len(fields) >= 13 {
					cpu = fields[7]
					mem = fields[12] // RSS column for memory in KB
					rowData := []string{timestamp, cpu, mem}
					if err := containerdWriter.Write(rowData); err != nil {
						fmt.Printf("Error writing to containerd_usage.csv: %v\n", err)
					}
					break
				}
			}

			kubeletWriter.Flush()
			containerdWriter.Flush()
		}
	}
}

var (
	createPods *bool
	stressTest *bool
	totalPods  *int
)

func main() {
	createPods = flag.Bool("create-pods", true, "Create initial batch of pods (default true)")
	stressTest = flag.Bool("stress-test", true, "Enable stress testing phase with pod replacement and resource monitoring (default true)")
	totalPods = flag.Int("max-pod", 100, "Maximum number of pods to create")

	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("Received signal. Shutting down...")
		cancel()
	}()

	clientset, err := getKubernetesClient()
	if err != nil {
		fmt.Printf("Failed to get Kubernetes client: %v\n", err)
		os.Exit(1)
	}

	var monitorWg sync.WaitGroup
	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	monitorWg.Add(1)
	go monitorResources(monitorCtx, &monitorWg)

	if *createPods {
		podsCreationDuration, err := createAndWaitForPods(ctx, clientset)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("Pod creation cancelled by signal.")
			} else {
				fmt.Printf("Error during pod creation: %v\n", err)
			}
			os.Exit(1)
		}
		fmt.Printf("Initial pods ready in %v.\n", podsCreationDuration)
	}

	if *stressTest {
		replaceCtx, replaceCancel := context.WithTimeout(context.Background(), podReplaceDuration)
		defer replaceCancel()

		go replacePods(replaceCtx, clientset)

		<-replaceCtx.Done()
		fmt.Printf("Pod replacement ran for %v.\n", podReplaceDuration)
	}

	monitorCancel()
	monitorWg.Wait()

	fmt.Println("Test completed. Data saved to kubelet_usage.csv and containerd_usage.csv")
}
