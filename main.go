package main

import (
	"bufio"
	"bytes"
	"context"
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
	kubeletProcessCmd  = "ps -ef | grep \"/kubelet\" | grep -v \"sudo\" | grep -v grep"
)

var (
	cpuReadings   []float64
	memReadings   []float64
	maxCpuReading float64
	dataMutex     sync.Mutex
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

		time.Sleep(time.Second * 2)
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

	podIndex := 600
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

func getKubeletProcessStats() (pid string, cpuPercent float64, memoryMB float64, err error) {
	cmd := exec.Command("bash", "-c", kubeletProcessCmd)
	output, err := cmd.Output()
	if err != nil {
		return "", 0, 0, fmt.Errorf("failed to find kubelet: %v", err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(output))
	if scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			return "", 0, 0, fmt.Errorf("invalid ps output format")
		}
		pid = fields[1]
	} else {
		return "", 0, 0, fmt.Errorf("kubelet process not found")
	}

	psCpuCmd := exec.Command("ps", "-p", pid, "-o", "%cpu", "--no-headers")
	psCpuOutput, err := psCpuCmd.Output()
	if err != nil {
		return pid, 0, 0, fmt.Errorf("failed to get CPU usage: %v", err)
	}
	cpuStr := strings.TrimSpace(string(psCpuOutput))
	cpuPercent, err = strconv.ParseFloat(cpuStr, 64)
	if err != nil {
		return pid, 0, 0, fmt.Errorf("failed to parse CPU: %v", err)
	}

	psMemCmd := exec.Command("ps", "-p", pid, "-o", "rss", "--no-headers")
	psMemOutput, err := psMemCmd.Output()
	if err != nil {
		return pid, cpuPercent, 0, fmt.Errorf("failed to get memory usage: %v", err)
	}
	memRssStr := strings.TrimSpace(string(psMemOutput))
	rssKB, err := strconv.ParseFloat(memRssStr, 64)
	if err != nil {
		return pid, cpuPercent, 0, fmt.Errorf("failed to parse memory: %v", err)
	}
	memoryMB = rssKB / 1024.0

	return pid, cpuPercent, memoryMB, nil
}

func getKubeletInstantaneousCPU(pid string) (float64, error) {
	if pid == "" {
		return 0, fmt.Errorf("PID is empty, cannot get instantaneous CPU")
	}
	topCmd := exec.Command("top", "-b", "-n", "1", "-p", pid)
	topOutput, err := topCmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to get stats from top for pid %s: %v", pid, err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(topOutput))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, pid) {
			fields := strings.Fields(line)
			if len(fields) >= 9 {
				cpuStr := fields[8]
				cpuPercent, err := strconv.ParseFloat(cpuStr, 64)
				if err != nil {
					return 0, fmt.Errorf("failed to parse CPU from top output: %v", err)
				}
				return cpuPercent, nil
			}
		}
	}

	return 0, fmt.Errorf("could not find process stats in top output for pid %s", pid)
}

func monitorKubeletResources(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("Starting kubelet resource monitoring...")

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping kubelet resource monitoring.")
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				continue
			}
			pid, avgCpu, mem, err := getKubeletProcessStats()
			if err != nil && ctx.Err() == nil {
				fmt.Printf("Error getting kubelet stats (ps): %v\n", err)
				continue
			}

			instCpu, err := getKubeletInstantaneousCPU(pid)
			if err != nil && ctx.Err() == nil {
				fmt.Printf("Error getting kubelet instantaneous CPU (top): %v\n", err)
				instCpu = avgCpu
			}

			dataMutex.Lock()
			cpuReadings = append(cpuReadings, avgCpu)
			memReadings = append(memReadings, mem)
			if instCpu > maxCpuReading {
				maxCpuReading = instCpu
			}
			dataMutex.Unlock()
			if ctx.Err() == nil {
				fmt.Printf("Kubelet PID: %s, CPU (ps-avg): %.2f%%, CPU (top-inst): %.2f%%, Memory: %.2fMB\n", pid, avgCpu, instCpu, mem)
			}
		}
	}
}

func writeAveragesToFile(cpuAvg, memAvg, cpuMax float64) error {
	file, err := os.Create("resource_averages.txt")
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	content := fmt.Sprintf("Average CPU Usage (from ps): %.2f%%\nMaximum Instantaneous CPU Usage (from top): %.2f%%\nAverage Memory Usage (RSS): %.2fMB\n", cpuAvg, cpuMax, memAvg)
	_, err = file.WriteString(content)
	if err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}
	return nil
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
	go monitorKubeletResources(monitorCtx, &monitorWg)

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
		fmt.Printf("Initial pods ready in %v.", podsCreationDuration)
		monitorCancel()
	}

	if *stressTest {
		replaceCtx, replaceCancel := context.WithTimeout(context.Background(), podReplaceDuration)
		defer replaceCancel()

		go replacePods(replaceCtx, clientset)

		<-replaceCtx.Done()
		fmt.Printf("Pod replacement ran for %v.\n", podReplaceDuration)

		monitorCancel()
	}

	monitorWg.Wait()

	dataMutex.Lock()
	var cpuAvg, memAvg, cpuMax float64
	cpuMax = maxCpuReading
	if len(cpuReadings) > 0 {
		var totalCpu float64
		for _, cpu := range cpuReadings {
			totalCpu += cpu
		}
		cpuAvg = totalCpu / float64(len(cpuReadings))
	}
	if len(memReadings) > 0 {
		var totalMem float64
		for _, mem := range memReadings {
			totalMem += mem
		}
		memAvg = totalMem / float64(len(memReadings))
	}
	dataMutex.Unlock()

	if err := writeAveragesToFile(cpuAvg, memAvg, cpuMax); err != nil {
		fmt.Printf("Failed to write averages to file: %v\n", err)
	} else {
		fmt.Printf("Wrote stats to resource_averages.txt: CPU Avg (ps) %.2f%%, CPU Max (top) %.2f%%, Memory Avg %.2fMB\n", cpuAvg, cpuMax, memAvg)
	}

	fmt.Println("Test completed.")
}
