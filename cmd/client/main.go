package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/chandan0804/distributed-task-scheduler/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	schedulerAddr = flag.String("scheduler", "127.0.0.1:8000", "Scheduler gRPC address")
	numTasks      = flag.Int("tasks", 10000, "Number of tasks to submit")
	concurrency   = flag.Int("concurrency", 100, "Concurrent submissions")
)

func main() {
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("Task Scheduler Client Demo")
	log.Printf("Connecting to scheduler at %s", *schedulerAddr)

	// Connect to scheduler
	conn, err := grpc.Dial(*schedulerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewTaskSchedulerClient(conn)

	// Check cluster health
	healthResp, err := client.HealthCheck(context.Background(), &pb.HealthCheckRequest{
		NodeId: "client",
	})
	if err != nil {
		log.Fatalf("Health check failed: %v", err)
	}

	log.Printf("Cluster Status:")
	log.Printf("  Leader: %s", healthResp.LeaderId)
	log.Printf("  Cluster Size: %d", healthResp.ClusterSize)
	log.Printf("  Pending Tasks: %d", healthResp.TasksPending)
	log.Printf("  Running Tasks: %d", healthResp.TasksRunning)
	log.Printf("  Completed Tasks: %d", healthResp.TasksCompleted)
	log.Println()

	// Submit tasks
	log.Printf("Submitting %d tasks with concurrency %d...", *numTasks, *concurrency)

	startTime := time.Now()
	var submitted, failed int64
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, *concurrency)

	for i := 0; i < *numTasks; i++ {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(taskNum int) {
			defer wg.Done()
			defer func() { <-semaphore }()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			req := &pb.SubmitTaskRequest{
				Name:        fmt.Sprintf("task-%d", taskNum),
				Payload:     []byte(fmt.Sprintf("payload for task %d", taskNum)),
				ScheduledAt: time.Now().Unix(),
				MaxRetries:  3,
			}

			resp, err := client.SubmitTask(ctx, req)
			if err != nil {
				atomic.AddInt64(&failed, 1)
				if taskNum%1000 == 0 {
					log.Printf("Failed to submit task %d: %v", taskNum, err)
				}
				return
			}

			if !resp.Success {
				atomic.AddInt64(&failed, 1)
				if taskNum%1000 == 0 {
					log.Printf("Task %d submission rejected: %s", taskNum, resp.Message)
				}
				return
			}

			atomic.AddInt64(&submitted, 1)
			if taskNum%1000 == 0 {
				log.Printf("Submitted task %d (ID: %s)", taskNum, resp.TaskId)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	log.Println()
	log.Printf("Task Submission Complete!")
	log.Printf("  Total Tasks: %d", *numTasks)
	log.Printf("  Submitted: %d", submitted)
	log.Printf("  Failed: %d", failed)
	log.Printf("  Duration: %v", duration)
	log.Printf("  Throughput: %.2f tasks/second", float64(submitted)/duration.Seconds())
	log.Println()

	// Monitor progress
	log.Println("Monitoring task progress (Ctrl+C to exit)...")
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		healthResp, err := client.HealthCheck(context.Background(), &pb.HealthCheckRequest{
			NodeId: "client",
		})
		if err != nil {
			log.Printf("Health check failed: %v", err)
			continue
		}

		log.Printf("[%s] Pending: %d | Running: %d | Completed: %d",
			time.Now().Format("15:04:05"),
			healthResp.TasksPending,
			healthResp.TasksRunning,
			healthResp.TasksCompleted,
		)

		// Exit when all tasks are processed
		if healthResp.TasksPending == 0 && healthResp.TasksRunning == 0 {
			log.Println()
			log.Printf("All tasks processed! Total completed: %d", healthResp.TasksCompleted)
			break
		}
	}
}
