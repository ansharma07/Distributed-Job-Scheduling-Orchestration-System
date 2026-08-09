package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/chandan0804/distributed-task-scheduler/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	schedulerAddr = flag.String("scheduler", "127.0.0.1:8000", "Scheduler gRPC address")
	workerID      = flag.String("worker-id", "", "Worker ID (auto-generated if empty)")
	capacity      = flag.Int("capacity", 10, "Worker capacity (concurrent tasks)")
)

type Worker struct {
	id          string
	capacity    int32
	client      pb.TaskSchedulerClient
	conn        *grpc.ClientConn
	activeTasks map[string]context.CancelFunc
}

func main() {
	flag.Parse()

	// Generate worker ID if not provided
	if *workerID == "" {
		*workerID = fmt.Sprintf("worker-%s", uuid.New().String()[:8])
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("Starting worker: %s", *workerID)

	// Connect to scheduler
	conn, err := grpc.Dial(*schedulerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(10*1024*1024),
			grpc.MaxCallSendMsgSize(10*1024*1024),
		),
	)
	if err != nil {
		log.Fatalf("Failed to connect to scheduler: %v", err)
	}
	defer conn.Close()

	client := pb.NewTaskSchedulerClient(conn)

	worker := &Worker{
		id:          *workerID,
		capacity:    int32(*capacity),
		client:      client,
		conn:        conn,
		activeTasks: make(map[string]context.CancelFunc),
	}

	// Start worker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start task streaming
	go worker.streamTasks(ctx)

	// Start result streaming
	go worker.streamResults(ctx)

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	log.Printf("Worker %s is running (capacity: %d)", *workerID, *capacity)
	log.Printf("Connected to scheduler at %s", *schedulerAddr)

	<-sigChan
	log.Println("Shutting down worker...")
	cancel()
	time.Sleep(2 * time.Second) // Allow graceful shutdown
	log.Println("Worker stopped")
}

func (w *Worker) streamTasks(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		stream, err := w.client.StreamTasks(ctx)
		if err != nil {
			log.Printf("Failed to create task stream: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Request tasks periodically
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Calculate available capacity
				availableCapacity := w.capacity - int32(len(w.activeTasks))
				if availableCapacity <= 0 {
					continue
				}

				// Request tasks
				req := &pb.TaskRequest{
					WorkerId: w.id,
					Capacity: availableCapacity,
				}

				if err := stream.Send(req); err != nil {
					log.Printf("Failed to send task request: %v", err)
					break
				}

				// Receive tasks
				resp, err := stream.Recv()
				if err != nil {
					log.Printf("Failed to receive tasks: %v", err)
					break
				}

				// Process received tasks
				for _, task := range resp.Tasks {
					go w.executeTask(ctx, task)
				}
			}
		}
	}
}

func (w *Worker) streamResults(ctx context.Context) {
	// This would be implemented to send results back
	// For now, results are sent directly in executeTask
}

func (w *Worker) executeTask(ctx context.Context, task *pb.Task) {
	startTime := time.Now()
	log.Printf("Executing task %s: %s", task.Id, task.Name)

	// Simulate task execution
	// In a real implementation, this would execute the actual task logic
	time.Sleep(time.Duration(100+len(task.Payload)) * time.Millisecond)

	// Send result
	result := &pb.TaskResult{
		TaskId:      task.Id,
		WorkerId:    w.id,
		Status:      pb.TaskStatus_COMPLETED,
		CompletedAt: time.Now().Unix(),
	}

	// Create result stream
	stream, err := w.client.StreamResults(ctx)
	if err != nil {
		log.Printf("Failed to create result stream: %v", err)
		return
	}

	if err := stream.Send(result); err != nil {
		log.Printf("Failed to send result: %v", err)
		return
	}

	ack, err := stream.Recv()
	if err != nil {
		log.Printf("Failed to receive ack: %v", err)
		return
	}

	if ack.Success {
		duration := time.Since(startTime)
		log.Printf("Task %s completed successfully in %v", task.Id, duration)
	} else {
		log.Printf("Task %s completion not acknowledged", task.Id)
	}

	stream.CloseSend()
}
