package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type SchedulePhase uint8

func (phase SchedulePhase) String() string {
	switch phase {
	case MapPhase:
		return "MapPhase"
	case ReducePhase:
		return "ReducePhase"
	case FinishedPhase:
		return "FinishPhase"
	default:
		panic(fmt.Sprintf("Unexpected phase: %d", phase))
	}
}

const (
	MapPhase SchedulePhase = iota + 1
	ReducePhase
	FinishedPhase
)

const TimeoutCheckInterval = 5 * time.Second
const MaxTaskRunInterval = 10 * time.Second

type Coordinator struct {
	files   []string
	nReduce int

	phase     SchedulePhase
	taskQueue chan *Task
	taskMeta  map[int]*Task

	heartBeatCh chan HeartBeatMessage
	reportCh    chan ReportMessage
	doneCh      chan struct{}
}

type HeartBeatMessage struct {
	reply *HeartBeatReply
	ok    chan struct{}
}

type ReportMessage struct {
	request *ReportRequest
	ok      chan struct{}
}

// MakeCoordinator returns a new coordinator.
//
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		taskQueue:   make(chan *Task, max(len(files), nReduce)),
		heartBeatCh: make(chan HeartBeatMessage),
		reportCh:    make(chan ReportMessage),
		doneCh:      make(chan struct{}, 1),
	}

	c.startMapPhase()
	c.server()

	go c.schedule()

	log.Printf("Coordinator has been started.")
	return &c
}

func (c *Coordinator) HeartBeat(args *HeartBeatRequest, reply *HeartBeatReply) error {
	msg := HeartBeatMessage{
		reply: reply,
		ok:    make(chan struct{}),
	}

	c.heartBeatCh <- msg

	<-msg.ok
	return nil
}

func (c *Coordinator) Report(args *ReportRequest, reply *HeartBeatReply) error {
	msg := ReportMessage{
		request: args,
		ok:      make(chan struct{}),
	}

	c.reportCh <- msg

	<-msg.ok
	return nil
}

func (c *Coordinator) startMapPhase() {
	c.phase = MapPhase
	c.taskMeta = make(map[int]*Task, len(c.files))
	for index, filePath := range c.files {
		task := &Task{
			taskNumber: index,
			filePath:   filePath,
			state:      Idle,
		}

		c.taskQueue <- task
		c.taskMeta[index] = task
	}
}

func (c *Coordinator) startReducePhase() {
	c.phase = ReducePhase
	c.taskMeta = make(map[int]*Task, c.nReduce)
	for index := 0; index < c.nReduce; index++ {
		task := &Task{
			taskNumber: index,
			state:      Idle,
		}
		c.taskQueue <- task
		c.taskMeta[index] = task
	}
}

func (c *Coordinator) schedule() {
	ticker := time.NewTicker(TimeoutCheckInterval)
	for {
		select {
		case msg := <-c.heartBeatCh:
			switch c.phase {
			case MapPhase:
				if c.allTaskDone() {
					log.Printf("Coordinator: %v finished, start %v", MapPhase, ReducePhase)
					c.startReducePhase()
				}
				c.assignTask(msg.reply)
			case ReducePhase:
				if c.allTaskDone() {
					c.phase = FinishedPhase
					msg.reply.JobType = ExitJob
					c.doneCh <- struct{}{}
				} else {
					c.assignTask(msg.reply)
				}
			case FinishedPhase:
				msg.reply.JobType = ExitJob
			default:
				panic(fmt.Sprintf("Coordinator: enter unexpected phase: %d", c.phase))
			}

			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			taskNumber := msg.request.TaskNumber
			c.taskMeta[taskNumber].state = Completed
			msg.ok <- struct{}{}
		case <-ticker.C:
			if c.phase == FinishedPhase {
				ticker.Stop()
				return
			}

			for _, task := range c.taskMeta {
				if task.state == InProgress && time.Now().Sub(task.startTime) > MaxTaskRunInterval {
					task.state = Idle
					c.taskQueue <- task
				}
			}
		}
	}
}

func (c *Coordinator) hasTaskToSchedule() bool {
	return len(c.taskQueue) > 0
}

func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.taskMeta {
		if task.state != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) assignTask(reply *HeartBeatReply) {
	if !c.hasTaskToSchedule() {
		reply.JobType = WaitJob
		log.Printf("Coordinator: no task to schedule, please wait.")
		return
	}

	task := <-c.taskQueue
	task.state = InProgress
	task.startTime = time.Now()

	reply.TaskNumber = task.taskNumber
	reply.NReduce = c.nReduce
	reply.NFiles = len(c.files)
	if c.phase == MapPhase {
		reply.FilePath = task.filePath
		reply.JobType = MapJob
	} else if c.phase == ReducePhase {
		reply.JobType = ReduceJob
	} else {
		panic(fmt.Sprintf("Enter unexpected phase: %d", c.phase))
	}

	log.Printf("Coordinator: assigned a task %v to worker", reply)
}

// Done returns whether the entire job has finished.
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	<-c.doneCh
	return true
}

// an example RPC handler.
//
// the RPC argument and request types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
//
// Coordinator start rpc server for Worker.
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
