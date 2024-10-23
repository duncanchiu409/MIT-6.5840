package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Status string

const (
	unstarted  Status = "unstarted"
	inprogress Status = "inprogress"
	completed  Status = "completed"
)

type TaskMeta struct {
	id        int
	fileName  string
	startTime time.Time
	endTime   time.Time
	status    Status
}

type Coordinator struct {
	// Your definitions here.
	mTasks     map[string]*TaskMeta
	rTasks     map[string]*TaskMeta
	mRemaining int
	rRemaining int
	nReduce    int
	cond       *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetMTask() (int, string) {
	for task := range c.mTasks {
		if c.mTasks[task].status == unstarted {
			c.mTasks[task].startTime = time.Now().UTC()
			c.mTasks[task].status = inprogress
			return c.mTasks[task].id, c.mTasks[task].fileName
		}
	}
	return 0, ""
}

func (c *Coordinator) GetRTask() (int, string) {
	for task := range c.rTasks {
		if c.rTasks[task].status == unstarted {
			c.rTasks[task].startTime = time.Now().UTC()
			c.rTasks[task].status = inprogress
			return c.rTasks[task].id, c.rTasks[task].fileName
		}
	}
	return 0, ""
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	if c.mRemaining != 0 {
		taskId, fileName := c.GetMTask()
		for fileName == "" { // Check if there are any process/request waiting
			if c.mRemaining == 0 {
				break
			}
			c.cond.Wait()
			taskId, fileName = c.GetMTask()
		}
		if fileName != "" {
			reply.TaskId = taskId
			reply.FileName = fileName
			reply.TaskType = mapTaskType
			reply.NReduce = c.nReduce
			return nil
		}
	}
	if c.rRemaining != 0 {
		taskId, fileName := c.GetRTask()
		for fileName == "" {
			if c.rRemaining == 0 {
				break
			}
			c.cond.Wait()
			taskId, fileName = c.GetRTask()
		}
		if fileName != "" {
			reply.TaskId = taskId
			reply.FileName = fileName
			reply.TaskType = reduceTaskType
			reply.NReduce = c.nReduce
			return nil
		}
	}
	return errors.New("complete tasks, no more tasks")
}

func (c *Coordinator) UpdateMTaskStatus(taskId int) error {
	for _, task := range c.mTasks {
		if task.id == taskId && task.status == inprogress {
			task.endTime = time.Now().UTC()
			task.status = completed
			c.mRemaining -= 1
		}
	}
	return nil
}

func (c *Coordinator) UpdateRTaskStatus(taskId int) error {
	for _, task := range c.rTasks {
		if task.id == taskId && task.status == inprogress {
			task.endTime = time.Now().UTC()
			task.status = completed
			c.rRemaining -= 1
		}
	}
	return nil
}

func (c *Coordinator) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	switch args.TaskType {
	case mapTaskType:
		err := c.UpdateMTaskStatus(args.TaskId)
		if err != nil {
			return err
		}
	case reduceTaskType:
		err := c.UpdateRTaskStatus(args.TaskId)
		if err != nil {
			return err
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	if c.mRemaining == 0 && c.rRemaining == 0 {
		ret = true
	}
	return ret
}

func (c *Coordinator) rescheduler() {
	for {
		c.cond.L.Lock()
		if c.mRemaining != 0 {
			for _, task := range c.mTasks {
				currTime := time.Now().UTC()
				if task.status == inprogress && currTime.Sub(task.startTime).Seconds() > 10 {
					task.startTime = currTime
					task.status = unstarted
					c.cond.Broadcast()
				}
			}
		} else if c.rRemaining != 0 {
			for _, task := range c.rTasks {
				currTime := time.Now().UTC()
				if task.status == inprogress && currTime.Sub(task.startTime).Seconds() > 10 {
					task.startTime = currTime
					task.status = unstarted
					c.cond.Broadcast()
				}
			}
		} else {
			c.cond.Broadcast()
			c.cond.L.Unlock()
			break
		}
		c.cond.L.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mTasks = map[string]*TaskMeta{}
	c.rTasks = map[string]*TaskMeta{}
	for i, fileName := range files {
		c.mTasks[fmt.Sprintf("%d", i)] = &TaskMeta{
			id:       i,
			fileName: fileName,
			status:   unstarted,
		}
	}
	for i := 0; i < nReduce; i++ {
		c.rTasks[fmt.Sprintf("%d", i)] = &TaskMeta{
			id:       i,
			fileName: fmt.Sprintf("mr-out-%d", i),
			status:   unstarted,
		}
	}
	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	c.cond = cond
	c.mRemaining = len(files)
	c.rRemaining = nReduce
	c.nReduce = nReduce

	go c.rescheduler()
	c.server()
	return &c
}
