package mrlib

// RPC definitions


import "os"
import "strconv"

// Task type
type TaskType int

const (
  Map       TaskType = 1
  Reduce    TaskType = 2
  Done      TaskType = 3
)

//
// The first RPC
// idle worker -> coordinator
// GetTask RPCs are sent from an idle worker to coordinator to ask
// the next task to perform
// 
type GetTaskArgs struct{}

type GetTaskReply struct {
  // what type of the task is this? (map, reduce, done)
  TaskType TaskType

  // task number of ether map/reduce task
  TaskNum int

  // needed for Map (to know which file to write)
  NReduceTask int

  // needed for Map (to know which file to read)
  MapFile string

  // needed for Reduce (to know how many intermediate map file to read)
  NMapTasks int
}


//
// The second RPC
// finshed worker -> coordinator
// FinishedTask RPCs are sent from an idle worker 
// to coordinator to indicate that a task has been completed
//
type FinishedTaskArgs struct {
  // what type of task was the worker assigned?
  TaskType TaskType

  // which task was it ?
  TaskNum int
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master
func masterSock() string {
  s := "/var/rmp/pc-mr-"
  s += strconv.Itoa(os.Getuid())
  return s
}
