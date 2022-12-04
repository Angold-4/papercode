package mrlib

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Master struct {

  // protect coordinator state from concurrent access
  mu sync.Mutex

  // allow coordinator to wait to assign reduce tasks until all map
  // tasks have finished, or when all tasks are assigned and are running

  // the coordinator is worken up either when a task has finished,
  // or if a timeout has expired.
  cond *sync.Cond

  mapFiles               []string
  nMapTasks              int
  nReduceTasks           int

  // keep track of when tasks are assigned
  // and which tasks have finished
  mapTasksFinished       []bool
  mapTasksIssued         []time.Time
  reduceTasksFinished    []bool
  reduceTasksIssued      []time.Time

  // set to true when all reduce tasks are complete
  isDone                 bool
}

//
// create a Master 
// mrmaster.go calls this function
// nReduce is the number of reduce tasks to use.
// 
func MakeMaster(files []string, nReduce int) *Master {
  m := Master{}

  m.cond = sync.NewCond(&m.mu)
  m.mapFiles = files
  m.nMapTasks = len(files)
  m.mapTasksFinished = make([]bool, len(files))
  m.mapTasksIssued = make([]time.Time, len(files)) // per file a task

  m.nReduceTasks = nReduce
  m.reduceTasksFinished = make([]bool, nReduce)
  m.reduceTasksIssued = make([]time.Time, nReduce)


  return &m
}
