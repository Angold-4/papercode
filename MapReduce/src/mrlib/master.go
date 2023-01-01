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
// mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
  m.mu.Lock()
  defer m.mu.Unlock() // unlock after leave
  return m.isDone
}

// start a thread (server) that listens for RPCs from worker.go
// 
func (m *Master) server() {
  rpc.Register(m) 
  // Register publishes the receiver's methods in the DefaultServer
  // e.g.  HandleGetTask
  rpc.HandleHTTP()

  sockname := masterSock()

  os.Remove(sockname) // empty it

  // Listen specify the addr
  l, e := net.Listen("unix", sockname) // l stands for listener
  if e != nil {
    log.Fatal("listen error:", e)
  }

  go http.Serve(l, nil) // k
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

  // wake up the GetTask handler thread every once in a while to check if some task hasn't finished

  go func() {
    for {
      m.mu.Lock()
      m.cond.Broadcast() 
      m.mu.Unlock()
      time.Sleep(time.Second)
    }
  }()

  m.server() // start handle HTTP from worker.go
  return &m
}
