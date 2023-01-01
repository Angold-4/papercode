package mrlib

import "fmt"
import "sort"
import "os"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "encoding/json"

type KeyValue struct {
  Key string
  Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map
// same key will be assigned into same tmpFile
// and this split way is pretty avg
//
func ihash(key string) int {
  h := fnv.New32a()
  h.Write([]byte(key))
  return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
  for {
    args := GetTaskArgs{}
    reply := GetTaskReply{}

    // this will wait until we get assigned a task
    // call will block until the master reply
    call("Master.HandleGetTask", &args, &reply)

    switch reply.TaskType {
    case Map:
    case Reduce:
    }
  }
}

// get name of the intermediate file,
// given the map and reduce task numbers
func getIntermediateFile(mapTaskN int, redTaskN int) string {
  return fmt.Sprintf("mmr-%d-%d", mapTaskN, redTaskN) }


// finalizeIntermediateFile atomically renames 
// temporary intermediate files to a completed intermediate file
func finalizeIntermediateFile(tmpFile string, mapTaskN int, redTaskN int) {
  finalFile := getIntermediateFile(mapTaskN, redTaskN)
  os.Rename(tmpFile, finalFile)
}

// Implementation of a map task
func performMap(filename string, taskNum int, nReduceTasks int,
mapf func(string, string) []KeyValue) {
  // 1. Read contents to map
  file, err := os.Open(filename)
  if err != nil {
    log.Fatalf("cannot open %v", filename)
  }

  content, err := ioutil.ReadAll(file)
  if err != nil {
    log.Fatalf("cannot open %v", filename)
  }

  file.Close()

  // 2. Apply map function to contents of ifile and collect the set of key-value pairs
  kva := mapf(filename, string(content)) // key-value array

  // 3. Create temporary files and encoders for each file
  tmpFiles := []*os.File{}
  tmpFilenames := []string{}
  encoders := []*json.Encoder{}
  for r := 0; r < nReduceTasks; r++ {
    tmpFile, err := ioutil.TempFile("", "")
    if err != nil {
      log.Fatalf("cannot open tmpFile")
    }
    tmpFiles = append(tmpFiles, tmpFile)
    tmpFilename := tmpFile.Name()
    tmpFilenames := append(tmpFilenames, tmpFilename)
    enc := json.NewEncoder(tmpFile) // will write this kv into tmpfile
    encoders = append(encoders, enc)
  }

  // 4. write output keys to their approprite (tmp) intermediate files
  // using the provided ihash function
  for _, kv := range kva {
    r := ihash(kv.Key) % nReduceTasks
    encoders[r].Encode(&kv) // followed by a newline character
  }

  for _, f := range tmpFiles {
    f.Close()
  }

  // 5. atomically rename temp files to final intermediate files
  for r := 0; r < nReduceTasks; r++ {
    finalizeIntermediateFile(tmpFilenames[r], taskNum, r)
  }
}

func performReduce(taskNum int, nMapTasks int, reducef func(string, []string) string) {
}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// 
func call(rpcname string, args interface{}, reply interface{}) bool {
  sockname := coordinatorSock() // cook up a tmp socket
  c, err := rpc.DialHTTP("unix", sockname) 
  // connects to an HTTP RPC server at the specified network address

  if err != nil {
    log.Fatal("dialing:", err)
  }
  defer c.Close()

  err = c.Call(rpcname, args, reply) // call that rpc function
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

