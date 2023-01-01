package main

//
// start the master process, which is implemented
// in ./mr/master.go
// 
// go run mrmaster pg*.txt
//

import "time"
import "os"
import "fmt"
import "mrlib"

func main() {
  if len(os.Args) != 2 {
    fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
    os.Exit(1)
  }

  m := mrlib.MakeMaster(os.Args[1:], 10) 
  // 10 is the number of of reduce task to use
  for m.Done() == false {
    // every seconds check whether the whole task is finished
    time.Sleep(time.Second)
  }

  time.Sleep(time.Second)
}
