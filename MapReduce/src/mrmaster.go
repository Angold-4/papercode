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
}
