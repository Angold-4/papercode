package main

//
// mrsequential runs the maps and reduces one at a time, in a single process
// no distributed speed up, single worker
// 
// go run mrsequential.go xxx.so filename
//

import "fmt"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

import "mrlib"

// for sorting by key
type ByKey []mrlib.KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


// load the application Map and Reduce functions
// from a plugin file, e.g. ./mrapps/wc.so

func loadPlugin(filename string) (
func(string, string) []mrlib.KeyValue, func(string, []string) string) {
  p, err := plugin.Open(filename)
  if err != nil {
    log.Fatalf("cannot load plugin %v", filename)
  }
  xmapf, err := p.Lookup("Map")
  if err != nil {
    log.Fatalf("cannot find Map in %v", filename)
  }
  xreducef, err := p.Lookup("Reduce")
  if err != nil {
    log.Fatalf("cannot find Reduce in %v", filename)
  }

  mapf := xmapf.(func(string, string) []mrlib.KeyValue)
  reducef := xreducef.(func(string, []string) string)

  return mapf, reducef
}


func main() {
  if len(os.Args) < 3 {
    fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles... \n")
    os.Exit(1)
  }

  mapf, reducef := loadPlugin(os.Args[1])

  // 
  // read each input file, pass it to Map, 
  // accumulate the intermediate Map output
  //
  intermediate := []mrlib.KeyValue{}
  for _, filename := range os.Args[2:] {
    file, err := os.Open(filename)
    if err != nil {
      log.Fatalf("cannot open %v", filename)
    }
    content, err := ioutil.ReadAll(file)
    if err != nil {
      log.Fatalf("cannot read %v", filename) 
    }
    file.Close()
    kva := mapf(filename, string(content)) // key-value array
    intermediate = append(intermediate, kva...)
  }

  // fmt.Print(intermediate) // debug

  sort.Sort(ByKey(intermediate))

  outname := "mr-out-0"
  ofile, _ := os.Create(outname)

  //
  // call Reduce on each distinct key in intermediate[,]
  // and print the result to mr-out-0
  //
  i := 0
  for i < len(intermediate) {
    j := i + 1

    // group same keys on values
    for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
      j++
    }
    values := []string{}
    for k := i; k < j; k++ {
      values = append(values, intermediate[k].Value)
    }

    output := reducef(intermediate[i].Key, values)

    fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
    
    i = j
  }

  ofile.Close()
}
