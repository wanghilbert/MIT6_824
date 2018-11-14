package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup

	taskChan := make(chan int, ntasks)

	for i := 0; i < ntasks; i++ {
		taskChan <- i
	}

ProLoop:
	for {
		select {
		case ch := <-registerChan:
			{
				select {
				case task := <-taskChan:
					{
						var fname string
						switch phase {
						case mapPhase:
							fname = mapFiles[task]
						case reducePhase:
							fname = ""
						}
						req := DoTaskArgs{JobName: jobName, File: fname, Phase: phase, TaskNumber: task, NumOtherPhase: n_other}
						wg.Add(1)
						go func() {
							defer func() {
								wg.Done()
								registerChan <- ch // This line could block the code, so it should be after "wg.Done()"
							}()
							if !call(ch, "Worker.DoTask", req, nil) {
								taskChan <- task
								time.Sleep(100 * time.Millisecond)
							}
						}()
					}
				default:
					{
						wg.Wait()
						if len(taskChan) == 0 {
							break ProLoop
						}
					}
				}
			}
		}
	}
	// wg.Wait()

	/*	for i := 0; i < ntasks; i++ {
			var fname string
			switch phase {
			case mapPhase:
				fname = mapFiles[i]
			case reducePhase:
				fname = ""
			}
			req := DoTaskArgs{JobName: jobName, File: fname, Phase: phase, TaskNumber: i, NumOtherPhase: n_other}
			select {
			case ch := <-registerChan:
				{
					wg.Add(1)
					go func() {
						defer func() {
							wg.Done()
							registerChan <- ch
						}()
						if !call(ch, "Worker.DoTask", req, nil) {
							i -= 1
						}
					}()
				}
			}
		}
		wg.Wait()*/
	fmt.Printf("Schedule: %v phase done\n", phase)
}
