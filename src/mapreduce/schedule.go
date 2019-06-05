package mapreduce

import (
	"fmt"
	"sync"
)

//
//schedule() starts and waits for all tasks in the given phase (mapPhase
//or reducePhase). the mapFiles argument holds the names of the files that
//are the inputs to the map phase, one per map task. nReduce is the
//number of reduce tasks.
// the registerChan argument yields a stream of registered workers;
// each item is the worker's RPC address
//suitable for passing to call(). registerChan will yield all
//existing registered workers (if any) and new ones as they register.

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	fmt.Println("\nin schedule")
	fmt.Printf("jobname = %s, len = %d,len2 = %d,reduce = %d, phase = %s\n\n\n",jobName, len(mapFiles),len(registerChan),nReduce, phase)
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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	//辣是真的牛皮这个

	//使用waitGroup来实现并发
	var wg sync.WaitGroup
	for index := 0;index<ntasks;index++{
		// 调度一个map或reduce就要+1
		wg.Add(1)
		var dotaskArgs DoTaskArgs
		dotaskArgs.JobName=jobName
		dotaskArgs.NumOtherPhase = n_other
		dotaskArgs.Phase = phase
		//fmt.Printf("the chan len = %d",len(registerChan))

		//fmt.Printf("the register name is %s\n",i)
		if(phase==mapPhase){
			dotaskArgs.File = mapFiles[index]
		}
		dotaskArgs.TaskNumber = index
		// 牛逼的来了
		go func(){
			defer wg.Done()
			//  当出错时 ，无限循环调度，直到出现某一个worker不出错完成任务  break
			for;; {
				i := <-registerChan
				if (phase == reducePhase) {
					fmt.Printf("\nhaha\n%s\n", i)
				}
				ok := call(i, "Worker.DoTask", &dotaskArgs, nil)
				if ok == false {
					fmt.Printf("Worker wrong %s", phase)
				}else {
					// 为什么要开一个新的go程  因为channel必须在不同的线程之间进行传输
					go func() { registerChan <- i }()
					break
				}
			}
			//close(ch)
		}()
	}
	fmt.Printf("Schedule: %v done\n", phase)
	wg.Wait()
}
