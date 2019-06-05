package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	fmt.Printf("Reduce: job name = %s, output file = %s, reduce task id = %d, nMap = %d\n",
		jobName, outFile, reduceTask, nMap);
	kvMap := make(map[string]([]string))//kvMap是用来存储 reduce键值的
	keys := make([]string,0,100)		//只存储值
	// 根据有多少个执行Map的Worker决定
	for i:=0;i<nMap;i++ {
		//得到中间文件
		filename := reduceName(jobName,i, reduceTask)
		f, err := os.Open(filename)
		if(err!=nil){
			log.Fatal("can not open the file:", filename)
		}
		defer f.Close()
		//decode中间文件
		decode := json.NewDecoder(f)
		var kv KeyValue
		for ;decode.More();{
			err := decode.Decode(&kv)
			if(err!=nil){
				log.Fatal("can not decode")
			}
			//将kv的建值加到kvMap中
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}
	// 将所有的键放在一起方便排序
	for k,_ := range(kvMap){
		keys = append(keys, k)
	}
	sort.Strings(keys)

	//导出输出文件
	f, err := os.Create(outFile)
	if (err != nil) {
		log.Fatal("can not create file")
	}
	defer f.Close()
	for _,key := range(keys) {
		encode := json.NewEncoder(f)
		encode.Encode(KeyValue{key, reduceF(key,kvMap[key])})
	}





	f, err = os.Create("./1.txt")
	if (err != nil) {
		log.Fatal("can not create file")
	}
	defer f.Close()
	for _,key := range(keys) {

		encode := json.NewEncoder(f)
		encode.Encode(KeyValue{key, reduceF(key,kvMap[key])})
	}
	//
	//doReduce manages one reduce task: it should read the intermediate
	//files for the task, sort the intermediate key/value pairs by key,
	//call the user-defined reduce function (reduceF) for each key, and
	//write reduceF's output to disk.
	//
	//You'll need to read one intermediate file from each map task;
	//reduceName(jobName, m, reduceTask) yields the file
	//name from map task m.
	//
	//Your doMap() encoded the key/value pairs in the intermediate
	//files, so you will need to decode them. If you used JSON, you can
	//read and decode by creating a decoder and repeatedly calling
	//.Decode(&kv) on it until it returns an error.
	//
	//You may find the first example in the golang sort package
	//documentation useful.
	//
	//reduceF() is the application's reduce function. You should
	//call it once per distinct key, with a slice of all the values
	//for that key. reduceF() returns the reduced value for that key.
	//
	//You should write the reduce output as JSON encoded KeyValue
	//objects to the file named outFile. We require you to use JSON
	//because that is what the merger than combines the output
	//from all the reduce tasks expects. There is nothing special about
	//JSON -- it is just the marshalling format we chose to use. Your
	//output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	//for i:=0;i<nMap;i++ {
	//	s := reduceName(jobName,i , reduceTask)
	//	file, err := os.Open(s)
	//	outfile,_= os.Create(outFile)
	//	if err==nil {
	//		enc := json.NewDecoder(file)
	//		for key := enc
	//		error2 := enc.Decode(KeyValue{key, reduceF(key, )})
	//		if(error2!=nil){break}
	//		_ = file.Close()
	//	}
	//}
	//kv_map := make(map[string]([]string))
	//
	//for i:=0;i<nMap;i++{
	//	// generate the name of the intermediate file
	//	filename := reduceName(jobName,i,reduceTask)
	//	// open the file
	//	f, err := os.Open(filename)
	//	if(err!=nil){
	//		log.Fatal("can not open file")
	//	}
	//	defer f.Close()
	//
	//	// decode
	//	decoder := json.NewDecoder(f)
	//	var kv KeyValue
	//	for ;decoder.More();{
	//		err := decoder.Decode(&kv)
	//		if(err!=nil){
	//			log.Fatal("json decode failed", err)
	//		}
	//		// append to the end
	//		kv_map[kv.Key] = append(kv_map[kv.Key], kv.Value)
	//	}
	//}
	//
	//keys := make([]string, 0, len(kv_map))
	//for k,_ := range kv_map {
	//	keys = append(keys, k)
	//}
	//sort.Strings(keys)
	//
	//outf, err:=os.Create(outFile)
	//if(err!=nil){
	//	log.Fatal("can not create file")
	//}
	//defer outf.Close()
	//encoder := json.NewEncoder(outf)
	//for _,k := range keys {
	//	encoder.Encode(KeyValue{k, reduceF(k, kv_map[k])})
	//}
	//outf, err=os.Create("./1.txt")
	//if(err!=nil){
	//	log.Fatal("can not create file")
	//}
	//defer outf.Close()
	//encoder = json.NewEncoder(outf)
	//for _,k := range keys {
	//	encoder.Encode(KeyValue{k, reduceF(k, kv_map[k])})
	//}
}
