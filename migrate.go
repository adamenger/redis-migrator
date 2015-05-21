package main

import (
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"sync"
)

var (
	mode     string
	source   = flag.String("source", "127.0.0.1:6379", "Redis server to pull data from")
	dest     = flag.String("dest", "127.0.0.1:6379", "Redis server to send data to")
	scanners = flag.Int("scanners", 1, "Count of scanners to spin up")
	workers  = flag.Int("workers", 2, "Count of workers to spin up")
)

type RedisObject struct {
	name       string
	ttl        int64
	serialized string
}

type Task struct {
	list []string
}

func dump_and_restore(source_conn redis.Conn, dest_conn redis.Conn, key string, wg sync.WaitGroup) {
	dumped_key, err := redis.String(source_conn.Do("dump", key))
	dumped_key_ttl, err := redis.Int64(source_conn.Do("pttl", key))
	if err != nil {
		fmt.Println(err)
	}
	dest_conn.Do("restore", key, dumped_key_ttl, dumped_key)
}

func source_connection(source string) redis.Conn {
	// attempt to connect to source server
	source_conn, err := redis.Dial("tcp", source)
	if err != nil {
		fmt.Println(err)
	}
	return source_conn
}

func dest_connection(dest string) redis.Conn {
	// attempt to connect to source server
	dest_conn, err := redis.Dial("tcp", dest)
	if err != nil {
		fmt.Println(err)
	}
	return dest_conn
}

func main() {
	// parse the cli flags
	flag.Parse()

	// grab all source keys
	var cursor int64
	var returned_keys []string
	var wg sync.WaitGroup
	work_queue := make(chan Task, *workers)

	// Start scanner

	wg.Add(1)
	for i := 0; i <= *scanners; i++ {
		go func(cursor int64, wg sync.WaitGroup, work_queue chan Task) {
			source_conn := source_connection(*source)

			defer wg.Done()

			// Initial redis scan
			source_keys, _ := redis.Values(source_conn.Do("scan", "0", "count", "10"))
			redis.Scan(source_keys, &cursor, &returned_keys)

			var next_cursor int64
			var tmp_keys []string
			for {
				reply, _ := redis.Values(source_conn.Do("scan", cursor, "count", "10"))
				redis.Scan(reply, &next_cursor, &tmp_keys)
				if cursor == 0 {
					break
				}
				// Set the cursor to the next page
				work_queue <- Task{list: tmp_keys}
				cursor = next_cursor
			}
		}(cursor, wg, work_queue)
	}

	for i := 0; i <= *workers; i++ {
		wg.Add(1)
		go func(wg sync.WaitGroup) {
			source_conn := source_connection(*source)
			dest_conn := dest_connection(*dest)
			for task := range work_queue {
				for _, key := range task.list {
					dump_and_restore(source_conn, dest_conn, key, wg)
				}
			}
			wg.Done()
		}(wg)
	}
	wg.Wait()
}
