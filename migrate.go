package main

import (
  "github.com/garyburd/redigo/redis"
  "fmt"
  "flag"
)

var (
  mode string
  source = flag.String("source", "127.0.0.1:6379", "Redis server to pull data from")
  dest = flag.String("dest", "127.0.0.1:6379", "Redis server to send data to")
)


func main() {
  // parse the cli flags
  flag.Parse()
  
  // attempt to connect to source server
  fmt.Println("Attempting to connect to source server...")
  source_conn, err := redis.Dial("tcp", *source)
  if err != nil {
    fmt.Println(err)
  }

  // attempt to connect to destination server
  fmt.Println("Attempting to connect to destination server...")
  dest_conn, err := redis.Dial("tcp", *dest)
  if err != nil {
    fmt.Println(err)
  }

  // grab all source keys
  source_keys, err := redis.Strings(source_conn.Do("keys", "*"))
  if err != nil {
    fmt.Println(err)
  }

  // loop through source keys and restore them on the destination server
  for _, element := range source_keys {
    fmt.Println("Dumping", element)
    dumped_key, err := source_conn.Do("dump", element)
    dumped_key_ttl, err := redis.Int(source_conn.Do("pttl", element))
    if err != nil {
      fmt.Println(err)
    }
    fmt.Println(fmt.Sprintf("Restoring %v (TTL: %v)", element, dumped_key_ttl))
    dest_conn.Do("restore", element, dumped_key_ttl, dumped_key)
  }
}
