package main

import (
	"datastore/server"
	"fmt"
	"os"
	"os/signal"
	"time"
)

func main() {
	running := true
	dataServer := server.New("localhost", 8888)
	err := dataServer.Start()

	if err != nil {
		println(err)
		return
	}

	defer dataServer.Stop()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)
	go func() {
		for sgl := range c {
			fmt.Printf("Recieved signal %q, shutting down\n", sgl.String())
			running = false
		}
	}()

	for running {
		time.Sleep(time.Second * 1)
	}
}
