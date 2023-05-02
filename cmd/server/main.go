package main

import (
	"log"

	"git.zantwi.ch/zantwich/jeans/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
