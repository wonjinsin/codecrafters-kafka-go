package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"

	v0 "github.com/codecrafters-io/kafka-starter-go/app/api/v0"
	v4 "github.com/codecrafters-io/kafka-starter-go/app/api/v4"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go response(conn)
	}
}

func response(conn net.Conn) {
	defer conn.Close()

	for {
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil && err == io.EOF {
			break
		}

		apiKey := binary.BigEndian.Uint16(buffer[4:6])
		var parsed []byte
		if apiKey == 75 {
			parsed = v0.Parse(buffer[:n])
		} else {
			parsed = v4.Parse(buffer[:n])
		}

		conn.Write(parsed)
		fmt.Println("write called")
	}
}
