package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

type RequestV2 struct {
	Message int32
	Header  *RequestHeaderV2
}

type RequestHeaderV2 struct {
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
}

type ResponseV2 struct {
	Message int32
	Header  *ResponseHeaderV2
}

type ResponseHeaderV2 struct {
	CorrelationID int32
}

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

		reader := bytes.NewReader(buffer[:n])

		request := new(RequestV2)
		requestHeader := new(RequestHeaderV2)
		request.Header = requestHeader
		binary.Read(reader, binary.BigEndian, &request.Message)
		binary.Read(reader, binary.BigEndian, &requestHeader.RequestAPIKey)
		binary.Read(reader, binary.BigEndian, &requestHeader.RequestAPIVersion)
		binary.Read(reader, binary.BigEndian, &requestHeader.CorrelationID)

		response := new(ResponseV2)
		responseHeader := new(ResponseHeaderV2)
		response.Header = responseHeader
		response.Header.CorrelationID = request.Header.CorrelationID

		binary.Write(conn, binary.BigEndian, response.Message)
		binary.Write(conn, binary.BigEndian, response.Header.CorrelationID)
		binary.Write(conn, binary.BigEndian, int16(35))

		fmt.Println("write called")

	}
}
