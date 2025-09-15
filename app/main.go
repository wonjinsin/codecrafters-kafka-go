package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

type Request struct {
	Message int32
	Header  *RequestHeader
	Body    *RequestBody
}

func NewRequest(b []byte) *Request {
	r := new(Request)
	r.Message = int32(binary.BigEndian.Uint32(b[:4]))

	header, _ := NewRequestHeader(b[4:])
	r.Header = header

	return r
}

type RequestHeader struct {
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
	ClientID          *RequestHeaderClientID
	TagBuffer         int8
}

func NewRequestHeader(b []byte) (r *RequestHeader, lastIndex int32) {
	r = new(RequestHeader)
	r.RequestAPIKey = int16(binary.BigEndian.Uint16(b[:2]))
	r.RequestAPIVersion = int16(binary.BigEndian.Uint16(b[2:4]))
	r.CorrelationID = int32(binary.BigEndian.Uint32(b[4:8]))
	lastIdx := int32(8)

	clientID, idx := NewRequestClientID(b[8:])
	r.ClientID = clientID
	lastIdx += idx

	r.TagBuffer = int8(b[lastIdx])

	return r, lastIndex + 1
}

type RequestHeaderClientID struct {
	Length   int16
	Contents string
}

func NewRequestClientID(b []byte) (r *RequestHeaderClientID, lastIdx int32) {
	r = new(RequestHeaderClientID)
	r.Length = int16(binary.BigEndian.Uint16(b[:2]))
	if r.Length == 0 {
		return r, 2
	}
	r.Contents = string(b[2:r.Length])
	return r, int32(2 + r.Length)
}

type RequestBody struct {
	ClientID        RequestBodyClientID
	SoftwareVersion RequestBodySoftwareVersion
	TagBuffer       int8
}

type RequestBodyClientID struct {
	Length   int8
	Contents string
}

type RequestBodySoftwareVersion struct {
	Length   int8
	Contents string
}

type Response struct {
	Message int32
	Header  *ResponseHeader
	Body    *ResponseBody
}

func NewResponse(r *Request) *Response {
	errorCode := int16(0)
	if r.Header.RequestAPIVersion > 4 {
		errorCode = 32
	}
	return &Response{
		Message: 33,
		Header: &ResponseHeader{
			CorrelationID: r.Header.CorrelationID,
		},
		Body: &ResponseBody{
			ErrorCode: errorCode,
			VersionsArray: ResponseBodyVersionsArray{
				Length: 3,
				Versions: ResponseBodyVersions{
					&ResponseBodyVersion{},
					&ResponseBodyVersion{},
					&ResponseBodyVersion{},
				},
			},
		},
	}
}

func (r Response) Parse() []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, r.Message)
	binary.Write(&buf, binary.BigEndian, r.Header.CorrelationID)
	binary.Write(&buf, binary.BigEndian, r.Body.ErrorCode)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Length)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Versions[0].APIKey)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Versions[0].MinVersion)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Versions[0].MaxVersion)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Versions[0].TagBuffer)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Versions[1].APIKey)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Versions[1].MinVersion)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Versions[1].MaxVersion)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Versions[1].TagBuffer)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Versions[2].APIKey)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Versions[2].MinVersion)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Versions[2].MaxVersion)
	binary.Write(&buf, binary.BigEndian, r.Body.VersionsArray.Versions[2].TagBuffer)
	binary.Write(&buf, binary.BigEndian, r.Body.ThrottleTime)
	binary.Write(&buf, binary.BigEndian, r.Body.TagBuffer)
	return buf.Bytes()
}

type ResponseHeader struct {
	CorrelationID int32
}

type ResponseBody struct {
	ErrorCode     int16
	VersionsArray ResponseBodyVersionsArray
	ThrottleTime  int32
	TagBuffer     int8
}

type ResponseBodyVersionsArray struct {
	Length   int8
	Versions ResponseBodyVersions
}

type ResponseBodyVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
	TagBuffer  int8
}

type ResponseBodyVersions []*ResponseBodyVersion

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

		request := NewRequest(buffer[:n])
		response := NewResponse(request)
		conn.Write(response.Parse())
		fmt.Println("write called")
	}
}
