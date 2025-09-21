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

	header, lastIdx := NewRequestHeader(b[4:])
	r.Header = header

	r.Body = NewRequestBody(b[lastIdx+5:])
	return r
}

type RequestHeader struct {
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
	ClientID          *RequestHeaderClientID
	TagBuffer         int8
}

func NewRequestHeader(b []byte) (*RequestHeader, int32) {
	r := new(RequestHeader)
	r.RequestAPIKey = int16(binary.BigEndian.Uint16(b[:2]))
	r.RequestAPIVersion = int16(binary.BigEndian.Uint16(b[2:4]))
	r.CorrelationID = int32(binary.BigEndian.Uint32(b[4:8]))
	nextIdx := int32(7 + 1)

	clientID, idx := NewRequestClientID(b[nextIdx:])
	r.ClientID = clientID
	nextIdx += idx + 1

	r.TagBuffer = int8(b[nextIdx])

	return r, nextIdx
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
	r.Contents = string(b[2 : r.Length+2])
	return r, int32(r.Length + 2 - 1)
}

type RequestBody struct {
	TopicsArray  *RequestTopicArray
	PartionLimit int32
	Cursor       *int8
	TagBuffer    int8
}

func NewRequestBody(b []byte) (r *RequestBody) {
	r = new(RequestBody)
	topicArray, lastIdx := NewRequestTopicArray(b)
	r.TopicsArray = topicArray
	r.PartionLimit = int32(binary.BigEndian.Uint32(b[lastIdx+1 : lastIdx+5]))
	var cursor *int8
	if int8(b[lastIdx+5]) >= 0 {
		c := int8(b[lastIdx+5])
		cursor = &c
	}
	r.Cursor = cursor
	r.TagBuffer = int8(b[lastIdx+6])
	return r
}

type RequestTopicArray struct {
	Length int8
	Topics RequestTopics
}

func NewRequestTopicArray(b []byte) (r *RequestTopicArray, lastIdx int32) {
	r = new(RequestTopicArray)
	r.Length = int8(b[0])
	r.Topics, lastIdx = NewRequestTopics(r.Length-1, b[1:])
	return r, lastIdx
}

type RequestTopic struct {
	NameLength int8
	Name       string
	TagBuffer  int8
}

func NewRequestTopic(b []byte) (r *RequestTopic, lastIdx int32) {
	r = new(RequestTopic)
	r.NameLength = int8(b[0])
	r.Name = string(b[1:r.NameLength])
	r.TagBuffer = int8(b[r.NameLength])
	return r, int32(r.NameLength)
}

type RequestTopics []*RequestTopic

func NewRequestTopics(length int8, b []byte) (r RequestTopics, lastIdx int32) {
	nextIdx := int32(0)
	for range length {
		t, topicIdx := NewRequestTopic(b[nextIdx:])
		r = append(r, t)
		nextIdx += topicIdx + 1
	}
	return r, nextIdx
}

type Response struct {
	Message int32
	Header  *ResponseHeader
	Body    *ResponseBody
}

func NewResponse(r *Request) *Response {
	return &Response{
		Message: 41,
		Header: &ResponseHeader{
			CorrelationID: r.Header.CorrelationID,
			TagBuffer:     0,
		},
		Body: &ResponseBody{
			ThrottleTime: 41,
			TopicsArray: ResponseBodyTopicsArray{
				Length: 2,
				Topics: ResponseBodyTopics{
					&ResponseBodyTopic{
						ErrorCode: 3,
						TopicName: &ResponseBodyTopicName{
							Length:   4,
							Contents: "Foo",
						},
						TopicID:                   [16]byte{},
						IsInternal:                0,
						PartitionsArray:           1,
						TopicAuthorizedOperations: 3576,
						TagBuffer:                 0,
					},
				},
			},
			NextCursor: -1,
			TagBuffer:  0,
		},
	}
}

func (r Response) Parse() []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, r.Message)
	binary.Write(&buf, binary.BigEndian, r.Header.CorrelationID)
	binary.Write(&buf, binary.BigEndian, r.Header.TagBuffer)
	binary.Write(&buf, binary.BigEndian, r.Body.ThrottleTime)
	binary.Write(&buf, binary.BigEndian, r.Body.TopicsArray.Length)
	binary.Write(&buf, binary.BigEndian, r.Body.TopicsArray.Topics[0].ErrorCode)
	binary.Write(&buf, binary.BigEndian, r.Body.TopicsArray.Topics[0].TopicName.Length)
	buf.Write([]byte(r.Body.TopicsArray.Topics[0].TopicName.Contents))
	buf.Write(r.Body.TopicsArray.Topics[0].TopicID[:])
	binary.Write(&buf, binary.BigEndian, r.Body.TopicsArray.Topics[0].IsInternal)
	binary.Write(&buf, binary.BigEndian, r.Body.TopicsArray.Topics[0].PartitionsArray)
	binary.Write(&buf, binary.BigEndian, r.Body.TopicsArray.Topics[0].TopicAuthorizedOperations)
	binary.Write(&buf, binary.BigEndian, r.Body.TopicsArray.Topics[0].TagBuffer)
	binary.Write(&buf, binary.BigEndian, r.Body.NextCursor)
	binary.Write(&buf, binary.BigEndian, r.Body.TagBuffer)
	return buf.Bytes()
}

type ResponseHeader struct {
	CorrelationID int32
	TagBuffer     int8
}

type ResponseBody struct {
	ThrottleTime int32
	TopicsArray  ResponseBodyTopicsArray
	NextCursor   int8
	TagBuffer    int8
}

type ResponseBodyTopicsArray struct {
	Length int8
	Topics ResponseBodyTopics
}

type ResponseBodyTopic struct {
	ErrorCode                 int16
	TopicName                 *ResponseBodyTopicName
	TopicID                   [16]byte
	IsInternal                int8
	PartitionsArray           int8
	TopicAuthorizedOperations int32
	TagBuffer                 int8
}

type ResponseBodyTopics []*ResponseBodyTopic

type ResponseBodyTopicName struct {
	Length   int32
	Contents string
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

		request := NewRequest(buffer[:n])
		fmt.Print(request)
		// response := NewResponse(request)
		// conn.Write(response.Parse())
		fmt.Println("write called")
	}
}
