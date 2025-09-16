package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

func main() {
	test := "000000230022"
	decoded, _ := hex.DecodeString(test)
	fmt.Println(decoded)

	a := int32(binary.BigEndian.Uint32(decoded[:4]))
	b := int16(binary.BigEndian.Uint16(decoded[4:6]))

	fmt.Println(a)
	fmt.Println(b)
	fmt.Println(decoded)
}
