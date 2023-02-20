package bbolt

import (
	"fmt"
	"testing"
	"unsafe"
)

func Test_unsafeAdd(t *testing.T) {
	myslice := make([]int, 4)
	fmt.Println(&myslice[0])
	fmt.Println(&myslice[3])
	fmt.Println(unsafe.Sizeof(myslice[0]))
	first := &myslice[0]
	offset := unsafe.Sizeof(myslice[0]) * 2
	address := unsafeAdd(unsafe.Pointer(first), uintptr(offset))
	fmt.Println(address)
}

func Test_unsafeIndex(t *testing.T) {
	myslice := []int{1, 2, 3, 5, 6, 7, 8, 9, 10}
	fmt.Println(&myslice[0])
	fmt.Println(&myslice[3])
	address := unsafeIndex(unsafe.Pointer(&myslice[0]), 16, unsafe.Sizeof(int(0)), 2)
	fmt.Println(address)
	fmt.Println(uintptr(address))
	var testslice = make([]int, 2)
	copy(testslice, (*[5]int)(address)[0:1:5])
	fmt.Println(testslice)
}

func Test_unsafeByteSlice(t *testing.T) {
	myslice := []int{1, 2, 3, 5, 6, 7, 8, 9, 10}
	result := unsafeByteSlice(unsafe.Pointer(&myslice[0]), 2, 1, 4)
	fmt.Println(result)
	// test的len=4-1， cap = 6-1
	test := myslice[1:4:6]
	fmt.Println(test)
}
