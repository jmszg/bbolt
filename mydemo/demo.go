package main

import (
	"fmt"
	"unsafe"
)

func main1() {
	//indexToRemove := 1
	//indexWhereToInsert := 4

	//slice := []int{0, 1, 2, 3, 4}
	//
	//slice = append(slice, 10)
	//fmt.Println("slice:", slice)
	//copy(slice[5+1:], slice[5:])
	//fmt.Println("slice:", slice)

	//slice = append(slice[:indexToRemove], slice[indexToRemove+1:]...)
	//fmt.Println("slice:", slice)
	//
	//newSlice := append(slice[:indexWhereToInsert], 1)
	//fmt.Println("newSlice:", newSlice)
	//
	//slice = append(newSlice, slice[indexWhereToInsert:]...)
	//fmt.Println("slice:", slice)

	var x struct {
		a bool
		b int32
		c []int
	}
	fmt.Println("SIZE")
	fmt.Println(unsafe.Sizeof(x))
	fmt.Println(unsafe.Sizeof(x.a))
	fmt.Println(unsafe.Sizeof(x.b))
	fmt.Println(unsafe.Sizeof(x.c))
	fmt.Println("ALIGN")
	fmt.Println(unsafe.Alignof(x))
	fmt.Println(unsafe.Alignof(x.a))
	fmt.Println(unsafe.Alignof(x.b))
	fmt.Println(unsafe.Alignof(x.c))
	fmt.Println("OFFSET")
	fmt.Println(unsafe.Offsetof(x.a))
	fmt.Println(unsafe.Offsetof(x.b))
	fmt.Println(unsafe.Offsetof(x.c))
}
