package bbolt

import (
	"reflect"
	"unsafe"
)

// 这个文件主要提供指针的偏移操作，这些操作在go里面认为是不安全的，而且只有unsafa提供了指针的操作

// 这个地方表示返回base指针偏移offset的地址(基地址+偏移量)
func unsafeAdd(base unsafe.Pointer, offset uintptr) unsafe.Pointer {
	// 任何类型都可以被转换为unsafe.Pointer
	// unsafe.Pointer可以被转化为任何类型的指针
	// uintptr可以被转化为unsafe.Ponter
	// unsafe.Ponter可以被转化为uintptr
	return unsafe.Pointer(uintptr(base) + offset)
}

// 返回元素索引位置的地址(基地址+偏移量+元素大小*个数)
func unsafeIndex(base unsafe.Pointer, offset uintptr, elemsz uintptr, n int) unsafe.Pointer {
	return unsafe.Pointer(uintptr(base) + offset + uintptr(n)*elemsz)
}

// 根据地址偏移量和数据值返回一个初始化的切片
func unsafeByteSlice(base unsafe.Pointer, offset uintptr, i, j int) []byte {
	// See: https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	//
	// This memory is not allocated from C, but it is unmanaged by Go's
	// garbage collector and should behave similarly, and the compiler
	// should produce similar code.  Note that this conversion allows a
	// subslice to begin after the base address, with an optional offset,
	// while the URL above does not cover this case and only slices from
	// index 0.  However, the wiki never says that the address must be to
	// the beginning of a C allocation (or even that malloc was used at
	// all), so this is believed to be correct.
	// 安全字节切片，统一切片指针类型
	return (*[maxAllocSize]byte)(unsafeAdd(base, offset))[i:j:j]
}

// unsafeSlice modifies the data, len, and cap of a slice variable pointed to by
// the slice parameter.  This helper should be used over other direct
// manipulation of reflect.SliceHeader to prevent misuse, namely, converting
// from reflect.SliceHeader to a Go slice type.
func unsafeSlice(slice, data unsafe.Pointer, len int) {
	// 转换为切片类型指针
	s := (*reflect.SliceHeader)(slice)
	s.Data = uintptr(data)
	s.Cap = len
	s.Len = len
}
