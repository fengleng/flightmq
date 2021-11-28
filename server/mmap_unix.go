// +build linux

package server

//func mmap(q *queue, size int) error {
//	data, err := syscall.Mmap(int(q.file.Fd()), 0, size, syscall.PROT_WRITE, syscall.MAP_SHARED)
//	if err != nil {
//		return fmt.Errorf("mmap %v.queue failed, %v", q.name, err)
//	}
//
//	q.data = data
//	return nil
//}
//
//func unmap(q *queue) error {
//	err := syscall.Munmap(q.data)
//	q.data = nil
//
//	if err != nil {
//		return fmt.Errorf("unmap %v.queue failed. %v", q.name, err)
//	} else {
//		return nil
//	}
//}

func mmap(fd uintptr, size int64) {
	b, err := syscall.Mmap(int(demo.file.Fd()), 0, defaultMemMapSize, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	dataRef = b
	demo.data = (*[defaultMaxFileSize]byte)(unsafe.Pointer(&b[0]))
}
