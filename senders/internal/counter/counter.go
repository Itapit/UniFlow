package counter

import (
	"encoding/binary"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func Count(counterFile *os.File)(uint64,error){

	h := windows.Handle(counterFile.Fd())
	var ol windows.Overlapped


	err := windows.LockFileEx(h, 2, 0, 8, 0, &ol)
	if err != nil {
		return 0, fmt.Errorf("failed to lock file: %w", err)
	}
	defer windows.UnlockFileEx(h, 0, 8, 0, &ol)
	buf:=make([]byte,8)
	var currentCount uint64

	counterFile.Seek(0,0)
	_,err=counterFile.Read(buf)
	
	binary.Decode(buf,binary.LittleEndian,&currentCount)
	if err!=nil{
		return 0,fmt.Errorf("%w",err)
	}
	nextCount:=currentCount+1
	binary.Encode(buf,binary.LittleEndian,nextCount)

	counterFile.Seek(0,0)
	_,err=counterFile.Write(buf)

	if err!=nil{
		return currentCount,fmt.Errorf("%w",err)
	}
	
	return currentCount,nil
}