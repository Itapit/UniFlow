package counter

import (
	"os"
)
func checkFile(name string)(){
	_,err:=os.Stat("../../../shared/counter/"+name)
	if(err!=nil){
		os.Create("../../../shared/counter/"+name)
	}
}