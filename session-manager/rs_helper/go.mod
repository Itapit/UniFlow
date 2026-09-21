module rs_helper

go 1.27.1

require (
	common v0.0.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/reedsolomon v1.14.2 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace common => ../../common
