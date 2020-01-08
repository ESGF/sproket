# A list of 64-bit GOOS/GOARCH supported by go out of the box
# darwin/amd64
# dragonfly/amd64
# freebsd/amd64
# linux/amd64
# linux/arm64
# linux/ppc64
# linux/ppc64le
# linux/mips64
# linux/mips64le
# linux/s390x
# netbsd/amd64
# openbsd/amd64
# plan9/amd64
# solaris/amd64
# windows/amd64

GOOS=darwin go build -o build/sproket-darwin cmd/sproket/main.go
GOOS=linux go build -o build/sproket-linux cmd/sproket/main.go
GOOS=windows go build -o build/sproket-windows cmd/sproket/main.go
