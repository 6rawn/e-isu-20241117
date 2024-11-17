all: app

app: *.go go.mod go.sum
	GOARCH=amd64 GOOS=linux go build -o app
