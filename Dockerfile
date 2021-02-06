FROM golang:1-alpine
RUN  apk add git
ADD . /go/src/github.com/maxim-kuderko/coeus

WORKDIR /go/src/github.com/maxim-kuderko/coeus/cmd
RUN  go get ./... && go build -o coeus main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=0 /go/src/github.com/maxim-kuderko/coeus/cmd/coeus .
EXPOSE 8080

CMD ["/root/coeus"]
