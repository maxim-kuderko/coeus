FROM golang:1-buster
#RUN  apk add git gcc-go
ADD . /go/src/github.com/maxim-kuderko/coeus

WORKDIR /go/src/github.com/maxim-kuderko/coeus/cmd
ENV CGO_ENABLED=1
RUN  go get  ./... && go  build -o coeus ./...

FROM debian:latest
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates

RUN update-ca-certificates
#RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=0 /go/src/github.com/maxim-kuderko/coeus/cmd/coeus .
EXPOSE 8080

CMD ["/root/coeus"]
