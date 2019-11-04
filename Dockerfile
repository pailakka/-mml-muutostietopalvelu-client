FROM golang:alpine

WORKDIR /go/src/app
COPY . .

RUN go build

CMD ["/go/src/app/mml-muutostietopalvelu-client"]