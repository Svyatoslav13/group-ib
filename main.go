package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	errorNotFound      = errors.New("Message not found")
	errorEmptyMessage  = errors.New("Empty message")
	errorBadRequestURI = errors.New("Bad request URI")
)

func main() {
	port := os.Args[1]
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		log.Fatal("Port number must be a number")
	}

	mux := http.NewServeMux()

	broker := &Broker{
		queues: make(map[string]chan string),
	}

	mux.Handle("/", queueHandler(broker))
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(portNumber), mux))
}

type Broker struct {
	sync.Mutex
	queues map[string]chan string
}

func (b *Broker) GetMessage(queueName string, timeout time.Duration) (string, error) {
	if timeout > 0 {
		time.Sleep(timeout * time.Second)
	}

	queue := b.getQueue(queueName)

	select {
	case msg := <-queue:
		return msg, nil
	default:
		return "", errorNotFound
	}
}

func (b *Broker) PutMessage(queueName string, value string) bool {
	queue := b.getQueue(queueName)

	select {
	case queue <- value:
		return true
	default:
		return false
	}
}

func (b *Broker) getQueue(name string) chan string {
	b.Lock()
	queue, found := b.queues[name]
	b.Unlock()

	if !found {
		queue = make(chan string, 100)
		b.queues[name] = queue
	}

	return queue
}

func queueHandler(broker *Broker) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		queueName, err := validateQueueName(r.RequestURI)
		if err != nil {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}

		switch r.Method {
		case "PUT":
			value, err := validateMessage(r.URL.Query().Get("v"))
			if err != nil {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			_ = broker.PutMessage(queueName, value)
			rw.WriteHeader(http.StatusOK)
		case "GET":
			timeout := validateTimeout(r.URL.Query().Get("timeout"))
			msg, err := broker.GetMessage(queueName, timeout)
			if err != nil {
				rw.WriteHeader(http.StatusNotFound)
				return
			}

			rw.WriteHeader(http.StatusOK)
			rw.Write([]byte(msg))
		default:
			rw.WriteHeader(http.StatusBadRequest)
			rw.Write([]byte("unsupported http method"))
		}

		return
	})
}

func validateTimeout(val string) time.Duration {
	timeout, err := strconv.Atoi(val)
	if err != nil || timeout <= 0 {
		return 0
	}

	return time.Duration(timeout)
}

func validateMessage(val string) (string, error) {
	if val == "" {
		return "", errorEmptyMessage
	}

	return val, nil
}

func validateQueueName(uri string) (string, error) {
	urlParts := strings.Split(uri, "/")

	if len(urlParts) != 2 || urlParts[1] == "" {
		return "", errorBadRequestURI
	}

	return strings.Split(urlParts[1], "?")[0], nil
}
