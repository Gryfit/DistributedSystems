package main

import (
	"bufio"
	"github.com/streadway/amqp"
	"log"
	"os"
	"strings"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

//./doctor [doc_id]
func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"main",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"log",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue ")

	err = ch.QueueBind(
		q.Name, // queue name
		"log",     // routing key
		"main", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue ")

	msgs, err := ch.Consume(
		q.Name, // queue
		"logger",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer on q")
	go func() {
		for{
			select {
			case msg := <-msgs:
				log.Print(msg)
			}
		}
	}()
	for{
		reader := bufio.NewReader(os.Stdin)
		println("Message to all: ")
		rawMsg, _ := reader.ReadString('\n')
		body := strings.TrimSuffix(rawMsg, "\n")

		err := ch.Publish(
			"main",
			"app.#",
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		failOnError(err, "Failed to publish a message")
		log.Printf("Sent to all")
	}

}