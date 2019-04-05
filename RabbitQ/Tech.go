package main

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"encoding/json"
	"time"
)
type Message struct {
	Injury string
	Name string
	DocId string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
func consumer(raw amqp.Delivery, ch *amqp.Channel) {
	var msg Message
	err := json.Unmarshal(raw.Body, &msg)
	failOnError(err, "Failed to unmarshal a message")

	time.Sleep(time.Second * 3)


	body := msg.Name + msg.Injury + "done"
	err = ch.Publish(
		"main",
		"app.doc."+string(msg.DocId),
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf("Send response to " + msg.DocId)

	err = ch.Publish(
		"main",
		"log",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish log")
	log.Printf("Sent log")
}
//./tech [injury1] [injury2]
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

	q1, err := ch.QueueDeclare(
		os.Args[1],
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue 1")

	err = ch.QueueBind(
		q1.Name, // queue name
		"app.tech." + os.Args[1],     // routing key
		"main", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue 2")

	msgs1, err := ch.Consume(
		q1.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer on q1")

	q2, err := ch.QueueDeclare(
		os.Args[2],
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue 2")

	err = ch.QueueBind(
		q2.Name, // queue name
		"app.tech." + os.Args[2],     // routing key
		"main", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue 2")

	msgs2, err := ch.Consume(
		q2.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	for{
		select {
		case msg := <-msgs1:
			go consumer(msg, ch)
		case msg := <-msgs2:
			go consumer(msg, ch)
		}
	}

}