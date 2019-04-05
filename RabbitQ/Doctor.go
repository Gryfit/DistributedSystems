package main

import (
	"bufio"
	"github.com/streadway/amqp"
	"log"
	"os"
	"strings"
	"encoding/json"
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
func consumer(docId string,msg amqp.Delivery, ch *amqp.Channel) {
	println("Received Message from tech")
	interaction(docId, ch)
}

func interaction(docId string, ch *amqp.Channel){
	reader := bufio.NewReader(os.Stdin)
	println("Patient injury: ")
	injury, _ := reader.ReadString('\n')
	injury = strings.TrimSuffix(injury, "\n")

	println("Patient name: ")
	name, _ := reader.ReadString('\n')
	name = strings.TrimSuffix(name, "\n")

	body, err := json.Marshal(Message{injury, name, docId})
	failOnError(err, "Failed to publish a message")
	err = ch.Publish(
		"main",
		"app.tech."+injury,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
	failOnError(err, "Failed to publish a message")
	log.Printf("Send injury")

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

	docQ, err := ch.QueueDeclare(
		os.Args[1],
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue 1")

	err = ch.QueueBind(
		docQ.Name, // queue name
		"app.doc." + os.Args[1],     // routing key
		"main", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue 2")

	msgs1, err := ch.Consume(
		docQ.Name, // queue
		os.Args[1],     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer on q1")

	interaction(os.Args[1], ch)

	for{
		select {
		case msg := <-msgs1:
			go consumer(os.Args[1], msg, ch)
		}
	}

}