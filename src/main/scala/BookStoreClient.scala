import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue

import com.rabbitmq.client._

import scala.io.StdIn
import scala.util.control.Breaks._

object BookStoreClient {

  private val REQUEST_QUEUE_A = "AddBook"
  private val REQUEST_QUEUE_B = "GetBook"
  private val REQUEST_QUEUE_C = "GetBooks"

  private val RESPONSE_QUEUE = "BookStore Server Response"

  val addBookCorrId = UUID.randomUUID.toString
  val getBookCorrId = UUID.randomUUID.toString
  val getBooksCorrId = UUID.randomUUID.toString

  // to block the main thread until response is sent from the server
  val response = new ArrayBlockingQueue[String](3)

  def main(args: Array[String]): Unit = {

    val factory = new ConnectionFactory()
    factory.setHost("localhost")
    val connection = factory.newConnection()
    val channel = connection.createChannel()

    channel.queueDeclare(RESPONSE_QUEUE, false, false, false, null)
    channel.queueDeclare(REQUEST_QUEUE_A, false, false, false, null)
    channel.queueDeclare(REQUEST_QUEUE_B, false, false, false, null)
    channel.queueDeclare(REQUEST_QUEUE_C, false, false, false, null)

    // callback consumer - to consume the response sent from the server
    val consumer = declareCallBackConsumer(channel)

    try {
      breakable {
        while (true) {
          System.out.println("Choose operation - Add Book(A), Get Book(B), Get Books(C) ")
          val operation: String = StdIn.readLine().toUpperCase

          operation match {
            case "A" =>
              println("You have selected to add a book to the store")
              print("Please enter Book Id - ")
              val bookId: String = StdIn.readLine()
              print("Please enter Book Name - ")
              val bookName: String = StdIn.readLine()
              print("Please enter Book Type - ")
              val bookType: String = StdIn.readLine()
              // publish request to add book
              publishRequest(channel, REQUEST_QUEUE_A, RESPONSE_QUEUE, bookId + "," + bookName + "," + bookType, addBookCorrId)

            case "B" =>
              println("You have selected to get a book from the store")
              print("Please enter Book Id - ")
              val bookId: String = StdIn.readLine()
              // publish request to get a book
              publishRequest(channel, REQUEST_QUEUE_B, RESPONSE_QUEUE, bookId, getBookCorrId)

            case "C" =>
              println("You have selected to get books from the store")
              // publish request to get books
              publishRequest(channel, REQUEST_QUEUE_C, RESPONSE_QUEUE, "", getBooksCorrId)
          }

          println("Waiting for server response....")
          // consume the response published by the book store server
          channel.basicConsume(RESPONSE_QUEUE, true, consumer)
          println(response.take())
          println("")
          println("Press E to exit or C to continue")
          if (StdIn.readChar().toUpper == 'E') {
            break
          }
        }
      }
    }
    catch {
      case ex: Exception => println("Error occurred" + ex.printStackTrace())
    }

    channel.close()
    connection.close()
  }

  // publish the request to the book store server
  def publishRequest(channel: Channel, requestQueue: String, responseQueue: String, message: String, corrId: String): Unit = {
    val props = new AMQP.BasicProperties.Builder().correlationId(corrId).replyTo(responseQueue).build
    channel.basicPublish("", requestQueue, props, message.getBytes("UTF-8"))
  }

  def declareCallBackConsumer(channel: Channel): DefaultConsumer = {
    val consumer = new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
        if (properties.getCorrelationId == addBookCorrId) {
          val responseMsg = new String(body, "UTF-8")
          response.offer("\nAdd Book operation response\n---------------------------\n" + responseMsg)
        }
        else if (properties.getCorrelationId == getBookCorrId) {
          val responseMsg = new String(body, "UTF-8")
          response.offer("\nGet Book operation response\n---------------------------\n" + responseMsg)

        }
        else if (properties.getCorrelationId == getBooksCorrId) {
          val responseMsg = new String(body, "UTF-8")
          response.offer("\nGet Books operation response\n---------------------------\n" + responseMsg)
        }
      }
    }
    consumer
  }
}
