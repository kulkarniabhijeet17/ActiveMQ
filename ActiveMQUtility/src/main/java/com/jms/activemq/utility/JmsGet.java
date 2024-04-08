package com.jms.activemq.utility;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static org.apache.activemq.ActiveMQConnection.DEFAULT_BROKER_URL;

public class JmsGet {
    private static int status = 1;
    private static final String CLIENTID = "JMSActiveMQ";
    private static ConnectionFactory connectionFactory;
    private static Connection connection;
    private static Session session;
    private static Destination destination;
    private static MessageConsumer consumer;
    private static String queueName;

    public static void main(String[] args) {
        establishConnection();
        receiveMessage(destination);
        close();
        System.exit(status);
    }

    private static void establishConnection() {
        // URL of the JMS server is required to create connection factory.
        // DEFAULT_BROKER_URL is : tcp://localhost:61616 and is indicates that JMS
        // server is running on localhost
        connectionFactory = new ActiveMQConnectionFactory(DEFAULT_BROKER_URL);
        try {
            Properties prop = loadProperties();

            // Getting JMS connection from the server and starting it
            connection = connectionFactory.createConnection();
            connection.setClientID(CLIENTID);
            connection.start();
            // Creating a non-transactional session to send/receive JMS message.
            session = connection.createSession(false, AUTO_ACKNOWLEDGE);
            queueName = prop.getProperty("mq.queue");
            // Destination represents here our queue ’MyFirstActiveMQ’ on the JMS
            // server.
            // The queue will be created automatically on the JSM server if its not already
            // created.
            destination = session.createQueue(queueName);
        } catch (JMSException e) {
            System.out.println("JMSException occurred" + Arrays.toString(e.getStackTrace()));
        } catch (IOException e) {
            System.out.println("IOException occurred" + Arrays.toString(e.getStackTrace()));
        }
    }

    private static void receiveMessage(Destination destination) {
        try {
            // MessageConsumer is used for receiving (consuming) messages from the queue.
            consumer = session.createConsumer(destination);
            // receive the message from the queue.
            Message message = consumer.receive();
            // Since We are using TestMessage in our example. MessageProducer sent us a TextMessage
            // So we need cast to it to get access to its getText() method which will give us the text of the message
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                System.out.printf("Received message '%s' from the queue '%s' running on local JMS Server.\n", textMessage.getText(), queueName);
            }
        } catch (JMSException e) {
            System.out.println("JMSException occurred" + Arrays.toString(e.getStackTrace()));
        }
    }

    private static Properties loadProperties() throws IOException {
        InputStream inputStream = JmsPut.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();

        if (inputStream == null) {
            System.out.println("Unable to find config.properties file");
            return null;
        }

        prop.load(inputStream);
        return prop;
    }

    public static void close() {
        try {
            consumer.close();
            consumer = null;
            session.close();
            session = null;
            connection.close();
            connection = null;
        } catch (JMSException e) {
            System.out.println("JMSException occurred" + Arrays.toString(e.getStackTrace()));
        }
    }
}
