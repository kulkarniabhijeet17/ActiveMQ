package com.jms.activemq.utility;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static org.apache.activemq.ActiveMQConnection.DEFAULT_BROKER_URL;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class JmsPut {
    private static int status = 1;
    private static final String CLIENTID = "JMSActiveMQ";
    private static ConnectionFactory connectionFactory;
    private static Connection connection;
    private static Session session;
    private static Destination destination;
    private static MessageProducer producer;

    public static void main(String[] args) {
        establishConnection();
        sendMessage(destination);
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
            // Destination represents here our queue ’MyFirstActiveMQ’ on the JMS
            // server.
            // The queue will be created automatically on the JSM server if its not already
            // created.
            destination = session.createQueue(prop.getProperty("mq.queue"));
        } catch (JMSException e) {
            System.out.println("JMSException occurred" + Arrays.toString(e.getStackTrace()));
        } catch (IOException e) {
            System.out.println("IOException occurred" + Arrays.toString(e.getStackTrace()));
        }
    }

    private static void sendMessage(Destination destination) {
        TextMessage message;
        try {
            Properties prop = loadProperties();
            File folder = new File(prop.getProperty("mq.inputFilePath"));
            for (final File fileEntry : folder.listFiles()) {
                if (fileEntry.isFile() && fileEntry.getName().endsWith(".input")) {
                    try (InputStream input = new FileInputStream(folder.getPath().replace('\\', '/') + "/" + fileEntry.getName())) {
                        Scanner s = new Scanner(input).useDelimiter("\\A");
                        String plainText = s.hasNext() ? s.next() : "";
                        System.out.println("Pushed String = " + plainText);
                        // We will send a text message
                        message = session.createTextMessage(plainText);
                        message.setText(plainText);
                        System.out.println("Pushed String Length = " + plainText.length());
                        // MessageProducer is used for sending (producing) messages to the queue.
                        producer = session.createProducer(destination);
                        // push the message into queue
                        producer.send(message);
                        System.out.println("Sent message:\n " + message);
                        System.out.printf("'%s' text message sent to the queue '%s' running on local JMS Server.\n", message);
                    } catch (Exception e) {
                        System.out.println("Exception occurred");
                    }
                }
            }
        } catch (IOException e) {

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
            producer.close();
            producer = null;
            session.close();
            session = null;
            connection.close();
            connection = null;
        } catch (JMSException e) {
            System.out.println("JMSException occurred" + Arrays.toString(e.getStackTrace()));
        }
    }
}