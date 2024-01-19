package mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import static java.lang.Thread.sleep;

public class PublishingSensorClient {//synchronous client

    public static void main(String[] args) {

        String topic1 = "/home/Lyon/sido/sht30/value";
        String topic2 = "/home/Lyon/sido/sht30/value2";
        String topic3 = "/home/Lyon/sido/dht22/value";
        String topic4 = "/home/Lyon/sido/dht22/value2";

        String[] topics = {topic1, topic2, topic3, topic4};

        int qos = args[0].equals("0") ? 0 : args[0].equals("1") ? 1 : 2;
        boolean cleanSession = args[1].equals("true");
        boolean retained = args[2].equals("true");
        String brokerURI = "tcp://192.168.109.75:1883"; // Enter the IP which runs Broker
        String clientId = "Adi_sensors";
        // MemoryPersistence persistence = new MemoryPersistence();


        try (
            ////instantiate a synchronous MQTT Client to connect to the targeted Mqtt Broker
            MqttClient mqttClient = new MqttClient(brokerURI, clientId)) {
            
            // instantiate a monitorAverage to receive the average data
            MonitorAverage monitorAverage = new MonitorAverage(brokerURI, "Adi", topics);
            
            ////specify the Mqtt Client's connection options
            MqttConnectOptions connectOptions = new MqttConnectOptions();
            //clean session
            connectOptions.setCleanSession(cleanSession);

            ////connect the mqtt client to the broker
            System.out.println("Mqtt Client: Connecting to Mqtt Broker running at: " + brokerURI);
            mqttClient.connect(connectOptions);
            System.out.println("Mqtt Client: sucessfully Connected.");

            ////publish a message
            for(int k = 0; k < 100; k++) {
                    int randomInt = (int) (Math.random() * 10000);
                    String messageContent = String.valueOf(randomInt);
                    System.out.println("Mqtt Client: Publishing message: " + messageContent + " to topic: " + topics[3]);
                    MqttMessage message = new MqttMessage(messageContent.getBytes());//instantiate the message including its content (payload)
                    message.setQos(qos);//set the message's QoS
                    message.setRetained(retained);//set the message's retained flag
                    mqttClient.publish(topics[3], message);//publish the message to a given topic
                    System.out.println("Mqtt Client: successfully published the message.");
                    System.out.println();
                sleep(1000);
            }
            ////disconnect the Mqtt Client
            mqttClient.disconnect();
            System.out.println("Mqtt Client: Disconnected.");

        } catch (MqttException e) {
            System.out.println("Mqtt Exception reason: " + e.getReasonCode());
            System.out.println("Mqtt Exception message: " + e.getMessage());
            System.out.println("Mqtt Exception location: " + e.getLocalizedMessage());
            System.out.println("Mqtt Exception cause: " + e.getCause());
            System.out.println("Mqtt Exception reason: " + e);
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}