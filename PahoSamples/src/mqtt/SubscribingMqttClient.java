package mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class SubscribingMqttClient implements MqttCallback {

    public static void main(String[] args) {
        String topic = "labs/paho-topic";
        String brokerURI = "tcp://localhost:1883";
        String clientId = "myClientID_Sub";

        try {
            MqttClient mqttClient = new MqttClient(brokerURI, clientId, new MemoryPersistence());
            MqttConnectOptions connectOptions = new MqttConnectOptions();
            connectOptions.setCleanSession(true); // Set cleaning session

            System.out.println("Subscriber: Connecting to Mqtt Broker at " + brokerURI);
            mqttClient.connect(connectOptions);
            System.out.println("Subscriber: Successfully Connected.");

            // Subscribe to the topic
            mqttClient.setCallback(new SubscribingMqttClient());
            mqttClient.subscribe(topic, 0); // Set QoS

            System.out.println("Subscriber: Waiting for messages. Press Ctrl+C to exit.");

            // Keep the subscriber running
            while (true) {
                // Do nothing, let the callback handle incoming messages
                Thread.sleep(1000);
            }

        } catch (MqttException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("Connection to MQTT broker lost! Reconnecting...");
        // Reconnect logic can be added here
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        String payload = new String(message.getPayload());
        System.out.println("Received message on topic: " + topic);
        System.out.println("Message content: " + payload);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Not used in this example
    }
}

