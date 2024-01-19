package mqtt;

import java.util.HashMap;
import java.util.Map;

// Run from bin with 'java -cp .:../../TP-3-4/pahoSamples/src/org.eclipse.paho.client.mqttv3-1.2.5.jar monitor.C_group128'
// Compile from bin with 'javac -cp .:../../TP-3-4/pahoSamples/src/org.eclipse.paho.client.mqttv3-1.2.5.jar ../src/monitor/*.java -d .'
//added external jar: c:\ada\work\lectures\slr203\mqtt\paho\paho-java-maven\org.eclipse.paho.client.mqttv3-1.2.5.jar 

import org.eclipse.paho.client.mqttv3.*;


public class MonitorAverage extends Thread implements MqttCallback{//asynchronous client
	private transient MqttClient group_client;
	private Map<String, Integer> topicAverage				= new HashMap<String, Integer>();
	
		
	public MonitorAverage(String brokerURI, String clientId, String... topics) {
		for (String topic: topics) {
			topicAverage.put(topic + "/average", 0);
		}
		
		// Create and connect the client
		// Also subscribe it to the topics
		spawn(brokerURI, clientId, topics);
	}

	private void spawn(String brokerURI, String clientId, String... topics) {
		try {
            MqttClient mqttClient = new MqttClient(brokerURI, clientId);
            MqttConnectOptions connectOptions = new MqttConnectOptions();
            // Clean session
            connectOptions.setCleanSession(false);
                        
            // Connect the mqtt client to the broker
            mqttClient.connect(connectOptions);
            System.out.println("Average monitor client: sucessfully Connected.");
            
			// Set the client
			this.group_client = mqttClient;
			
			// Subscribe it to the topics provided
			for (String topic: topics) {
				group_client.subscribe(topic);
				group_client.subscribe(topic + "/average");
			}
			
			// Set this object as callback for message receival
			group_client.setCallback(this);
            
        } catch (MqttException e) {
            System.out.println("Mqtt Exception reason: " + e.getReasonCode());
			System.out.println("Mqtt Exception message: " + e.getMessage());
			System.out.println("Mqtt Exception location: " + e.getLocalizedMessage());
			System.out.println("Mqtt Exception cause: " + e.getCause());
			System.out.println("Mqtt Exception reason: " + e);
			e.printStackTrace();
        }
	}

	@Override
	public void connectionLost(Throwable cause) {
		cause.getCause();
		cause.printStackTrace();
		System.out.println("Too bad, connection lost !!!");
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		if (topic.endsWith("/average")) {
			float val = Float.parseFloat(new String(message.getPayload()));
			int old_val = topicAverage.get(topic);
			System.out.println("Difference from mean (" 
							+ topic.substring(0, topic.length() - "/average".length()) + "): " 
							+ (val - old_val));
		} else {
			int val = Integer.parseInt(new String(message.getPayload()));
			topicAverage.put(topic, val);
		}
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {}
}
	