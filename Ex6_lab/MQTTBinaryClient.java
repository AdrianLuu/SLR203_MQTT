import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.io.IOException;

public class MQTTBinaryClient {

    public static void main(String[] args) {
        try{
            Socket socket = new Socket("localhost", 1883); // Broker adress
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            // CONNECT Packet
            byte[] connectPacket = {
                0x10, // Command type: 0001  Control flags: 0000
                0x13, // Remaining length: 0001 0011 (19)
                0x00, 0x04, // Protocol name length - 4
                'M', 'Q', 'T', 'T', // Protocol name - MQTT
                0x04, // Protocol level - MQTT protocol version is 4
                0x02, // Connect flag: 0000 0010, the second bit represents Clean Session, which is true here
                0x00, 0x3c, // Keep alive timer: 0000 0000 0011 1100 - 60 Sec
                0x00, 0x03, // Client ID length - 3
                'A', 'd', 'i' // Client ID - Adi
            };

            // Calculate the Remaining Length
            int remainingLength = connectPacket.length - 2;
            connectPacket[1] = (byte) remainingLength;

            out.write(connectPacket); // Send CONNECT packet to Broker
  
            // Read CONNACK
            byte[] connackPacket = new byte[4];
            in.read(connackPacket);

            // Display the contents of the CONNACK message
            System.out.println("Received CONNACK byte: ");
            for (byte b : connackPacket) {
                System.out.printf("0x%02x ", b);
            }

            System.out.println();

            // PUBLISH Packet
            byte[] publishPacket = {
                0x32, // Command type: PUBLISH  Control flag: 1101 (DUP: 1, QoS: 2, Retain: 1)
                0x0a, // Remaining length
                0x00, 0x07, // Topic name length
                'm', 'q', 't', 't', 'l', 'a', 'b', // Topic name: mqttlab
                0x01, 0x02, // Message id
                0x48, 0x65, 0x79, 0x42, 'r', 'o' // Payload: HeyBro
            };

            // Calculate the Remaining Length
            int remainingLength2 = publishPacket.length - 2;
            publishPacket[1] = (byte) remainingLength2;

            out.write(publishPacket); // Send PUBLISH packet to Broker

            // Read PUBACK
            byte[] pubackPacket = new byte[4];
            in.read(pubackPacket);

            // Display the contents of the PUBACK message
            System.out.println("Received PUBACK byte: ");
            for (byte b : pubackPacket) {
                System.out.printf("0x%02x ", b);
            }

            socket.close();

        } catch (IOException e){
            e.printStackTrace();
        }
    }
}
