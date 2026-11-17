import java.io.FileOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public final class GUDPFileReceiver {
    private GUDPFileReceiver() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: GUDPFileReceiver <port> <output-file>");
            System.exit(2);
        }

        int port = Integer.parseInt(args[0]);
        DatagramSocket transport = new DatagramSocket(new InetSocketAddress(port));
        GUDPSocket socket = new GUDPSocket(transport);
        long receivedBytes = 0;
        try (FileOutputStream output = new FileOutputStream(args[1])) {
            DatagramPacket packet = new DatagramPacket(
                    new byte[GUDPPacket.MAX_DATA_LEN], GUDPPacket.MAX_DATA_LEN);
            socket.receive(packet);
            if (packet.getLength() != Long.BYTES) {
                throw new IllegalStateException("Missing 8-byte file-length header");
            }
            long remaining = ByteBuffer.wrap(
                    packet.getData(), packet.getOffset(), packet.getLength()).getLong();
            if (remaining < 0) {
                throw new IllegalStateException("Invalid file length");
            }

            while (remaining > 0) {
                packet.setLength(packet.getData().length);
                socket.receive(packet);
                int length = packet.getLength();
                if (length == 0 || length > remaining) {
                    throw new IllegalStateException("Unexpected datagram before expected file length");
                }
                output.write(packet.getData(), packet.getOffset(), length);
                receivedBytes += length;
                remaining -= length;
            }

            packet.setLength(packet.getData().length);
            socket.receive(packet);
            if (packet.getLength() != 0) {
                throw new IllegalStateException("Expected FIN after file contents");
            }
            output.flush();
            System.out.println("Received " + receivedBytes + " bytes into " + args[1]);
        } finally {
            socket.close();
        }
    }
}
