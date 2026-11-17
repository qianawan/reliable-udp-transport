import java.io.File;
import java.io.FileInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public final class GUDPFileSender {
    private GUDPFileSender() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: GUDPFileSender <host> <port> <input-file>");
            System.exit(2);
        }

        InetAddress host = InetAddress.getByName(args[0]);
        int port = Integer.parseInt(args[1]);
        File inputFile = new File(args[2]);
        if (!inputFile.isFile()) {
            throw new IllegalArgumentException("Input file does not exist: " + inputFile);
        }

        DatagramSocket transport = new DatagramSocket();
        GUDPSocket socket = new GUDPSocket(transport);
        InetSocketAddress destination = new InetSocketAddress(host, port);
        try {
            ByteBuffer metadata = ByteBuffer.allocate(Long.BYTES);
            metadata.putLong(inputFile.length());
            byte[] header = metadata.array();
            socket.send(new DatagramPacket(header, header.length, destination));

            byte[] buffer = new byte[GUDPPacket.MAX_DATA_LEN];
            try (FileInputStream input = new FileInputStream(inputFile)) {
                int length;
                while ((length = input.read(buffer)) != -1) {
                    if (length == 0) {
                        continue;
                    }
                    socket.send(new DatagramPacket(buffer, 0, length, destination));
                }
            }
            socket.finish();
            System.out.println("Sent " + inputFile.length() + " bytes to " + destination);
        } finally {
            socket.close();
        }
    }
}
