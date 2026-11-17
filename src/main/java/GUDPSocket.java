import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Application-facing reliable datagram socket using a Go-Back-N sender.
 */
public class GUDPSocket implements GUDPSocketAPI {
    private final DatagramSocket datagramSocket;
    private final ConcurrentLinkedQueue<GUDPEndPoint> senderQueue =
            new ConcurrentLinkedQueue<GUDPEndPoint>();
    private final ConcurrentLinkedQueue<GUDPEndPoint> receiverQueue =
            new ConcurrentLinkedQueue<GUDPEndPoint>();
    private final GUDPSender sender;
    private final GUDPReceiver receiver;
    private final Object senderLock = new Object();
    private boolean senderStarted;
    private boolean receiverStarted;
    private boolean finishCalled;

    public GUDPSocket(DatagramSocket socket) {
        if (socket == null || socket.isClosed()) {
            throw new IllegalArgumentException("GUDP requires an open DatagramSocket");
        }
        datagramSocket = socket;
        sender = new GUDPSender(socket, senderQueue);
        sender.setName("GUDPSender");
        receiver = new GUDPReceiver(socket, receiverQueue, senderQueue);
        receiver.setName("GUDPReceiver");
    }

    @Override
    public void send(DatagramPacket packet) throws IOException {
        if (datagramSocket.isClosed()) {
            throw new IOException("GUDP socket is closed");
        }
        GUDPPacket dataPacket = GUDPPacket.encapsulate(packet);
        InetSocketAddress destination = dataPacket.getSocketAddress();

        synchronized (senderLock) {
            if (finishCalled) {
                throw new IOException("Cannot send after GUDP finish()");
            }
            GUDPEndPoint endpoint = find(senderQueue, destination);
            if (endpoint == null) {
                endpoint = new GUDPEndPoint(destination.getAddress(), destination.getPort());
                int baseSequence = 2 + (int) (Math.random() * 9998);

                GUDPPacket bsn = new GUDPPacket(ByteBuffer.allocate(GUDPPacket.HEADER_SIZE));
                bsn.setVersion(GUDPPacket.GUDP_VERSION);
                bsn.setType(GUDPPacket.TYPE_BSN);
                bsn.setSeqno(baseSequence);
                bsn.setSocketAddress(destination);
                bsn.setPayloadLength(0);

                endpoint.setBase(baseSequence);
                endpoint.setNextseqnum(baseSequence);
                endpoint.setLast(baseSequence);
                endpoint.add(bsn);
                senderQueue.add(endpoint);
            }

            int sequence = endpoint.getLast() + 1;
            dataPacket.setSeqno(sequence);
            endpoint.setLast(sequence);
            endpoint.add(dataPacket);
        }

        startSenderIfNeeded();
        startReceiverIfNeeded();
    }

    @Override
    public void receive(DatagramPacket packet) throws IOException {
        if (packet == null) {
            throw new IOException("Destination datagram cannot be null");
        }
        startReceiverIfNeeded();

        while (true) {
            if (datagramSocket.isClosed()) {
                throw new IOException("GUDP socket is closed");
            }
            for (GUDPEndPoint endpoint : receiverQueue) {
                GUDPPacket received = endpoint.remove();
                if (received != null) {
                    received.decapsulate(packet);
                    return;
                }
            }
            sleep(50, "Interrupted while waiting for a GUDP packet");
        }
    }

    @Override
    public void finish() throws IOException {
        synchronized (senderLock) {
            finishCalled = true;
            for (GUDPEndPoint endpoint : senderQueue) {
                if (endpoint.getFinished()) {
                    continue;
                }

                int sequence = endpoint.getLast() + 1;
                GUDPPacket fin = new GUDPPacket(ByteBuffer.allocate(GUDPPacket.HEADER_SIZE));
                fin.setVersion(GUDPPacket.GUDP_VERSION);
                fin.setType(GUDPPacket.TYPE_FIN);
                fin.setSeqno(sequence);
                fin.setSocketAddress(endpoint.getRemoteEndPoint());
                fin.setPayloadLength(0);

                endpoint.setLast(sequence);
                endpoint.setFinished(true);
                endpoint.add(fin);
            }
        }

        if (senderQueue.isEmpty()) {
            return;
        }
        startSenderIfNeeded();
        startReceiverIfNeeded();

        while (!senderQueue.isEmpty()) {
            if (!sender.isAlive()) {
                throw new IOException("GUDP sender stopped before all packets were acknowledged");
            }
            sleep(50, "Interrupted while waiting for GUDP transmission to finish");
        }
        if (sender.getFailure() != null) {
            throw sender.getFailure();
        }
    }

    @Override
    public void close() throws IOException {
        sender.stopGUDPSender();
        receiver.stopGUDPReceiver();
        datagramSocket.close();
        try {
            sender.join(1000);
            receiver.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing GUDP threads", e);
        }
    }

    private synchronized void startSenderIfNeeded() {
        if (!senderStarted) {
            sender.start();
            senderStarted = true;
        }
    }

    private synchronized void startReceiverIfNeeded() {
        if (!receiverStarted) {
            receiver.start();
            receiverStarted = true;
        }
    }

    private GUDPEndPoint find(
            ConcurrentLinkedQueue<GUDPEndPoint> queue,
            InetSocketAddress destination) {
        for (GUDPEndPoint endpoint : queue) {
            if (endpoint.getRemoteEndPoint().equals(destination)) {
                return endpoint;
            }
        }
        return null;
    }

    private void sleep(long millis, String message) throws IOException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(message, e);
        }
    }
}
