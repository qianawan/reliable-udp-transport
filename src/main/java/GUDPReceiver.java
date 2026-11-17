import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Receives GUDP control and data packets. DATA and FIN packets are delivered
 * in order; cumulative ACKs report the last contiguous sequence number.
 */
public class GUDPReceiver extends Thread {
    private final DatagramSocket socket;
    private final ConcurrentLinkedQueue<GUDPEndPoint> receiverQueue;
    private final ConcurrentLinkedQueue<GUDPEndPoint> senderQueue;
    private volatile boolean running = true;

    public GUDPReceiver(
            DatagramSocket socket,
            ConcurrentLinkedQueue<GUDPEndPoint> receiverQueue,
            ConcurrentLinkedQueue<GUDPEndPoint> senderQueue) {
        this.socket = socket;
        this.receiverQueue = receiverQueue;
        this.senderQueue = senderQueue;
    }

    public void stopGUDPReceiver() {
        running = false;
    }

    @Override
    public void run() {
        byte[] buffer = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
        while (running) {
            DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(datagram);
                GUDPPacket packet = GUDPPacket.unpack(datagram);
                handle(packet);
            } catch (IOException | IllegalArgumentException e) {
                if (running) {
                    System.err.println("Ignoring invalid or unreadable GUDP packet: " + e.getMessage());
                }
            }
        }
    }

    private void handle(GUDPPacket packet) throws IOException {
        InetSocketAddress peer = packet.getSocketAddress();
        switch (packet.getType()) {
            case GUDPPacket.TYPE_ACK:
                handleAck(peer, packet.getSeqno());
                break;
            case GUDPPacket.TYPE_BSN:
                handleBsn(peer, packet);
                break;
            case GUDPPacket.TYPE_DATA:
                handleData(peer, packet, false);
                break;
            case GUDPPacket.TYPE_FIN:
                handleData(peer, packet, true);
                break;
            default:
                break;
        }
    }

    private void handleAck(InetSocketAddress peer, int ackedSequence) {
        GUDPEndPoint endpoint = find(senderQueue, peer);
        if (endpoint == null) {
            return;
        }

        synchronized (endpoint) {
            int base = endpoint.getBase();
            if (ackedSequence < base || ackedSequence > endpoint.getLast()) {
                return;
            }

            endpoint.removeAllACK(ackedSequence);
            endpoint.setBase(ackedSequence + 1);
            endpoint.setState(GUDPEndPoint.endPointState.RCV);
        }
    }

    private void handleBsn(InetSocketAddress peer, GUDPPacket packet) throws IOException {
        int sequence = packet.getSeqno();
        GUDPEndPoint endpoint = find(receiverQueue, peer);
        if (endpoint == null) {
            endpoint = new GUDPEndPoint(peer.getAddress(), peer.getPort());
            endpoint.setBase(sequence);
            endpoint.setExpectedseqnum(sequence + 1);
            receiverQueue.add(endpoint);
            sendAck(peer, sequence);
            return;
        }

        if (endpoint.getFinished()) {
            endpoint.clear();
            endpoint.setBase(sequence);
            endpoint.setExpectedseqnum(sequence + 1);
            sendAck(peer, sequence);
            return;
        }

        int lastContiguous = endpoint.getExpectedseqnum() - 1;
        sendAck(peer, sequence == lastContiguous ? sequence : lastContiguous);
    }

    private void handleData(InetSocketAddress peer, GUDPPacket packet, boolean finish)
            throws IOException {
        GUDPEndPoint endpoint = find(receiverQueue, peer);
        if (endpoint == null) {
            return;
        }

        int sequence = packet.getSeqno();
        int expected = endpoint.getExpectedseqnum();
        if (!endpoint.getFinished() && sequence == expected) {
            endpoint.add(packet);
            endpoint.setExpectedseqnum(expected + 1);
            if (finish) {
                endpoint.setFinished(true);
            }
            sendAck(peer, sequence);
        } else {
            sendAck(peer, expected - 1);
        }
    }

    private GUDPEndPoint find(
            ConcurrentLinkedQueue<GUDPEndPoint> queue,
            InetSocketAddress peer) {
        for (GUDPEndPoint endpoint : queue) {
            if (endpoint.getRemoteEndPoint().equals(peer)) {
                return endpoint;
            }
        }
        return null;
    }

    private void sendAck(InetSocketAddress peer, int sequence) throws IOException {
        GUDPPacket ack = new GUDPPacket(ByteBuffer.allocate(GUDPPacket.HEADER_SIZE));
        ack.setVersion(GUDPPacket.GUDP_VERSION);
        ack.setType(GUDPPacket.TYPE_ACK);
        ack.setSeqno(sequence);
        ack.setPayloadLength(0);
        ack.setSocketAddress(peer);
        socket.send(ack.pack());
    }
}
