import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class GUDPIntegrationTest {
    private GUDPIntegrationTest() {
    }

    public static void main(String[] args) throws Exception {
        testPacketOffsetAndLength();
        testLoopbackTransferAfterDroppedData();
        System.out.println("All GUDP integration checks passed.");
    }

    private static void testPacketOffsetAndLength() throws Exception {
        byte[] source = "prefix-payload-suffix".getBytes("UTF-8");
        DatagramPacket outgoing = new DatagramPacket(
                source, 7, 7, InetAddress.getLoopbackAddress(), 8000);
        GUDPPacket packet = GUDPPacket.encapsulate(outgoing);
        packet.setSeqno(44);

        DatagramPacket wire = packet.pack();
        GUDPPacket decoded = GUDPPacket.unpack(wire);
        byte[] destination = new byte[20];
        DatagramPacket incoming = new DatagramPacket(destination, 3, 12);
        decoded.decapsulate(incoming);

        assertEquals("payload", new String(destination, 3, incoming.getLength(), "UTF-8"),
                "Datagram offset/length must be preserved");
        assertEquals(44, decoded.getSeqno(), "Sequence number must survive encode/decode");
    }

    private static void testLoopbackTransferAfterDroppedData() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        DatagramSocket serverTransport = new DatagramSocket(new InetSocketAddress(loopback, 0));
        DatagramSocket clientTransport = new DatagramSocket(new InetSocketAddress(loopback, 0));
        GUDPSocket server = new GUDPSocket(serverTransport);
        GUDPSocket client = new GUDPSocket(clientTransport);
        InetSocketAddress serverAddress =
                new InetSocketAddress(loopback, serverTransport.getLocalPort());
        DropFirstDataProxy proxy = new DropFirstDataProxy(serverAddress);
        AtomicReference<Throwable> receiverFailure = new AtomicReference<Throwable>();
        AtomicReference<byte[]> firstPayload = new AtomicReference<byte[]>();
        AtomicReference<byte[]> secondPayload = new AtomicReference<byte[]>();

        Thread applicationReceiver = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    DatagramPacket first = new DatagramPacket(
                            new byte[GUDPPacket.MAX_DATA_LEN], GUDPPacket.MAX_DATA_LEN);
                    server.receive(first);
                    firstPayload.set(Arrays.copyOfRange(
                            first.getData(), first.getOffset(), first.getOffset() + first.getLength()));

                    DatagramPacket second = new DatagramPacket(
                            new byte[GUDPPacket.MAX_DATA_LEN], GUDPPacket.MAX_DATA_LEN);
                    server.receive(second);
                    secondPayload.set(Arrays.copyOfRange(
                            second.getData(), second.getOffset(), second.getOffset() + second.getLength()));

                    DatagramPacket fin = new DatagramPacket(
                            new byte[GUDPPacket.MAX_DATA_LEN], GUDPPacket.MAX_DATA_LEN);
                    server.receive(fin);
                    if (fin.getLength() != 0) {
                        throw new AssertionError("Expected FIN to be delivered as an empty datagram");
                    }
                } catch (Throwable e) {
                    receiverFailure.set(e);
                }
            }
        }, "GUDP application receiver");
        applicationReceiver.setDaemon(true);
        applicationReceiver.start();

        try {
            byte[] first = new byte[GUDPPacket.MAX_DATA_LEN];
            for (int i = 0; i < first.length; i++) {
                first[i] = (byte) (i % 251);
            }
            byte[] second = "windowed GUDP payload".getBytes("UTF-8");
            client.send(new DatagramPacket(first, first.length, proxy.getAddress(), proxy.getPort()));
            client.send(new DatagramPacket(second, second.length, proxy.getAddress(), proxy.getPort()));
            client.finish();

            applicationReceiver.join(12000);
            assertFalse(applicationReceiver.isAlive(), "Receiver must finish after FIN");
            if (receiverFailure.get() != null) {
                throw new AssertionError("Receiver thread failed", receiverFailure.get());
            }
            assertTrue(Arrays.equals(first, firstPayload.get()), "1000-byte payload must arrive intact");
            assertTrue(Arrays.equals(second, secondPayload.get()), "Second payload must arrive in order");
            assertTrue(proxy.droppedData.get(), "Proxy must have dropped a DATA packet");
        } finally {
            client.close();
            server.close();
            proxy.close();
        }
    }

    private static void assertEquals(Object expected, Object actual, String message) {
        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new AssertionError(message + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    private static void assertFalse(boolean value, String message) {
        if (value) {
            throw new AssertionError(message);
        }
    }

    private static void assertTrue(boolean value, String message) {
        if (!value) {
            throw new AssertionError(message);
        }
    }

    private static final class DropFirstDataProxy implements AutoCloseable {
        private final DatagramSocket socket;
        private final InetSocketAddress server;
        private final AtomicBoolean droppedData = new AtomicBoolean();
        private volatile InetSocketAddress client;
        private volatile boolean running = true;
        private final Thread thread;

        DropFirstDataProxy(InetSocketAddress server) throws SocketException {
            this.server = server;
            this.socket = new DatagramSocket(new InetSocketAddress(
                    InetAddress.getLoopbackAddress(), 0));
            this.socket.setSoTimeout(250);
            this.thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    relay();
                }
            }, "GUDP test proxy");
            this.thread.setDaemon(true);
            this.thread.start();
        }

        InetAddress getAddress() {
            return socket.getLocalAddress();
        }

        int getPort() {
            return socket.getLocalPort();
        }

        private void relay() {
            byte[] buffer = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
            while (running) {
                DatagramPacket received = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(received);
                    InetSocketAddress source = (InetSocketAddress) received.getSocketAddress();
                    if (source.equals(server)) {
                        InetSocketAddress destination = client;
                        if (destination != null) {
                            socket.send(new DatagramPacket(
                                    received.getData(), received.getOffset(), received.getLength(),
                                    destination));
                        }
                    } else {
                        client = source;
                        if (received.getLength() >= GUDPPacket.HEADER_SIZE
                                && ByteBuffer.wrap(received.getData(), received.getOffset(),
                                        received.getLength()).getShort(2) == GUDPPacket.TYPE_DATA
                                && droppedData.compareAndSet(false, true)) {
                            continue;
                        }
                        socket.send(new DatagramPacket(
                                received.getData(), received.getOffset(), received.getLength(),
                                server));
                    }
                } catch (SocketTimeoutException ignored) {
                    // Poll running so close can end the relay thread.
                } catch (Exception e) {
                    if (running) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        @Override
        public void close() {
            running = false;
            socket.close();
            try {
                thread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
