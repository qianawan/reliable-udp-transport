import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * The GUDP wire packet: version (2 bytes), type (2 bytes), sequence number
 * (4 bytes), followed by an optional payload.
 */
public class GUDPPacket {
    public static final short GUDP_VERSION = 1;
    public static final short HEADER_SIZE = 8;
    public static final int MAX_DATA_LEN = 1000;
    public static final int MAX_DATAGRAM_LEN = MAX_DATA_LEN + HEADER_SIZE;
    public static final int MAX_WINDOW_SIZE = 3;
    public static final short TYPE_DATA = 1;
    public static final short TYPE_BSN = 2;
    public static final short TYPE_ACK = 3;
    public static final short TYPE_FIN = 4;

    private final ByteBuffer byteBuffer;
    private InetSocketAddress socketAddress;
    private int payloadLength;

    public GUDPPacket(ByteBuffer buffer) {
        if (buffer == null || buffer.capacity() < HEADER_SIZE) {
            throw new IllegalArgumentException("GUDP buffer must hold the 8-byte header");
        }
        this.byteBuffer = buffer.order(ByteOrder.BIG_ENDIAN);
    }

    public static GUDPPacket encapsulate(DatagramPacket packet) throws IOException {
        if (packet == null) {
            throw new IOException("Cannot encapsulate a null datagram");
        }
        if (!(packet.getSocketAddress() instanceof InetSocketAddress)) {
            throw new IOException("GUDP requires an InetSocketAddress destination");
        }
        InetSocketAddress address = (InetSocketAddress) packet.getSocketAddress();
        int length = packet.getLength();
        if (length < 0 || length > MAX_DATA_LEN) {
            throw new IOException("Payload exceeds the GUDP maximum of " + MAX_DATA_LEN + " bytes");
        }

        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + length).order(ByteOrder.BIG_ENDIAN);
        GUDPPacket result = new GUDPPacket(buffer);
        result.setVersion(GUDP_VERSION);
        result.setType(TYPE_DATA);
        result.setSocketAddress(address);
        result.setPayload(packet.getData(), packet.getOffset(), length);
        return result;
    }

    public void decapsulate(DatagramPacket packet) throws IOException {
        if (packet == null) {
            throw new IOException("Cannot decapsulate into a null datagram");
        }
        int offset = packet.getOffset();
        int length = getPayloadLength();
        byte[] destination = packet.getData();
        if (length < 0 || offset < 0 || offset + length > destination.length) {
            throw new IOException("Destination datagram buffer is too small for GUDP payload");
        }
        getPayload(destination, offset, length);
        packet.setLength(length);
        if (socketAddress != null) {
            packet.setSocketAddress(socketAddress);
        }
    }

    public static GUDPPacket unpack(DatagramPacket packet) throws IOException {
        if (packet == null) {
            throw new IOException("Cannot unpack a null datagram");
        }
        int offset = packet.getOffset();
        int length = packet.getLength();
        if (length < HEADER_SIZE || length > MAX_DATAGRAM_LEN) {
            throw new IOException("Invalid GUDP packet length: " + length);
        }

        byte[] packetBytes = Arrays.copyOfRange(packet.getData(), offset, offset + length);
        GUDPPacket result = new GUDPPacket(ByteBuffer.wrap(packetBytes).order(ByteOrder.BIG_ENDIAN));
        result.payloadLength = length - HEADER_SIZE;
        if (!(packet.getSocketAddress() instanceof InetSocketAddress)) {
            throw new IOException("GUDP requires an InetSocketAddress source");
        }
        InetSocketAddress address = (InetSocketAddress) packet.getSocketAddress();
        result.socketAddress = address;

        if (result.getVersion() != GUDP_VERSION) {
            throw new IOException("Unsupported GUDP version: " + result.getVersion());
        }
        short type = result.getType();
        if (type < TYPE_DATA || type > TYPE_FIN) {
            throw new IOException("Unknown GUDP packet type: " + type);
        }
        if (type != TYPE_DATA && result.payloadLength != 0) {
            throw new IOException("Control packets must not carry a payload");
        }
        return result;
    }

    public DatagramPacket pack() throws IOException {
        if (socketAddress == null) {
            throw new IOException("GUDP packet has no destination address");
        }
        int length = HEADER_SIZE + getPayloadLength();
        if (length > byteBuffer.capacity()) {
            throw new IOException("GUDP packet length exceeds its backing buffer");
        }
        return new DatagramPacket(getBytes(), length, socketAddress);
    }

    public byte[] getBytes() {
        int length = HEADER_SIZE + payloadLength;
        if (length > byteBuffer.capacity()) {
            throw new IllegalStateException("GUDP packet length exceeds its backing buffer");
        }
        return Arrays.copyOf(byteBuffer.array(), length);
    }

    public short getVersion() {
        return byteBuffer.getShort(0);
    }

    public short getType() {
        return byteBuffer.getShort(2);
    }

    public int getSeqno() {
        return byteBuffer.getInt(4);
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public void setVersion(short version) {
        byteBuffer.putShort(0, version);
    }

    public void setType(short type) {
        byteBuffer.putShort(2, type);
    }

    public void setSeqno(int seqno) {
        byteBuffer.putInt(4, seqno);
    }

    public void setSocketAddress(InetSocketAddress socketAddress) {
        this.socketAddress = socketAddress;
    }

    public void setPayload(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        setPayload(payload, 0, payload.length);
    }

    public void setPayload(byte[] payload, int offset, int length) {
        if (payload == null || offset < 0 || length < 0 || offset + length > payload.length
                || length > MAX_DATA_LEN || HEADER_SIZE + length > byteBuffer.capacity()) {
            throw new IllegalArgumentException("Invalid GUDP payload range or length");
        }
        byteBuffer.position(HEADER_SIZE);
        byteBuffer.put(payload, offset, length);
        payloadLength = length;
    }

    public void setPayloadLength(int length) {
        if (length < 0 || length > MAX_DATA_LEN || HEADER_SIZE + length > byteBuffer.capacity()) {
            throw new IllegalArgumentException("Invalid GUDP payload length: " + length);
        }
        payloadLength = length;
    }

    public int getPayloadLength() {
        return payloadLength;
    }

    public void getPayload(byte[] destination, int length) {
        getPayload(destination, 0, length);
    }

    public void getPayload(byte[] destination, int offset, int length) {
        if (destination == null || offset < 0 || length < 0 || offset + length > destination.length
                || length > payloadLength) {
            throw new IllegalArgumentException("Invalid GUDP payload destination range");
        }
        ByteBuffer copy = byteBuffer.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
        copy.position(HEADER_SIZE);
        copy.get(destination, offset, length);
    }
}
