# Reliable UDP Transport

A Java implementation of a small reliable transport layer over UDP. It uses a Go-Back-N sender, cumulative acknowledgements, retransmission timers, and a three-packet send window.

## Features

- 8-byte, big-endian GUDP header with version, packet type, and sequence number
- BSN, DATA, ACK, and FIN control/data packet types
- In-order delivery with cumulative acknowledgements
- Go-Back-N retransmission after timeout
- Configurable endpoint timeout and retry limits
- File sender and receiver examples
- Loopback integration test that drops a DATA packet and checks Go-Back-N retransmission

## Build and test

Requires JDK 9 or newer. The project compiles to Java 8 bytecode.

~~~sh
./scripts/test.sh
~~~

The test uses only the local loopback interface. It does not contact external hosts.

## File transfer demo

Start a receiver:

~~~sh
java -cp build/classes GUDPFileReceiver 9000 received.bin
~~~

Send a file to it:

~~~sh
java -cp build/classes GUDPFileSender 127.0.0.1 9000 input.bin
~~~

Compile the library and examples first:

~~~sh
mkdir -p build/classes
find src/main/java examples -name '*.java' -print > build/sources.txt
javac --release 8 -d build/classes @build/sources.txt
~~~

The file sender starts with an 8-byte big-endian file-length header; subsequent packets carry file bytes.

The receiver treats an empty datagram delivered by FIN as end-of-file. Because the small GUDPSocketAPI does not expose a separate close-notification event, an empty application datagram is reserved and cannot be distinguished from FIN.

## Protocol limits

- Maximum application payload: 1000 bytes per datagram
- Send window: 3 packets
- Initial retransmission timeout: 3 seconds
- Maximum retransmissions: 9
- Sequence-number wraparound is not implemented
- The transport has no encryption, authentication, congestion control, or checksum beyond UDP's checksum

## Source and contribution notes

This project uses the GUDP starter files supplied in GUDP.tgz. Its sender, receiver, and socket files were incomplete teaching skeletons. The user-supplied GUDPSocket.java and GUDPSender.java are the implementation starting points for this repository, and the repository versions include further modifications. The receiver implementation, packet-boundary validation, integration test, file examples, and project packaging were added for this repository. Review and understand the code before describing individual contributions on a CV.

The duplicate 1-GUDPSender.java backup is omitted. Compared with that backup, GUDPSender.java includes a guard for a finished endpoint with an empty queue and stops an existing timer before restarting it; this project uses GUDPSender.java as its starting variant. The loopback test exercises a dropped DATA packet and a successful Go-Back-N retransmission, but it does not prove correctness for every network condition or protocol edge case.

The original iCloud files were not modified.
