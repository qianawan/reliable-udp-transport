import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Iterator;

/*
 * GUDPSender monitors send queues and sends packets whenever
 * there are packets in the queues
 */
public class GUDPSender extends Thread {

    private final DatagramSocket sock;
    private final ConcurrentLinkedQueue<GUDPEndPoint> senderQueue;

    private volatile boolean runFlag = true;
    private boolean debug = false;
    private volatile IOException failure;

    public GUDPSender(
            DatagramSocket sock,
            ConcurrentLinkedQueue<GUDPEndPoint> senderQueue) {

        this.sock = sock;
        this.senderQueue = senderQueue;
    }


    /*
     * Stop the sender thread
     */
    public void stopGUDPSender() {

        this.runFlag = false;

        // Wake sender immediately if it is sleeping
        this.interrupt();
    }

    public IOException getFailure() {
        return failure;
    }


    @Override
    public void run() {

        while (this.runFlag) {

            Iterator<GUDPEndPoint> iter =
                    senderQueue.iterator();

            while (this.runFlag && iter.hasNext()) {

                GUDPEndPoint endPoint = iter.next();

                /*
                 * Execute one FSM step
                 */
                synchronized (endPoint) {
                    FSMSender(endPoint);
                }

                /*
                 * Transmission to this endpoint is finished
                 * and all packets have been ACKed.
                 */
                if (endPoint.getFinished()
                        && endPoint.isEmptyQueue()) {

                    endPoint.stopTimer();
                    iter.remove();
                }
            }

            /*
             * If there is nothing to process, do not busy-loop.
             */
            if (senderQueue.isEmpty() && this.runFlag) {

                try {
                    Thread.sleep(10);

                } catch (InterruptedException e) {

                    if (!this.runFlag) {
                        break;
                    }
                }
            }
        }


        /*
         * Sender was stopped before transmission completed.
         * Stop timers and remove all remaining endpoints so
         * finish() can terminate gracefully.
         */
        Iterator<GUDPEndPoint> cleanupIter =
                senderQueue.iterator();

        while (cleanupIter.hasNext()) {

            GUDPEndPoint endPoint =
                    cleanupIter.next();

            endPoint.stopTimer();
            cleanupIter.remove();
        }
    }


    public void FSMSender(GUDPEndPoint endPoint) {

         if (endPoint.getFinished()
            && endPoint.isEmptyQueue()) {

        endPoint.stopTimer();
        return;
    }
        switch (endPoint.getState()) {


        /*
         * Initial state
         */
        case INIT:

            endPoint.setState(
                    GUDPEndPoint.endPointState.WAIT);

            break;


        /*
         * Wait until another packet can be sent.
         */
        case WAIT:

            boolean windowFull =
                    endPoint.getNextseqnum()
                    >= endPoint.getBase()
                    + endPoint.getWindowSize();

            boolean noMorePacket =
                    endPoint.getNextseqnum()
                    > endPoint.getLast();


            /*
             * We cannot send anything yet:
             * wait for ACK or new data.
             */
            if (windowFull || noMorePacket) {

                try {
                    Thread.sleep(100);

                } catch (InterruptedException e) {

                    if (!this.runFlag) {
                        return;
                    }
                }

            } else if (!endPoint.isEmptyQueue()
                    || endPoint.getFinished()) {

                /*
                 * There is a packet inside the current window
                 * that can be transmitted.
                 */
                endPoint.setState(
                        GUDPEndPoint.endPointState.SEND);
            }

            break;


        /*
         * Send packets while the GBN window allows.
         */
        case SEND:

            while (
                    endPoint.getNextseqnum()
                            < endPoint.getBase()
                            + endPoint.getWindowSize()

                    &&

                    endPoint.getNextseqnum()
                            <= endPoint.getLast()) {

                GUDPPacket gudppkt =
                        endPoint.getPacket(
                                endPoint.getNextseqnum());


                if (gudppkt == null) {
                    break;
                }


                try {

                    DatagramPacket udppacket =
                            gudppkt.pack();

                    udppacket.setSocketAddress(
                            endPoint.getRemoteEndPoint());

                    sock.send(udppacket);

                } catch (IOException e) {

                    if (debug) {
                        e.printStackTrace();
                    }
                }


                /*
                 * Start timer when sending the oldest
                 * unACKed packet.
                 */
                if (endPoint.getBase()
                        == endPoint.getNextseqnum()

                        &&

                        endPoint.getBase()
                        <= endPoint.getLast()) {

                    endPoint.stopTimer();
                    endPoint.startTimer();
                    
                }


                endPoint.setNextseqnum(
                        endPoint.getNextseqnum() + 1);
            }


            endPoint.setState(
                    GUDPEndPoint.endPointState.WAIT);

            break;


        /*
         * ACK received.
         * GUDPReceiver already updated base and removed
         * ACKed packets.
         */
        case RCV:

            if (endPoint.getBase()
                    == endPoint.getNextseqnum()

                    ||

                    endPoint.getBase()
                    > endPoint.getLast()) {

                /*
                 * Nothing remains in flight.
                 */
                endPoint.stopTimer();
                endPoint.setRetry(0);

            } else if (
                    endPoint.getBase()
                    < endPoint.getNextseqnum()) {

                /*
                 * Some packets remain unACKed.
                 * Restart timer for the oldest one.
                 */
                endPoint.stopTimer();
                endPoint.startTimer();
            }


            endPoint.setState(
                    GUDPEndPoint.endPointState.WAIT);

            break;


        /*
         * Timer expired.
         */
        case TIMEOUT:

            if (endPoint.getRetry()
                    >= endPoint.getMaxRetry()) {

                /*
                 * Make sure no timer remains alive.
                 */
                endPoint.stopTimer();

                failure = new IOException(
                        "GUDP retry limit reached for "
                                + endPoint.getRemoteEndPoint().getAddress()
                                + ":" + endPoint.getRemoteEndPoint().getPort());
                stopGUDPSender();

                break;
            }


            /*
             * The previous timer already expired.
             * Cancel it before starting a new one.
             */
            endPoint.stopTimer();
            endPoint.startTimer();


            /*
             * Go-Back-N:
             * resend every unACKed packet.
             */
            for (int seq = endPoint.getBase();
                    seq < endPoint.getNextseqnum();
                    seq++) {

                GUDPPacket retryPkt =
                        endPoint.getPacket(seq);


                if (retryPkt != null) {

                    try {

                        DatagramPacket udppacket =
                                retryPkt.pack();

                        udppacket.setSocketAddress(
                                endPoint.getRemoteEndPoint());

                        sock.send(udppacket);

                    } catch (IOException e) {

                        if (debug) {
                            e.printStackTrace();
                        }
                    }
                }
            }


            endPoint.setRetry(
                    endPoint.getRetry() + 1);

            endPoint.setState(
                    GUDPEndPoint.endPointState.WAIT);

            break;


        default:

            endPoint.setState(
                    GUDPEndPoint.endPointState.WAIT);

            break;
        }
    }
}
