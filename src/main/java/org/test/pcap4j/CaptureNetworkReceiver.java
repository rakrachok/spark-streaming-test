package org.test.pcap4j;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CaptureNetworkReceiver extends Receiver<Packet> {

    private static final Logger log = LoggerFactory.getLogger(CaptureNetworkReceiver.class);

    private static final String READ_TIMEOUT_KEY
            = CaptureNetworkReceiver.class.getName() + ".readTimeout";
    private static final int READ_TIMEOUT
            = Integer.getInteger(READ_TIMEOUT_KEY, 10); // [ms]

    private static final String SNAPLEN_KEY
            = CaptureNetworkReceiver.class.getName() + ".snaplen";
    private static final int SNAPLEN
            = Integer.getInteger(SNAPLEN_KEY, 65536); // [bytes]

    private static final String BUFFER_SIZE_KEY
            = CaptureNetworkReceiver.class.getName() + ".bufferSize";
    private static final int BUFFER_SIZE
            = Integer.getInteger(BUFFER_SIZE_KEY, 1 * 1024 * 1024); // [bytes]

    private static final String NIF_NAME_KEY
            = CaptureNetworkReceiver.class.getName() + ".nifName";
    private static final String NIF_NAME
            = System.getProperty(NIF_NAME_KEY);

    private Thread t;

    public CaptureNetworkReceiver() {
        super(StorageLevel.MEMORY_AND_DISK_SER_2());
    }

    @Override
    public void onStart() {
        t = new Thread(() -> receive());
        t.start();
    }

    @Override
    public void onStop() {
        t.interrupt();
    }

    private void receive() {
        log.info("Starting monitoring");
        try {
            PcapNetworkInterface lo0 = Pcaps.getDevByName("lo0");
            PcapHandle loHandle = lo0.openLive(SNAPLEN, PcapNetworkInterface.PromiscuousMode.NONPROMISCUOUS, READ_TIMEOUT);
            while (true) {
                byte[] rawPacket = loHandle.getNextRawPacket();
                if (rawPacket == null) {
                    log.warn("Nothing to read.");
                    restart("Nothing to read. The receiver is being restarted");
                } else {
                    store(ByteBuffer.wrap(rawPacket));
                }
            }
        } catch (PcapNativeException | NotOpenException e) {
            log.error("Error", e);
            stop("Receiver has been stopped by the error.", e);
        }
    }
}
