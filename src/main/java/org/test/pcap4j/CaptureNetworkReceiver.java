package org.test.pcap4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PacketListener;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.Pcaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CaptureNetworkReceiver extends Receiver<Tuple2<String, byte[]>> {

    private static final Logger log = LoggerFactory.getLogger(CaptureNetworkReceiver.class);

    private static final String READ_TIMEOUT_KEY
            = CaptureNetworkReceiver.class.getName() + ".readTimeout";
    private static final int READ_TIMEOUT
            = Integer.getInteger(READ_TIMEOUT_KEY, 10); // [ms]

    private static final String SNAPLEN_KEY
            = CaptureNetworkReceiver.class.getName() + ".snaplen";
    private static final int SNAPLEN
            = Integer.getInteger(SNAPLEN_KEY, 65536); // [bytes]

    private static final String NIF_NAME_KEY
            = CaptureNetworkReceiver.class.getName() + ".nifName";
    private static final String NIF_NAME
            = System.getProperty(NIF_NAME_KEY);

    private ConcurrentMap<String, Thread> localReceivers = new ConcurrentHashMap<>();

    public CaptureNetworkReceiver() {
        super(StorageLevel.MEMORY_ONLY());
    }

    @Override
    public void onStart() {
        log.info("---CaptureNetworkReceiver onStart---");
        List<PcapNetworkInterface> nifs;
        try {
            if (StringUtils.isBlank(NIF_NAME)) {
                nifs = Pcaps.findAllDevs();
            } else {
                nifs = new ArrayList<>(1);
                nifs.add(Pcaps.getDevByName(NIF_NAME));
            }
        } catch (PcapNativeException e) {
            throw new RuntimeException(e);
        }

        if (nifs == null) {
            stop("Cannot find any NIFs");
            return;
        }

        for (PcapNetworkInterface nif : nifs) {
            localReceivers.computeIfAbsent(nif.getName(), t -> new Thread(() -> receive(nif)));
        }

        localReceivers.forEach((s, t) -> {
            log.info("Starting a thread for " + s);
            t.start();
        });
    }

    @Override
    public void onStop() {
        log.info("---CaptureNetworkReceiver onStop---");
        log.info("Killing threads");
        localReceivers.forEach((s, t) -> t.interrupt());
    }

    private void receive(PcapNetworkInterface nif) {
        log.info("Starting monitoring NIF: " + nif.getName());
        PcapHandle nifHandle;
        try {
            nifHandle = nif.openLive(SNAPLEN, PcapNetworkInterface.PromiscuousMode.NONPROMISCUOUS, READ_TIMEOUT);
        } catch (PcapNativeException e) {
            log.error("Error", e);
            log.info("Stopping a thread for " + nif.getName());
            return;
        }

        try {
            nifHandle.loop(5 * 60, (PacketListener) packet -> {
                log.info("Packet received: " + packet);
                byte[] rawPacket = packet.getRawData();
                log.info("Storing bytes: " + Arrays.toString(rawPacket));
                store(new Tuple2<>(nif.getName(), rawPacket));
            });
        } catch (PcapNativeException | InterruptedException | NotOpenException e) {
            e.printStackTrace();
        }
        restartReceiver();
    }

    //The last thread will restart the receiver
    private void restartReceiver() {
        int aliveThreads = 0;
        String nif = null;
        Thread currentThread = null;

        for (Map.Entry<String, Thread> entry : localReceivers.entrySet()) {
            if (entry.getValue().isAlive()) {
                aliveThreads++;
                currentThread = entry.getValue();
                nif = entry.getKey();
            }
        }

        if (aliveThreads == 1 && currentThread == Thread.currentThread()) {
            restart("The receiver is being restarted by the NIF: " + nif);
        }
    }

}
