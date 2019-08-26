package io.openmessaging.arms;

import static io.openmessaging.GlobalConfig.BombSize;
import static io.openmessaging.GlobalConfig.CacheSize;

import io.openmessaging.arms.ArmsCatalog.BombCatalog;
import io.openmessaging.arms.commmon.ThroughputRate;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Born
 */
public class MortarFile {

    private static final Logger log = LoggerFactory.getLogger(MortarFile.class);

    private ConcurrentMap<String, FileChannel> map = new ConcurrentHashMap<>();

    private ConcurrentLinkedQueue<BombBlock> ready = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<BombBlock> working = new ConcurrentLinkedQueue<>();


    private LinkedHashMap<Long, BombBlock> cache = new LinkedHashMap<Long, BombBlock>(8, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > CacheSize;
        }
    };

    private Mortar mortar;

    private ThroughputRate assemblyRate = new ThroughputRate(1000); //4194304
    private ThroughputRate messageRate = new ThroughputRate(1000); //4194304


    public MortarFile(Collection<BombBlock> bombBlockCollections) {
        ready.addAll(bombBlockCollections);

        this.mortar = new Mortar();
        new Thread(mortar, "MortarFile-" + 0).start();

        Thread printLog = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    log.info("ready:{},working:{},assemblyRate:{},messageRate:{},indexRate:{}",
                            ready.size(), working.size(), assemblyRate.getThroughputRate(),messageRate.getThroughputRate());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "printLog");
        printLog.setDaemon(true);
        printLog.start();
    }

    public void findIndexFile(String fileName, ByteBuffer byteBuffer,long offset) throws IOException {
        FileChannel fileChannel = map.get(fileName);
        fileChannel.read(byteBuffer, offset);
    }

    public void findBodyFile(String fileName, ByteBuffer byteBuffer, long offset) throws IOException {
        FileChannel fileChannel = map.get(fileName);
        fileChannel.read(byteBuffer, offset);
    }

    public void stopWrite() {
        mortar.stop();
        while (!working.isEmpty()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public BombBlock pollReady() {
        BombBlock bombBlock;
        while ((bombBlock = ready.poll()) == null) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        bombBlock.reload();
        return bombBlock;
    }

    public ConcurrentLinkedQueue<BombBlock> getWorking() {
        return working;
    }

    public void recycle(BombBlock bombBlock) {
        while (!ready.offer(bombBlock)) {

        }
        assemblyRate.note();
    }

    public CompletableFuture<ByteBuffer> findIndexFileAsync(String fileName, long offset) {
        return CompletableFuture.supplyAsync(() -> {
            ByteBuffer byteBuffer = ByteBuffer.allocate(BombSize);
//            BombBlock bombBlock = pollReady();
//            ByteBuffer byteBuffer = bombBlock.reload();
            FileChannel fileChannel = map.get(fileName);
            try {
                fileChannel.read(byteBuffer, offset);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return byteBuffer;
        });
    }

    class Mortar implements Runnable {

        private boolean stop = false;

        public void stop() {
            stop = true;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    if (stop) {
                        writeFile();
                        log.info("file working over. readSize:{}, writeSize:{}", ready.size(), working.size());
                        return;
                    } else {
                        writeFile();
                    }
                } catch (Exception e) {
                    log.error("working file error", e);
                }
            }
        }

        private void writeFile() {
            try {
                Iterator<BombBlock> it = working.iterator();
                while (it.hasNext()) {
                    BombBlock block = it.next();
                    String fileName = block.getFileName();
                    FileChannel fileChannel = map.get(fileName);
                    if (fileChannel == null) {
                        map.putIfAbsent(fileName, new RandomAccessFile(fileName, "rw").getChannel());
                        fileChannel = map.get(fileName);
                    }
                    ByteBuffer byteBuffer = block.getByteBuffer();
                    fileChannel.write(byteBuffer);
                    byteBuffer.clear();
                    it.remove();
                    recycle(block);
                }
            } catch (Exception e) {
                log.error("working file error", e);
            }
        }
    }

    public ConcurrentMap<String, FileChannel> getFileMap() {
        return map;
    }

    public ThroughputRate getMessageRate() {
        return messageRate;
    }
}
