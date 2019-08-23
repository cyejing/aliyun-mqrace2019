package io.openmessaging.arms;

import io.openmessaging.arms.commmon.ThroughputRate;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Iterator;
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

    private ConcurrentLinkedQueue<BombBlock> read = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<BombBlock> write = new ConcurrentLinkedQueue<>();

    private Mortar mortar;


    private ThroughputRate fillingRate = new ThroughputRate(1000); //4194304
    private ThroughputRate assemblyRate = new ThroughputRate(1000); //4194304
    private ThroughputRate messageRate = new ThroughputRate(1000); //4194304
    private ThroughputRate indexRate = new ThroughputRate(1000); //4194304

    public MortarFile(Collection<BombBlock> bombBlockCollections) {
        read.addAll(bombBlockCollections);

        this.mortar = new Mortar();
        new Thread(mortar, "MortarFile-" + 0).start();

        Thread printLog = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    log.info("read:{},write:{},fillingRate:{},assemblyRate:{},messageRate:{},indexRate:{}", read.size(),
                            write.size(),
                            fillingRate.getThroughputRate(), assemblyRate.getThroughputRate(),
                            messageRate.getThroughputRate(), indexRate.getThroughputRate());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "printLog");
        printLog.setDaemon(true);
        printLog.start();
    }

    public ThroughputRate getFillingRate() {
        return fillingRate;
    }

    public ThroughputRate getMessageRate() {
        return messageRate;
    }

    public ThroughputRate getIndexRate() {
        return indexRate;
    }

    public void findIndexFile(String fileName, ByteBuffer byteBuffer,long offset) throws IOException {
        FileChannel fileChannel = map.get(fileName);
        fileChannel.read(byteBuffer, offset);
    }

    public void findBodyFile(String fileName, ByteBuffer byteBuffer,long offset) throws IOException {
        FileChannel fileChannel = map.get(fileName);
        fileChannel.read(byteBuffer, offset);
    }

    public void stopWrite() {
        mortar.stop();
        while (!write.isEmpty()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public BombBlock pollRead() {
        BombBlock bombBlock;
        while ((bombBlock = read.poll()) == null) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        bombBlock.reload();
        return bombBlock;
    }

    public ConcurrentLinkedQueue<BombBlock> getWrite() {
        return write;
    }

    public void recycle(BombBlock bombBlock) {
        while (!read.offer(bombBlock)) {

        }
        assemblyRate.note();
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
                        log.info("file write over. readSize:{}, writeSize:{}", read.size(), write.size());
                        return;
                    }else{
                        writeFile();
                    }
                } catch (Exception e) {
                    log.error("write file error", e);
                }
            }
        }

        private void writeFile() {
            try{
                Iterator<BombBlock> it = write.iterator();
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
                log.error("write file error", e);
            }
        }
    }

    public ConcurrentMap<String, FileChannel> getFileMap() {
        return map;
    }
}
