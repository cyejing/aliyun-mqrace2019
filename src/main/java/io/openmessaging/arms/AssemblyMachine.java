package io.openmessaging.arms;

import static io.openmessaging.GlobalConfig.BodyByte;
import static io.openmessaging.GlobalConfig.BombBodySize;
import static io.openmessaging.GlobalConfig.BombIndexSize;
import static io.openmessaging.GlobalConfig.BombSize;
import static io.openmessaging.GlobalConfig.IndexByte;

import io.openmessaging.GlobalConfig;
import io.openmessaging.Message;
import io.openmessaging.arms.ArmsCatalog.BombCatalog;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Born
 */
public class AssemblyMachine {

    private static final Logger log = LoggerFactory.getLogger(AssemblyMachine.class);

    private static ConcurrentHashMap<Long, Carriage> localMap = new ConcurrentHashMap();


    private ArmsCatalog armsCatalog;
    private MortarFile mortarFile;
    private ThreadLocal<Carriage> localCarriage = ThreadLocal.withInitial(() -> {
        Carriage carriage = new Carriage();
        localMap.putIfAbsent(Thread.currentThread().getId(), carriage);
        return carriage;
    });

    private AtomicBoolean stopWrite = new AtomicBoolean(false);


    public AssemblyMachine( ArmsCatalog armsCatalog,MortarFile mortarFile) {
        this.armsCatalog = armsCatalog;
        this.mortarFile = mortarFile;
    }

    public void filling(Message message) {
        long threadId = Thread.currentThread().getId();
        ByteBuffer bodyBuffer = localCarriage.get().bodyBuffer;
        ByteBuffer indexBuffer = localCarriage.get().indexBuffer;
        indexBuffer.putLong(message.getT());
        indexBuffer.putLong(message.getA());
        indexBuffer.putLong(localCarriage.get().bodyFileOffset.getAndAdd(BodyByte));
        bodyBuffer.put(message.getBody(), 0, BodyByte);
        if (bodyBuffer.position() >= BombBodySize) {
            BombBlock bombBlock = mortarFile.pollReady();
            bombBlock.setFileName(GlobalConfig.BombFile + threadId);
            ByteBuffer byteBuffer = bombBlock.reload();
            bodyBuffer.flip();
            byteBuffer.put(bodyBuffer.array());
            byteBuffer.flip();
            bodyBuffer.clear();
            mortarFile.getWorking().offer(bombBlock);
            localCarriage.get().bodyFileOffset.getAndAdd(BombSize - BombBodySize); // TODO
        }
        if (indexBuffer.position() >= BombIndexSize) {
            BombBlock bombBlock = mortarFile.pollReady();
            bombBlock.setFileName(GlobalConfig.IndexFile + threadId);
            ByteBuffer byteBuffer = bombBlock.reload();
            indexBuffer.flip();
            byteBuffer.put(indexBuffer.array());
            byteBuffer.flip();
            indexBuffer.clear();
            mortarFile.getWorking().offer(bombBlock);
        }
        if (indexBuffer.position() == IndexByte) {
            armsCatalog.addBombCatalog(new BombCatalog(message.getT(), message.getA(),
                    localCarriage.get().indexFileOffset.getAndAdd(BombSize),
                    GlobalConfig.IndexFile + threadId));
        }
    }

    private CountDownLatch latch = new CountDownLatch(1);

    public void stopWrite() {
        try {
            if (stopWrite.compareAndSet(false, true)) {
                for (Entry<Long, Carriage> entry : localMap.entrySet()) {
                    {
                        BombBlock bombBlock = mortarFile.pollReady();
                        bombBlock.setFileName(GlobalConfig.BombFile + entry.getKey());
                        ByteBuffer byteBuffer = bombBlock.reload();
                        entry.getValue().bodyBuffer.flip();
                        byteBuffer.put(entry.getValue().bodyBuffer);
                        byteBuffer.flip();
                        mortarFile.getWorking().offer(bombBlock);
                    }
                    {
                        BombBlock bombBlock = mortarFile.pollReady();
                        bombBlock.setFileName(GlobalConfig.IndexFile + entry.getKey());
                        ByteBuffer byteBuffer = bombBlock.reload();
                        entry.getValue().indexBuffer.flip();
                        byteBuffer.put(entry.getValue().indexBuffer);
                        byteBuffer.flip();
                        mortarFile.getWorking().offer(bombBlock);
                    }
                }

                mortarFile.stopWrite();
                latch.countDown();
            }
            latch.await();
        } catch (Exception e) {
            log.error("stopWrite error:", e);
        }
    }


    class Carriage {

        ByteBuffer bodyBuffer = ByteBuffer.allocate(64 * 1024);
        ByteBuffer indexBuffer = ByteBuffer.allocate(64 * 1024);

        AtomicLong bodyFileOffset = new AtomicLong(0);
        AtomicLong indexFileOffset = new AtomicLong(0);

        @Override
        public String toString() {
            return "Carriage{" +
                    "bodyBuffer=" + bodyBuffer +
                    ", indexBuffer=" + indexBuffer +
                    ", bodyFileOffset=" + bodyFileOffset +
                    ", indexFileOffset=" + indexFileOffset +
                    '}';
        }
    }
}
