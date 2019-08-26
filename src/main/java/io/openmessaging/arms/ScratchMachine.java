package io.openmessaging.arms;

import static io.openmessaging.GlobalConfig.BodyByte;
import static io.openmessaging.GlobalConfig.IndexByte;

import io.openmessaging.GlobalConfig;
import io.openmessaging.Message;
import io.openmessaging.arms.ArmsCatalog.BombCatalog;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Born
 */
public class ScratchMachine {
    private static final Logger log = LoggerFactory.getLogger(ScratchMachine.class);

    private ArmsCatalog armsCatalog;
    private MortarFile mortarFile;

    public ScratchMachine(ArmsCatalog armsCatalog, MortarFile mortarFile) {
        this.armsCatalog = armsCatalog;
        this.mortarFile = mortarFile;
    }

    public List<Message> findMessage(long aMin, long aMax, long tMin, long tMax) {
        Set<String> fileNames = armsCatalog.getFileNames();
        List<Message> result = new ArrayList<>();
        for (String fileName : fileNames) {
            Set<BombCatalog> bombCatalogs = armsCatalog.findBombCatalogs(fileName, tMin, tMax);
            NavigableSet<BombCatalog> indexFile = findIndexFile(bombCatalogs, aMin, aMax, tMin, tMax);
            List<Message> messages = findBodyFile(indexFile);
            result.addAll(messages);
        }
        mortarFile.getMessageRate().note();
        result.sort(Comparator.comparingLong(Message::getT));
        return new ArrayList<>(result);
    }

    public NavigableSet<BombCatalog> findIndexFile(Set<BombCatalog> bombCatalogs, long aMin, long aMax, long tMin, long tMax) {
        try {
            NavigableSet<BombCatalog> result = new ConcurrentSkipListSet<>(Comparator.comparingLong(BombCatalog::getOffset));
            ByteBuffer byteBuffer = ByteBuffer.allocate(GlobalConfig.BombSize);
            for (BombCatalog bombCatalog : bombCatalogs) {
                String fileName = bombCatalog.getFileName();
                mortarFile.findIndexFile(fileName, byteBuffer, bombCatalog.getOffset());
                byteBuffer.flip();
                for (int i = 0; i < GlobalConfig.IndexSize; i++) {
                    if (byteBuffer.position() + IndexByte > byteBuffer.limit()) {
                        break;
                    }
                    long t = byteBuffer.getLong();
                    long a = byteBuffer.getLong();
                    long offset = byteBuffer.getLong();
                    if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                        result.add(new BombCatalog(t, a, offset, fileName));
                    }
                }
                byteBuffer.clear();
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    public AvgResult findAvgIndexFile(Set<BombCatalog> bombCatalogs, long aMin, long aMax, long tMin, long tMax) {
        try {
            long sum=0;
            long count=0;
            ByteBuffer byteBuffer = ByteBuffer.allocate(GlobalConfig.BombSize);
            for (BombCatalog bombCatalog : bombCatalogs) {
                String fileName = bombCatalog.getFileName();
                mortarFile.findIndexFile(fileName, byteBuffer, bombCatalog.getOffset());
                byteBuffer.flip();
                for (int i = 0; i < GlobalConfig.IndexSize; i++) {
                    if (byteBuffer.position() + IndexByte > byteBuffer.limit()) {
                        break;
                    }
                    long t = byteBuffer.getLong();
                    long a = byteBuffer.getLong();
                    long offset = byteBuffer.getLong();
                    if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                        sum += a;
                        count += 1;
                    }
                }
                byteBuffer.clear();
            }
            return new AvgResult(sum, count);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }



    public CompletableFuture<AvgResult> findAvgIndexFileAsync(Set<BombCatalog> bombCatalogs, long aMin, long aMax, long tMin, long tMax) {
        try {
            CompletableFuture<AvgResult> result = CompletableFuture.completedFuture(new AvgResult(0, 0));

            for (BombCatalog bombCatalog : bombCatalogs) {
                String fileName = bombCatalog.getFileName();
                CompletableFuture<ByteBuffer> bombFuture = mortarFile.findIndexFileAsync(fileName, bombCatalog.getOffset());
                result = result.thenCombine(bombFuture, (avgResult, bombBlock) ->{
                    ByteBuffer byteBuffer = bombBlock;
                    byteBuffer.flip();
                    for (int i = 0; i < GlobalConfig.IndexSize; i++) {
                        if (byteBuffer.position() + IndexByte > byteBuffer.limit()) {
                            break;
                        }
                        long t = byteBuffer.getLong();
                        long a = byteBuffer.getLong();
                        long offset = byteBuffer.getLong();
                        if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                            avgResult.sum += a;
                            avgResult.count += 1;
                        }
                    }
                    byteBuffer.clear();
//                    mortarFile.recycle(bombBlock);
                    return avgResult;
                });

            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }



    public List<Message> findBodyFile(NavigableSet<BombCatalog> bombCatalogs) {
        if (bombCatalogs == null || bombCatalogs.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            List<Message> result = new ArrayList<>();
            ByteBuffer byteBuffer = ByteBuffer.allocate(GlobalConfig.BombSize);
            BombCatalog first = bombCatalogs.first();
            long fileOffset = first.getOffset();
            String firstFileName = GlobalConfig.BombFile + first.getFileName().substring(first.getFileName().lastIndexOf(GlobalConfig.IndexFile)+GlobalConfig.IndexFile
                    .length());
            try {
                mortarFile.findBodyFile(firstFileName, byteBuffer, first.getOffset());
            } catch (Exception e) {
                log.error("findBodyFile error,name:{},catalog:{}",firstFileName,first);
                e.printStackTrace();
                return Collections.emptyList();
            }
            byteBuffer.flip();

            for (BombCatalog bombCatalog : bombCatalogs) {
                if (bombCatalog.getOffset() >= fileOffset + GlobalConfig.BombSize) {
                    byteBuffer.clear();
                    String fileName = GlobalConfig.BombFile + bombCatalog.getFileName().substring(bombCatalog.getFileName().lastIndexOf(GlobalConfig.IndexFile)+GlobalConfig.IndexFile.length());
                    mortarFile.findBodyFile(fileName, byteBuffer, bombCatalog.getOffset());
                    byteBuffer.flip();
                    fileOffset = bombCatalog.getOffset();
                }
                byteBuffer.position(new Long(bombCatalog.getOffset() - fileOffset).intValue());
                byte[] body = new byte[BodyByte];
                byteBuffer.get(body, 0, BodyByte);
                result.add(new Message(bombCatalog.getA(), bombCatalog.getT(), body));
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public long findAvg(long aMin, long aMax, long tMin, long tMax) {
        Set<String> fileNames = armsCatalog.getFileNames();
        CompletableFuture<AvgResult> result = CompletableFuture.completedFuture(new AvgResult(0, 0));
        for (String fileName : fileNames) {
            Set<BombCatalog> bombCatalogs = armsCatalog.findBombCatalogs(fileName, tMin, tMax);

            CompletableFuture<AvgResult> async = findAvgIndexFileAsync(bombCatalogs, aMin, aMax, tMin, tMax);
            result = result.thenCombine(async, (avgResult1, avgResult2) -> {
                avgResult1.sum = avgResult1.sum + avgResult2.sum;
                avgResult1.count = avgResult1.count + avgResult2.count;
                return avgResult1;
            });
        }
        AvgResult join = result.join();
        if (join.count == 0) {
            return 0;
        }
        return join.sum / join.count;
    }


    public static class AvgResult{

        public long sum;
        public long count;

        public AvgResult(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }

    }
}
