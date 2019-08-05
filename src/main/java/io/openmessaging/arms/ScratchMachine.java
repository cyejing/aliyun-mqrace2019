package io.openmessaging.arms;

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
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author Born
 */
public class ScratchMachine {

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
                    if (byteBuffer.position() + 16 > byteBuffer.limit()) {
                        break;
                    }
                    int t = byteBuffer.getInt();
                    int a = byteBuffer.getInt();
                    long offset = byteBuffer.getLong();
                    if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                        result.add(new BombCatalog(t, a, offset, fileName));
                        mortarFile.getIndexRate().note();
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
            NavigableSet<BombCatalog> result = new ConcurrentSkipListSet<>(Comparator.comparingLong(BombCatalog::getOffset));
            ByteBuffer byteBuffer = ByteBuffer.allocate(GlobalConfig.BombSize);
            for (BombCatalog bombCatalog : bombCatalogs) {
                String fileName = bombCatalog.getFileName();
                mortarFile.findIndexFile(fileName, byteBuffer, bombCatalog.getOffset());
                byteBuffer.flip();
                for (int i = 0; i < GlobalConfig.IndexSize; i++) {
                    if (byteBuffer.position() + 16 > byteBuffer.limit()) {
                        break;
                    }
                    int t = byteBuffer.getInt();
                    int a = byteBuffer.getInt();
                    long offset = byteBuffer.getLong();
                    if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                        sum += a;
                        count += 1;
                        mortarFile.getIndexRate().note();
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
            mortarFile.findBodyFile(firstFileName, byteBuffer, first.getOffset());
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
                byte[] body = new byte[34];
                byteBuffer.get(body,0,32);
                result.add(new Message(bombCatalog.getA(), bombCatalog.getT(), body));
                mortarFile.getMessageRate().note();
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public long findAvg(long aMin, long aMax, long tMin, long tMax) {
        Set<String> fileNames = armsCatalog.getFileNames();
        long sum = 0;
        long count = 0;
        for (String fileName : fileNames) {
            Set<BombCatalog> bombCatalogs = armsCatalog.findBombCatalogs(fileName, tMin, tMax);
            AvgResult avgResult = findAvgIndexFile(bombCatalogs, aMin, aMax, tMin, tMax);
            sum += avgResult.sum;
            count += avgResult.count;
        }
        if (count == 0) {
            return 0;
        }
        return sum / count;
    }


    class AvgResult{

        long sum;
        long count;

        public AvgResult(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }
    }
}
