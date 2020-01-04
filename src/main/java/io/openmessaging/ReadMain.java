package io.openmessaging;

import static io.openmessaging.GlobalConfig.IndexByte;

import io.openmessaging.arms.ArmsCatalog;
import io.openmessaging.arms.ArmsCatalog.BombCatalog;
import io.openmessaging.arms.MortarFile;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Born
 */
public class ReadMain {
    private static final Logger log = LoggerFactory.getLogger(ReadMain.class);


    public ReadMain() {
    }


    public static void rebuildIndex(DefaultMessageStoreImpl messageStore) throws Exception {
        File file = new File(".");
        File[] files = file.listFiles();
        MortarFile mortarFile = messageStore.getMortarFile();
        for (int i = 0; i < files.length; i++) {
            String fileName = files[i].getName();
            if (fileName.indexOf(GlobalConfig.IndexFile) >= 0 || fileName.indexOf(GlobalConfig.BombFile) >= 0) {
                mortarFile.getFileMap().putIfAbsent(fileName, new RandomAccessFile(fileName, "rw").getChannel());
            }
        }
        ArmsCatalog armsCatalog = messageStore.getArmsCatalog();

        ByteBuffer byteBuffer = ByteBuffer.allocate(GlobalConfig.BombSize);
        for (String fileName : mortarFile.getFileMap().keySet()) {
            if (fileName.indexOf(GlobalConfig.IndexFile) >= 0){
                int offsetIndex = 0;
                byteBuffer.clear();
                mortarFile.findIndexFile(fileName,byteBuffer, offsetIndex);
                byteBuffer.flip();

                while (byteBuffer.remaining() >= IndexByte) {
                    long t = byteBuffer.getLong();
                    long a = byteBuffer.getLong();
                    armsCatalog.addBombCatalog(new BombCatalog(t, a, offsetIndex, fileName));
                    offsetIndex += GlobalConfig.BombSize;
                    byteBuffer.clear();
                    mortarFile.findIndexFile(fileName, byteBuffer, offsetIndex);
                    byteBuffer.flip();
                }
            }
        }

    }

    public static void main(String[] args) throws Exception{
        DefaultMessageStoreImpl messageStore = new DefaultMessageStoreImpl();
        rebuildIndex(messageStore);
        List<Message> messages = messageStore.getMessage(23967864, 23999999, 23973319, 24013517);
//        long avgValue = messageStore.getAvgValue(2016307, 2116307, 2062912, 2116630);
//        System.out.println(avgValue);
        System.out.println();

    }

}
