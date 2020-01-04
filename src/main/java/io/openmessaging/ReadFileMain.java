package io.openmessaging;

import io.openmessaging.arms.ScratchMachine.AvgResult;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Born
 */
public class ReadFileMain {
    private static final Logger log = LoggerFactory.getLogger(ReadFileMain.class);


    public ReadFileMain() {
    }



    public static void main(String[] args) throws Exception{
//
//        FileChannel fileChannel = new RandomAccessFile("index-19", "rw").getChannel();
//        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(GlobalConfig.BombSize);
//
//        fileChannel.read(byteBuffer, 85852160 - 24 *5);
//        byteBuffer.flip();
//
//        System.out.println();

        CompletableFuture<AvgResult> future = CompletableFuture.completedFuture(new AvgResult(0, 0));

        future = future.thenCombine(CompletableFuture.supplyAsync(() -> new AvgResult(100, 10)), (avgResult1, avgResult2) -> {
            avgResult1.sum = avgResult1.sum + avgResult2.sum;
            avgResult1.count = avgResult1.count + avgResult2.count;
            return avgResult1;
        });



        future.whenComplete((avgResult, throwable) -> {
            System.out.println(avgResult.sum);
            System.out.println(avgResult.count);
        });

        future =future.thenCombine(CompletableFuture.supplyAsync(() -> new AvgResult(100, 10)), (avgResult1, avgResult2) -> {
            avgResult1.sum = avgResult1.sum + avgResult2.sum;
            avgResult1.count = avgResult1.count + avgResult2.count;
            return avgResult1;
        });

        future =future.thenCombine(CompletableFuture.supplyAsync(() -> new AvgResult(100, 10)), (avgResult1, avgResult2) -> {
            avgResult1.sum = avgResult1.sum + avgResult2.sum;
            avgResult1.count = avgResult1.count + avgResult2.count;
            return avgResult1;
        });


        future.whenComplete((avgResult, throwable) -> {
            System.out.println(avgResult.sum);
            System.out.println(avgResult.count);
        });


    }



}
