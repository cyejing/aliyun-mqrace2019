package io.openmessaging.arms;

import java.nio.ByteBuffer;

/**
 * @author Born
 */
public class BombBlock {

    private final ByteBuffer byteBuffer;
    private String fileName;

    public BombBlock() {
        this.byteBuffer = ByteBuffer.allocateDirect(64 * 1024);
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public ByteBuffer reload() {
        byteBuffer.clear();
        return byteBuffer;
    }

    public boolean isFull() {
        if (byteBuffer.position() == byteBuffer.capacity()) {
            return true;
        }
        return false;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
