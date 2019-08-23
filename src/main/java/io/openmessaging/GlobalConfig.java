package io.openmessaging;

/**
 * @author Born
 */
public interface GlobalConfig {

        String BombFile = "/alidata1/race2019/data/bomb-";
    String IndexFile = "/alidata1/race2019/data/index-";
//    String BombFile = "bomb-";
//    String IndexFile = "index-";
    int QueueSize = 1024 * 20;
    int BombSize = 64 * 1024;

    int IndexByte = 24;
    int bodyByte = 34;

    int IndexSize = (64 * 1024) / IndexByte;
    int BodySize = (64 * 1024) / bodyByte;

    int BombIndexSize = IndexSize * IndexByte;
    int BombBodySize = BodySize * bodyByte;

}
