package io.openmessaging;

/**
 * @author Born
 */
public interface GlobalConfig {

    //    String BombFile = "/alidata1/race2019/data/bomb-";
//    String IndexFile = "/alidata1/race2019/data/index-";
    String BombFile = "bomb-";
    String IndexFile = "index-";
    int QueueSize = 1024 * 20;
    int BombSize = 64 * 1024;
    int IndexSize = (64 * 1024) / 16;
    int BodySize = (64 * 1024) / 32;

}
