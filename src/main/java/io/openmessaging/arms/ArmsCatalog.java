package io.openmessaging.arms;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author Born
 */
public class ArmsCatalog {


    private Map<String, ConcurrentSkipListSet<BombCatalog>> map = new ConcurrentHashMap();


    public boolean addBombCatalog(BombCatalog bombCatalog) {
        String fileName = bombCatalog.getFileName();
        ConcurrentSkipListSet<BombCatalog> bombCatalogs = map.get(fileName);
        if (bombCatalogs == null) {
            ConcurrentSkipListSet<BombCatalog> set = new ConcurrentSkipListSet<>(Comparator.comparingLong(BombCatalog::getT));
            map.putIfAbsent(fileName, set);
            bombCatalogs = map.get(fileName);
        }
        return bombCatalogs.add(bombCatalog);
    }

    public Set<String> getFileNames() {
        return map.keySet();
    }

    public Map<String, ConcurrentSkipListSet<BombCatalog>> getMap() {
        return map;
    }

    public Set<BombCatalog> findBombCatalogs(String fileName,long min, long max) {
        ConcurrentSkipListSet<BombCatalog> bombCatalogs = map.get(fileName);
        if (max < bombCatalogs.first().getT()) {
            return Collections.emptySet();
        }
//        long rMin = Math.max(min, bombCatalogs.first().t);
//        long rMax = Math.min(max, bombCatalogs.last().t);
        BombCatalog lower = bombCatalogs.lower(new BombCatalog(min));
        if (lower == null) {
            lower = bombCatalogs.first();
        }
        return bombCatalogs.subSet(lower, true, new BombCatalog(max), true);
    }


    public static class BombCatalog implements Serializable {

        private long t;
        private long a;

        private long offset;

        private String fileName;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BombCatalog)) {
                return false;
            }
            BombCatalog that = (BombCatalog) o;
            return getT() == that.getT() &&
                    getA() == that.getA() &&
                    getOffset() == that.getOffset() &&
                    Objects.equals(getFileName(), that.getFileName());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getT(), getA(), getOffset(), getFileName());
        }

        public BombCatalog() {
        }

        public BombCatalog(long t) {
            this.t = t;
        }

        public BombCatalog(long t, long a, long offset,String fileName) {
            this.t = t;
            this.a = a;
            this.offset = offset;
            this.fileName = fileName;
        }

        public long getT() {
            return t;
        }

        public void setT(long t) {
            this.t = t;
        }

        public long getA() {
            return a;
        }

        public void setA(long a) {
            this.a = a;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        @Override
        public String toString() {
            return "BombCatalog{" +
                    "t=" + t +
                    ", a=" + a +
                    ", offset=" + offset +
                    ", fileName='" + fileName + '\'' +
                    '}';
        }
    }
}
