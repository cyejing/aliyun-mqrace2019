package io.openmessaging;

import static io.openmessaging.GlobalConfig.QueueSize;

import io.openmessaging.arms.ArmsCatalog;
import io.openmessaging.arms.AssemblyMachine;
import io.openmessaging.arms.BombBlock;
import io.openmessaging.arms.MortarFile;
import io.openmessaging.arms.ScratchMachine;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Born
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private static final Logger log = LoggerFactory.getLogger(DefaultMessageStoreImpl.class);

    private AssemblyMachine assemblyMachine;
    private ScratchMachine scratchMachine;
    private ArmsCatalog armsCatalog;
    private MortarFile mortarFile;


    {
        List<BombBlock> bombBlocks = new ArrayList<>();
        for (int i = 0; i < QueueSize; i++) {
            bombBlocks.add(new BombBlock());
        }
        armsCatalog = new ArmsCatalog();
        mortarFile = new MortarFile(bombBlocks);
        assemblyMachine = new AssemblyMachine(armsCatalog,mortarFile);
        scratchMachine = new ScratchMachine(armsCatalog, mortarFile);

    }


    private AtomicInteger printSize = new AtomicInteger(20000);
    @Override
    void put(Message message) {
        assemblyMachine.filling(message);
    }

    @Override
    List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        assemblyMachine.stopWrite();
        return scratchMachine.findMessage(aMin, aMax, tMin, tMax);
    }

    @Override
    long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        return scratchMachine.findAvg(aMin, aMax, tMin, tMax);
    }

    public ArmsCatalog getArmsCatalog() {
        return armsCatalog;
    }

    public MortarFile getMortarFile() {
        return mortarFile;
    }
}
