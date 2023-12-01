package simpledb.log;

import simpledb.file.BlockId;

import java.nio.ByteBuffer;
import java.util.*;

public class AuditManager {
    private LogMgr logMgr;
    private static final int MODIFY = 2;

    public AuditManager(LogMgr logMgr) {
        this.logMgr = logMgr;
    }

    public List<String> findModificationsByBlock(BlockId blk) {
        List<String> modifications = new ArrayList<>();
        for (Iterator<byte[]> it = logMgr.iterator(); it.hasNext(); ) {
            byte[] record = it.next();
            ByteBuffer buffer = ByteBuffer.wrap(record);
            int recordType = buffer.getInt();
            if (recordType == MODIFY) {
                long timestamp = buffer.getLong();
                String ipAddress = new String(readNextString(buffer));
                int txId = buffer.getInt();
                String blockId = new String(readNextString(buffer));
                if (blockId.equals(blk.toString())) {
                    String oldValue = new String(readNextString(buffer));
                    String newValue = new String(readNextString(buffer));
                    modifications.add("Modified by Tx " + txId + " from IP " + ipAddress + " on " + new Date(timestamp) +
                                       ": oldValue=" + oldValue + ", newValue=" + newValue);
                }
            }
        }
        return modifications;
    }

    public List<String> findActivityByTransaction(int txId) {
        List<String> activities = new ArrayList<>();
        for (Iterator<byte[]> it = logMgr.iterator(); it.hasNext(); ) {
            byte[] record = it.next();
            ByteBuffer buffer = ByteBuffer.wrap(record);
            int recordType = buffer.getInt();
            if (recordType == MODIFY) {
                long timestamp = buffer.getLong();
                String ipAddress = new String(readNextString(buffer));
                int recordTxId = buffer.getInt();
                if (recordTxId == txId) {
                    String blockId = new String(readNextString(buffer));
                    String oldValue = new String(readNextString(buffer));
                    String newValue = new String(readNextString(buffer));
                    activities.add("Modified by Tx " + txId + " from IP " + ipAddress + " on " + new Date(timestamp) +
                                   ": Block " + blockId + ", oldValue=" + oldValue + ", newValue=" + newValue);
                }
            }
        }
        return activities;
    }

    public List<String> findActivityByIPAddress(String ipAddress) {
        List<String> activities = new ArrayList<>();
        for (Iterator<byte[]> it = logMgr.iterator(); it.hasNext(); ) {
            byte[] record = it.next();
            ByteBuffer buffer = ByteBuffer.wrap(record);
            int recordType = buffer.getInt();
            if (recordType == MODIFY) {
                long timestamp = buffer.getLong();
                String recordIpAddress = new String(readNextString(buffer));
                if (recordIpAddress.equals(ipAddress)) {
                    int txId = buffer.getInt();
                    String blockId = new String(readNextString(buffer));
                    String oldValue = new String(readNextString(buffer));
                    String newValue = new String(readNextString(buffer));
                    activities.add("Modified by Tx " + txId + " from IP " + ipAddress + " on " + new Date(timestamp) +
                                   ": Block " + blockId + ", oldValue=" + oldValue + ", newValue=" + newValue);
                }
            }
        }
        return activities;
    }

    private byte[] readNextString(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] strBytes = new byte[length];
        buffer.get(strBytes);
        return strBytes;
    }
}
