package simpledb.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.*;

import java.util.Iterator;
import java.util.List;
import java.util.Date;

/**
 * The log manager, which is responsible for
 * writing log records into a log file using a buffer manager.
 */
public class LogMgr {
   private BufferMgr bufferMgr;
   private String logfile;
   private Buffer currentBuffer;
   private Page logpage;
   private int latestLSN = 0;
   private int lastSavedLSN = 0;
   private FileMgr fm;
   private static final int CHECKPOINT = 0;
   private static final int AUDIT = 1;
   private static final int MODIFY = 2;

   /**
    * Creates the manager for the specified log file.
    *
    * @param bufferMgr the buffer manager
    * @param logfile   the name of the log file
    */
   public LogMgr(BufferMgr bufferMgr, String logfile) {
      this.bufferMgr = bufferMgr;
      this.logfile = logfile;
      this.fm = bufferMgr.getFileMgr();
      int logsize = bufferMgr.length(logfile);
      BlockId blk = logsize == 0 ? appendNewBlock() : new BlockId(logfile, logsize - 1);
      currentBuffer = bufferMgr.pin(blk);
      logpage = currentBuffer.contents();
   }

   /**
    * Appends a log record to the buffer.
    *
    * @param logrec a byte array containing the log record
    * @return the LSN of the final value
    */
   public synchronized int append(byte[] logrec) {
      int boundary = logpage.getInt(0);
      int recsize = logrec.length;
      int bytesneeded = recsize + Integer.BYTES;
      if (boundary - bytesneeded < Integer.BYTES) {
         bufferMgr.unpin(currentBuffer);
         BlockId newblk = appendNewBlock();
         currentBuffer = bufferMgr.pin(newblk);
         logpage = currentBuffer.contents();
         boundary = logpage.getInt(0);
      }
      int recpos = boundary - bytesneeded;

      logpage.setBytes(recpos, logrec);
      logpage.setInt(0, recpos);
      latestLSN += 1;
      return latestLSN;
   }

   /**
    * Ensures that the log record corresponding to the specified LSN has been
    * written to disk.
    *
    * @param lsn the LSN of a log record
    */
   public void flush(int lsn) {
      if (lsn >= lastSavedLSN) {
         bufferMgr.flush(currentBuffer);
         lastSavedLSN = latestLSN;
      }
   }

   /**
    * Appends a new block to the log file.
    *
    * @return the BlockId of the new block
    */
   private BlockId appendNewBlock() {
      BlockId blk = bufferMgr.append(logfile);
      bufferMgr.pin(blk);
      logpage = currentBuffer.contents();
      logpage.setInt(0, fm.blockSize());
      bufferMgr.flush(currentBuffer);
      return blk;
   }

   public void archiveLog(String archiveDir) {
      try {
         File currentLogFile = new File(logfile);
         File archiveFile = new File(archiveDir, currentLogFile.getName());
         // Ensure the archive directory exists
         archiveFile.getParentFile().mkdirs();
         // Move the current log file to the archive directory
         if (!currentLogFile.renameTo(archiveFile)) {
            throw new IOException("Failed to move log file to archive.");
         }
         // Start a new log file
         BlockId newblk = appendNewBlock();
         currentBuffer = bufferMgr.pin(newblk);
         logpage = currentBuffer.contents();
      } catch (IOException e) {
         throw new RuntimeException("Error archiving log file: " + e.getMessage(), e);
      }
   }

   public Iterator<byte[]> iterator() {
      return new Iterator<byte[]>() {
         private BlockId currentBlk = new BlockId(logfile, 0);
         private int currentPos = Integer.BYTES;

         @Override
         public boolean hasNext() {
            return !isEndOfLog();
         }

         @Override
         public byte[] next() {
            if (!hasNext()) {
               throw new NoSuchElementException();
            }
            // Read the next log record
            Buffer buf = bufferMgr.pin(currentBlk);
            Page p = buf.contents();
            int recordSize = p.getInt(currentPos);
            byte[] record = new byte[recordSize];
            p.getBytes(currentPos + Integer.BYTES);
            // Move to the next record
            currentPos += Integer.BYTES + recordSize;
            if (currentPos >= fm.blockSize()) {
               // Move to the next block if the end of the current block is reached
               bufferMgr.unpin(buf);
               currentBlk = new BlockId(logfile, currentBlk.number() + 1);
               currentPos = Integer.BYTES;
            } else {
               bufferMgr.unpin(buf);
            }
            return record;
         }

         private boolean isEndOfLog() {
            // Check if the end of the log file is reached
            // This could be based on the file size or a special marker in the log
            if (currentBlk.number() >= bufferMgr.length(logfile)) {
               return true;
            }
            Buffer buf = bufferMgr.pin(currentBlk);
            Page p = buf.contents();
            int boundary = p.getInt(0);
            bufferMgr.unpin(buf);
            return currentPos >= boundary;
         }
      };
   }

   public synchronized int writeCheckpointRecord(List<Integer> activeTxIds) {
      ByteBuffer buffer = ByteBuffer.allocate(1024); // Example buffer size
      buffer.putInt(CHECKPOINT);
      buffer.putLong(new Date().getTime()); // Current timestamp
      for (int txId : activeTxIds) {
         buffer.putInt(txId);
      }
      byte[] record = buffer.array();
      return append(record);
   }

   public synchronized int writeAuditRecord(String ipAddress, int txId, BlockId blk, String operation) {
      ByteBuffer buffer = ByteBuffer.allocate(1024); // Adjust size as needed
      buffer.putInt(AUDIT);
      buffer.putLong(new Date().getTime());
      buffer.put(ipAddress.getBytes());
      buffer.putInt(txId);
      buffer.put(blk.toString().getBytes());
      buffer.put(operation.getBytes());
      byte[] record = buffer.array();
      return append(record);
   }

   public synchronized int writeModifyRecord(String ipAddress, int txId, BlockId blk, String oldValue,
         String newValue) {
      ByteBuffer buffer = ByteBuffer.allocate(1024); // Example buffer size
      buffer.putInt(MODIFY);
      buffer.putLong(new Date().getTime()); // Current timestamp
      putString(buffer, ipAddress);
      buffer.putInt(txId);
      putString(buffer, blk.toString());
      putString(buffer, oldValue);
      putString(buffer, newValue);
      byte[] record = buffer.array();
      return append(record);
   }

   private void putString(ByteBuffer buffer, String str) {
      byte[] strBytes = str.getBytes();
      buffer.putInt(strBytes.length);
      buffer.put(strBytes);
   }
}
