package simpledb.log;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.*;

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

   /**
    * Creates the manager for the specified log file.
    *
    * @param bufferMgr the buffer manager
    * @param logfile   the name of the log file
    */
   public LogMgr(BufferMgr bufferMgr, String logfile) {
      this.bufferMgr = bufferMgr;
      this.logfile = logfile;

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
}
