package simpledb.tx.recovery;

import java.util.*;
import simpledb.file.*;
import simpledb.log.*;
import simpledb.buffer.*;
import simpledb.tx.Transaction;

import static simpledb.tx.recovery.LogRecord.*;

public class RecoveryMgr {
   private LogMgr lm;
   private BufferMgr bm;
   private Transaction tx;
   private int txnum;
   private Set<BlockId> modifiedBlocks; // For Programming Ex 5.45

   public RecoveryMgr(Transaction tx, int txnum, LogMgr lm, BufferMgr bm) {
      this.tx = tx;
      this.txnum = txnum;
      this.lm = lm;
      this.bm = bm;
      this.modifiedBlocks = new HashSet<>();
      StartRecord.writeToLog(lm, txnum);
   }

   public void commit() {
      bm.flushAll(txnum);
      int lsn = CommitRecord.writeToLog(lm, txnum);
      lm.flush(lsn);
   }

   public void rollback() {
      doRollback();
      bm.flushAll(txnum);
      int lsn = RollbackRecord.writeToLog(lm, txnum);
      lm.flush(lsn);
   }

   public void recover() {
      doUndoPhase();
      doRedoPhase();
   }

   private void doUndoPhase() {
      Iterator<byte[]> iter = lm.iterator();
      while (iter.hasNext()) {
         byte[] bytes = iter.next();
         LogRecord rec = LogRecord.createLogRecord(bytes);
         if (rec.txNumber() == txnum && !rec.isCommit()) {
            rec.undo(tx);
         }
      }
   }

   private void doRedoPhase() {
      Iterator<byte[]> iter = lm.iterator();
      while (iter.hasNext()) {
         byte[] bytes = iter.next();
         LogRecord rec = LogRecord.createLogRecord(bytes);
         if (rec.txNumber() == txnum && rec.isCommit()) {
            rec.redo(tx);
         }
      }
   }

   public int setInt(Buffer buff, int offset, int newval) {
      int oldval = buff.contents().getInt(offset);
      BlockId blk = buff.block();
      int lsn = SetIntRecord.writeToLog(lm, txnum, blk, offset, oldval, newval);
      return lsn;
   }

   public int setString(Buffer buff, int offset, String newval) {
      String oldval = buff.contents().getString(offset);
      BlockId blk = buff.block();
      int lsn = SetStringRecord.writeToLog(lm, txnum, blk, offset, oldval, newval);
      return lsn;
   }

   /**
    * Rollback the transaction, by iterating
    * through the log records until it finds
    * the transaction's START record,
    * calling undo() for each of the transaction's
    * log records.
    */
   private void doRollback() {
      Iterator<byte[]> iter = lm.iterator();
      while (iter.hasNext()) {
         byte[] bytes = iter.next();
         LogRecord rec = LogRecord.createLogRecord(bytes);
         if (rec.txNumber() == txnum) {
            if (rec.op() == START)
               return;
            rec.undo(tx);
         }
      }
   }

   /**
    * Do a complete database recovery.
    * The method iterates through the log records.
    * Whenever it finds a log record for an unfinished
    * transaction, it calls undo() on that record.
    * The method stops when it encounters a CHECKPOINT record
    * or the end of the log.
    */
   private void doRecover() {
      Collection<Integer> finishedTxs = new ArrayList<>();
      Iterator<byte[]> iter = lm.iterator();
      while (iter.hasNext()) {
         byte[] bytes = iter.next();
         LogRecord rec = LogRecord.createLogRecord(bytes);
         if (rec.op() == CHECKPOINT)
            return;
         if (rec.op() == COMMIT || rec.op() == ROLLBACK)
            finishedTxs.add(rec.txNumber());
         else if (!finishedTxs.contains(rec.txNumber()))
            rec.undo(tx); // Undo only if the transaction is not finished
      }
   }

   public void saveBlockCopy(BlockId blk, FileMgr fm) {
      // Create a Page to read the original block
      Page currentPage = new Page(fm.blockSize());
      fm.read(blk, currentPage);
      // Create a backup block id and a new Page for the backup
      BlockId backupBlk = new BlockId("backup_" + blk.fileName(), blk.number());
      Page backupPage = new Page(fm.blockSize());
      // Copy contents to the backup page
      byte[] data = currentPage.contents().array();
      backupPage.contents().put(data);
      // Write the backup page to the backup block
      fm.write(backupBlk, backupPage);
   }

   public void restoreBlockCopy(BlockId blk, FileMgr fm) {
      // Identify the backup block
      BlockId backupBlk = new BlockId("backup_" + blk.fileName(), blk.number());
      // Create a Page to read the backup block
      Page backupPage = new Page(fm.blockSize());
      fm.read(backupBlk, backupPage);
      // Create a new Page for the original block
      Page originalPage = new Page(fm.blockSize());
      // Copy contents from the backup page to the original page
      byte[] data = backupPage.contents().array();
      originalPage.contents().put(data);
      // Write the original page back to the original block
      fm.write(blk, originalPage);
   }
}
