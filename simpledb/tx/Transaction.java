package simpledb.tx;

import simpledb.file.*;
import simpledb.log.LogMgr;
import simpledb.buffer.*;
import simpledb.tx.recovery.*;
import simpledb.tx.concurrency.ConcurrencyMgr;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Transaction {
   private static int nextTxNum = 0;
   private static final int END_OF_FILE = -1;
   private RecoveryMgr recoveryMgr;
   private ConcurrencyMgr concurMgr;
   private BufferMgr bm;
   private FileMgr fm;
   private int txnum;
   private BufferList mybuffers;

   private boolean active; // Flag to indicate if the transaction is active
   private List<BlockId> newlyAppendedBlocks; // List to track newly appended blocks

   private static List<Transaction> activeTransactions = new ArrayList<>();
   private static final Object checkpointLock = new Object();
   private static volatile boolean isCheckpointing = false;
   private static final String TXNUM_STORAGE_FILE = "txnum.dat";

   private FileMgr fileMgr;
   private List<String> tempFilesCreated;


   public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm) {
      this.fm = fm;
      this.bm = bm;
      this.txnum = nextTxNumber();
      this.recoveryMgr = new RecoveryMgr(this, txnum, lm, bm);
      this.concurMgr = new ConcurrencyMgr();
      this.mybuffers = new BufferList(bm);
      this.active = true;
      this.newlyAppendedBlocks = new ArrayList<>();

      this.fileMgr = fileMgr;
      this.tempFilesCreated = new ArrayList<>();

      synchronized (checkpointLock) {
         while (isCheckpointing) {
            try {
               checkpointLock.wait();
            } catch (InterruptedException e) {
               throw new RuntimeException("Transaction interrupted during checkpoint wait.", e);
            }
         }
         activeTransactions.add(this);
      }
   }

   public void commit() {
      recoveryMgr.commit();
      concurMgr.release();
      mybuffers.unpinAll();
      active = false;
      activeTransactions.remove(this);

      // Delete temporary files created in this transaction
      for (String tempFileName : tempFilesCreated) {
         fileMgr.deleteFile(tempFileName);
      }
      tempFilesCreated.clear();
   }

   public void rollback() {
      recoveryMgr.rollback();
      concurMgr.release();
      mybuffers.unpinAll();
      if (!newlyAppendedBlocks.isEmpty()) {
         BlockId lastAppendedBlock = newlyAppendedBlocks.get(newlyAppendedBlocks.size() - 1);
         fm.truncate(lastAppendedBlock.filename(), lastAppendedBlock.number());
      }
      active = false;
      newlyAppendedBlocks.clear();
      activeTransactions.remove(this);

      // Delete temporary files created in this transaction
      for (String tempFileName : tempFilesCreated) {
         fileMgr.deleteFile(tempFileName);
      }
      tempFilesCreated.clear();
   }

   public void addTempFile(String tempFileName) {
      tempFilesCreated.add(tempFileName);
   }


   public void recover() {
      bm.flushAll(txnum);
      recoveryMgr.recover();
   }

   public void pin(BlockId blk) {
      concurMgr.sLock(blk);
      mybuffers.pin(blk);
   }

   public void unpin(BlockId blk) {
      mybuffers.unpin(blk);
   }

   public int getInt(BlockId blk, int offset) {
      concurMgr.sLock(blk);
      Buffer buff = mybuffers.getBuffer(blk);
      return buff.contents().getInt(offset);
   }

   public String getString(BlockId blk, int offset) {
      concurMgr.sLock(blk);
      Buffer buff = mybuffers.getBuffer(blk);
      return buff.contents().getString(offset);
   }

   public void setInt(BlockId blk, int offset, int val, boolean okToLog) {
      concurMgr.xLock(blk);
      Buffer buff = mybuffers.getBuffer(blk);
      int lsn = -1;
      if (okToLog)
         lsn = recoveryMgr.setInt(buff, offset, val);
      buff.contents().setInt(offset, val);
      buff.setModified(txnum, lsn);
   }

   public void setString(BlockId blk, int offset, String val, boolean okToLog) {
      concurMgr.xLock(blk);
      Buffer buff = mybuffers.getBuffer(blk);
      int lsn = -1;
      if (okToLog)
         lsn = recoveryMgr.setString(buff, offset, val);
      buff.contents().setString(offset, val);
      buff.setModified(txnum, lsn);
   }

   public int size(String filename) {
      BlockId dummyblk = new BlockId(filename, END_OF_FILE);
      concurMgr.sLock(dummyblk);
      return fm.length(filename);
   }

   public BlockId append(String filename) {
      BlockId dummyblk = new BlockId(filename, END_OF_FILE);
      concurMgr.xLock(dummyblk);
      BlockId newBlock = fm.append(filename);
      newlyAppendedBlocks.add(newBlock);
      return newBlock;
   }

   public int blockSize() {
      return fm.blockSize();
   }

   public int availableBuffs() {
      return bm.available();
   }

   public static void performQuiescentCheckpoint(LogMgr lm) {
      synchronized (checkpointLock) {
         isCheckpointing = true;
         for (Transaction tx : activeTransactions) {
            while (tx.isActive()) {
               try {
                  checkpointLock.wait();
               } catch (InterruptedException e) {
                  throw new RuntimeException("Interrupted during checkpointing.", e);
               }
            }
         }
         int lsn = lm.writeCheckpointRecord(activeTransactions);
         lm.flush(lsn);
         isCheckpointing = false;
         checkpointLock.notifyAll();
      }
   }

   public static void performNonQuiescentCheckpoint(LogMgr lm) {
      synchronized (checkpointLock) {
         int lsn = lm.writeCheckpointRecord(activeTransactions);
         lm.flush(lsn);
      }
   }

   private boolean isActive() {
      return this.active;
   }

   private List<BlockId> getNewlyAppendedBlocks() {
      return new ArrayList<>(this.newlyAppendedBlocks);
   }

   private static synchronized int nextTxNumber() {
      int lastTxNum = readLastTxNum();
      nextTxNum = lastTxNum + 1;
      saveLastTxNum(nextTxNum);
      return nextTxNum;
   }
   private static void saveLastTxNum(int txNum) {
      try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(TXNUM_STORAGE_FILE))) {
         dos.writeInt(txNum);
      } catch (IOException e) {
         throw new RuntimeException("Unable to save transaction number: " + e.getMessage(), e);
      }
   }
   private static int readLastTxNum() {
      File file = new File(TXNUM_STORAGE_FILE);
      if (!file.exists()) {
         return 0;
      }
      try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
         return dis.readInt();
      } catch (IOException e) {
         throw new RuntimeException("Unable to read transaction number: " + e.getMessage(), e);
      }
   }
}
