package simpledb.tx.concurrency;

import java.util.*;
import simpledb.file.BlockId;
import simpledb.tx.Transaction;

class LockTable {
   private static final long MAX_TIME = 10000; // 10 seconds
   private Map<BlockId, Integer> locks = new HashMap<>();
   private Map<BlockId, List<WaitingTransaction>> waitLists = new HashMap<>();
   private Map<Integer, Long> transactionTimestamps = new HashMap<>();

   public synchronized void sLock(BlockId blk, int txnum) {
      try {
         long timestamp = System.currentTimeMillis();
         transactionTimestamps.put(txnum, timestamp);

         while (hasXlock(blk) && !waitingTooLong(timestamp)) {
            if (waitDieLogic(txnum, blk)) { // Wait-Die logic
               throw new LockAbortException();
            }
            WaitingTransaction wt = new WaitingTransaction(txnum, Thread.currentThread());
            waitLists.computeIfAbsent(blk, k -> new ArrayList<>()).add(wt);
            wait(MAX_TIME);
         }
         if (hasXlock(blk))
            throw new LockAbortException();
         int val = getLockVal(blk);
         locks.put(blk, val + 1);
      } catch (InterruptedException e) {
         throw new LockAbortException();
      }
   }

   public synchronized void xLock(BlockId blk, int txnum) {
      try {
         long timestamp = System.currentTimeMillis();
         transactionTimestamps.put(txnum, timestamp);

         while ((hasOtherSLocks(blk) || hasXlock(blk)) && !waitingTooLong(timestamp)) {
            if (woundWaitLogic(txnum, blk)) { // Wound-Wait logic
               throw new LockAbortException();
            }
            WaitingTransaction wt = new WaitingTransaction(txnum, Thread.currentThread());
            waitLists.computeIfAbsent(blk, k -> new ArrayList<>()).add(wt);
            wait(MAX_TIME);
         }
         locks.put(blk, -1);
      } catch (InterruptedException e) {
         throw new LockAbortException();
      }
   }

   public synchronized void unlock(BlockId blk, int txnum) {
      int val = getLockVal(blk);
      if (val > 1)
         locks.put(blk, val - 1);
      else {
         locks.remove(blk);
         notifyWaitingTransactions(blk);
      }
   }

   private boolean hasXlock(BlockId blk) {
      return getLockVal(blk) < 0;
   }

   private boolean hasOtherSLocks(BlockId blk) {
      return getLockVal(blk) > 1;
   }

   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }

   private int getLockVal(BlockId blk) {
      Integer val = locks.get(blk);
      return val == null ? 0 : val;
   }

   private void notifyWaitingTransactions(BlockId blk) {
      List<WaitingTransaction> waitList = waitLists.get(blk);
      if (waitList != null && !waitList.isEmpty()) {
         WaitingTransaction wt = waitList.remove(0);
         synchronized (wt.thread) {
            wt.thread.notify();
         }
      }
   }

   private boolean waitDieLogic(int txnum, BlockId blk) {
      Long txTimestamp = transactionTimestamps.get(txnum);
      for (Map.Entry<Integer, Long> entry : transactionTimestamps.entrySet()) {
         if (locks.containsKey(blk) && locks.get(blk) != -1 && txTimestamp != null) {
            int otherTxNum = entry.getKey();
            Long otherTxTimestamp = entry.getValue();
            // If current transaction is older and waiting for a lock held by a younger
            // transaction, it should abort.
            if (otherTxNum != txnum && otherTxTimestamp != null && txTimestamp > otherTxTimestamp) {
               return true;
            }
         }
      }
      return false;
   }

   private boolean woundWaitLogic(int txnum, BlockId blk) {
      Long txTimestamp = transactionTimestamps.get(txnum);
      for (Map.Entry<Integer, Long> entry : transactionTimestamps.entrySet()) {
         if (locks.containsKey(blk) && locks.get(blk) != -1 && txTimestamp != null) {
            int otherTxNum = entry.getKey();
            Long otherTxTimestamp = entry.getValue();
            // If current transaction is older and a younger transaction holds the lock,
            // wound the younger transaction.
            if (otherTxNum != txnum && otherTxTimestamp != null && txTimestamp < otherTxTimestamp) {
               return true;
            }
         }
      }
      return false;
   }
   private static class WaitingTransaction {
      int txnum;
      Thread thread;

      WaitingTransaction(int txnum, Thread thread) {
         this.txnum = txnum;
         this.thread = thread;
      }
   }
}
