package simpledb.buffer;

import simpledb.file.*;
import simpledb.log.LogMgr;

import java.util.*;

public class BufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private Map<BlockId, Buffer> bufferPoolMap;
   private static final long MAX_TIME = 10000; // 10 seconds
   private Deque<Buffer> lruQueue; // For LRU strategy

   public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      bufferPoolMap = new HashMap<>();
      lruQueue = new ArrayDeque<>();

      for (int i = 0; i < numbuffs; i++) {
         bufferpool[i] = new Buffer(fm, lm);
         lruQueue.addLast(bufferpool[i]);
      }
   }

   public synchronized int available() {
      return numAvailable;
   }

   public synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool) {
         if (buff.modifyingTx() == txnum) {
            buff.flush();
         }
      }
   }

   public synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) {
         numAvailable++;
         lruQueue.remove(buff);
         lruQueue.addLast(buff);
         notifyAll();
      }
   }

   public synchronized Buffer pin(BlockId blk) {
      try {
         long timestamp = System.currentTimeMillis();
         Buffer buff = tryToPin(blk);
         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = tryToPin(blk);
         }
         if (buff == null) {
            throw new BufferAbortException();
         }
         return buff;
      } catch (InterruptedException e) {
         throw new BufferAbortException();
      }
   }

   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }

   private Buffer tryToPin(BlockId blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseBuffer();
         if (buff == null) {
            return null;
         }
         buff.assignToBlock(blk);
         bufferPoolMap.put(blk, buff);
      }
      if (!buff.isPinned()) {
         numAvailable--;
         lruQueue.remove(buff);
      }
      buff.pin();
      lruQueue.addLast(buff);
      return buff;
   }

   private Buffer findExistingBuffer(BlockId blk) {
      return bufferPoolMap.getOrDefault(blk, null);
   }

   private Buffer chooseBuffer() {
      Buffer bestBuffer = null;
      long lowestLSN = Long.MAX_VALUE;

      for (Buffer buff : lruQueue) {
         if (!buff.isPinned() && buff.modifyingTx() == -1) {
            return buff; // Return first found unmodified buffer
         }
         if (!buff.isPinned() && buff.modifyingTx() != -1 && buff.getLSN() < lowestLSN) {
            bestBuffer = buff;
            lowestLSN = buff.getLSN();
         }
      }
      return bestBuffer != null ? bestBuffer : lruQueue.peekFirst(); // Fallback to LRU
   }

   // Method to retrieve the LSN from Buffer
   private int getLSN(Buffer buff) {
      return buff.getLSN();
   }
}
