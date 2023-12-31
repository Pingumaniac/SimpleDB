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
   private FileMgr fm; // File manager for file operations

   // New attribute for buffer reservation
   private Map<TransactionId, Set<Buffer>> reservedBuffers;

   public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
      this.fm = fm;
      this.bufferpool = new Buffer[numbuffs];
      this.numAvailable = numbuffs;
      this.bufferPoolMap = new HashMap<>();
      this.lruQueue = new ArrayDeque<>();
      this.reservedBuffers = new HashMap<>();

      // Initialize buffer pool
      for (int i = 0; i < numbuffs; i++)
         bufferpool[i] = new Buffer();
   }

   // Method to reserve buffers for a transaction
   public synchronized void reserveBuffers(TransactionId txId, int numBuffers) {
      if (numBuffers > numAvailable) {
         throw new BufferAbortException();
      }
      Set<Buffer> reservedSet = new HashSet<>();
      for (int i = 0; i < numBuffers; i++) {
         Buffer buff = chooseUnpinnedBuffer();
         if (buff == null) {
            releaseBuffers(txId);
            throw new BufferAbortException();
         }
         buff.pin();
         reservedSet.add(buff);
         numAvailable--;
      }
      reservedBuffers.put(txId, reservedSet);
   }

   // Method to release buffers reserved by a transaction
   public synchronized void releaseBuffers(TransactionId txId) {
      Set<Buffer> buffers = reservedBuffers.get(txId);
      if (buffers != null) {
         for (Buffer buff : buffers) {
            buff.unpin();
            numAvailable++;
         }
         reservedBuffers.remove(txId);
      }
   }

   // Modified pin method
   public synchronized void pin(BlockId blk, TransactionId txId) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null) {
            if (!reservedBuffers.containsKey(txId) || reservedBuffers.get(txId).isEmpty()) {
               throw new BufferAbortException();
            }
            // Use a reserved buffer
            buff = reservedBuffers.get(txId).iterator().next();
            reservedBuffers.get(txId).remove(buff);
            buff.assignToBlock(blk);
         } else {
            buff.assignToBlock(blk);
            bufferPoolMap.put(blk, buff);
         }
      }
      buff.pin();
   }

   // Modified unpin method
   public synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }

   private Buffer findExistingBuffer(BlockId blk) {
      return bufferPoolMap.get(blk);
   }

   private Buffer chooseUnpinnedBuffer() {
      for (Buffer buff : bufferpool) {
         if (!buff.isPinned()) {
            lruQueue.remove(buff); // Remove from LRU queue if it's there
            lruQueue.addLast(buff); // Add to the end of the LRU queue
            return buff;
         }
      }
      return null; // No unpinned buffers available
   }

   public synchronized void releaseTransactionBuffers(TransactionId txId) {
      // Check if the transaction has reserved any buffers
      Set<Buffer> transactionBuffers = reservedBuffers.get(txId);
      if (transactionBuffers != null) {
         // Iterate over each buffer reserved by the transaction
         for (Buffer buff : transactionBuffers) {
            // Unpin the buffer if it is currently pinned
            while (buff.isPinned()) {
               buff.unpin();
            }
            // Remove the buffer from the reserved set
            numAvailable++;
            lruQueue.remove(buff);
         }
         // Remove the transaction's entry from the reservedBuffers map
         reservedBuffers.remove(txId);
      }

      // Also go through all buffers in the pool to unpin any buffers pinned by this transaction
      for (Buffer buffer : bufferpool) {
         if (buffer.pinningTx() == txId) {
            while (buffer.isPinned()) {
               buffer.unpin();
            }
            numAvailable++;
            lruQueue.remove(buffer);
         }
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

   public synchronized int length(String filename) {
      return fm.length(filename);
   }

   public synchronized Buffer append(String filename) {
      BlockId blk = fm.append(filename); // Append a new block
      Buffer buff = pin(blk); // Pin the new block in the buffer pool
      if (buff == null) {
         throw new BufferAbortException();
      }
      return buff;
   }

   public synchronized void flush(BlockId blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff != null) {
         buff.flush();
      }
   }

   public FileMgr getFileMgr() {
      return fm;
   }
}
