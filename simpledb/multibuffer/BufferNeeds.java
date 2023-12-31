package simpledb.multibuffer;

import simpledb.buffer.BufferMgr;

public class BufferNeeds {
   private static final int RESERVE_BUFFER_COUNT = 2; // Number of buffers to reserve for other operations

   public static int bestRoot(int available, int size, BufferMgr bufferMgr) {
      int avail = calculateAvailableBuffers(available, bufferMgr) - RESERVE_BUFFER_COUNT;
      if (avail <= 1)
         return 1;
      int k = Integer.MAX_VALUE;
      double i = 1.0;
      while (k > avail) {
         i++;
         k = (int)Math.ceil(Math.pow(size, 1/i));
      }
      return k;
   }

   public static int bestFactor(int available, int size, BufferMgr bufferMgr) {
      int avail = calculateAvailableBuffers(available, bufferMgr) - RESERVE_BUFFER_COUNT;
      if (avail <= 1)
         return 1;
      int k = size;
      double i = 1.0;
      while (k > avail) {
         i++;
         k = (int)Math.ceil(size / i);
      }
      return k;
   }

   private static int calculateAvailableBuffers(int totalBuffers, BufferMgr bufferMgr) {
      // Adjust available buffer count based on the buffer reservation system
      return totalBuffers - bufferMgr.getReservedBufferCount();
   }
}
