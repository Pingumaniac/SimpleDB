package simpledb.log;

import java.util.Iterator;
import simpledb.file.*;
import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;

class LogIterator implements Iterator<byte[]> {
   private BufferMgr bufferMgr;
   private BlockId blk;
   private Buffer buffer;
   private Page p;
   private int currentpos;
   private int boundary;

   public LogIterator(BufferMgr bufferMgr, BlockId blk) {
      this.bufferMgr = bufferMgr;
      this.blk = blk;
      buffer = bufferMgr.pin(blk);
      p = buffer.contents();
      moveToBlock(blk);
   }

   public boolean hasNext() {
      return currentpos < p.contents().capacity() || blk.number() > 0;
   }

   public byte[] next() {
      if (currentpos == p.contents().capacity()) {
         bufferMgr.unpin(buffer);
         blk = new BlockId(blk.fileName(), blk.number() - 1);
         buffer = bufferMgr.pin(blk);
         p = buffer.contents();
         moveToBlock(blk);
      }
      byte[] rec = p.getBytes(currentpos);
      currentpos += Integer.BYTES + rec.length;
      return rec;
   }

   private void moveToBlock(BlockId blk) {
      boundary = p.getInt(0);
      currentpos = boundary;
   }

   // Ensure to unpin the buffer when the iterator is no longer in use
   protected void finalize() {
      bufferMgr.unpin(buffer);
   }
}
