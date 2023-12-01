package simpledb.tx.concurrency;

import java.util.*;
import simpledb.file.BlockId;

public class ConcurrencyMgr {

   private static LockTable locktbl = new LockTable();
   private Map<BlockId, String> locks = new HashMap<BlockId, String>();

   public void sLock(BlockId blk) {
      if (locks.get(blk) == null) {
         locktbl.sLock(blk);
         locks.put(blk, "S");
      }
   }

   public void xLock(BlockId blk) {
      if (!hasXLock(blk)) {
         sLock(blk);
         locktbl.xLock(blk);
         locks.put(blk, "X");
      }
   }

   public void release() {
      for (BlockId blk : locks.keySet())
         locktbl.unlock(blk);
      locks.clear();
   }

   public void release(BlockId blk) {
      locktbl.unlock(blk);
      locks.remove(blk);
   }

   private boolean hasXLock(BlockId blk) {
      String locktype = locks.get(blk);
      return locktype != null && locktype.equals("X");
   }
}
