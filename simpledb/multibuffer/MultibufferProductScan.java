package simpledb.multibuffer;

import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.record.*;

public class MultibufferProductScan implements Scan {
   private Transaction tx;
   private Scan lhsscan, rhsscan=null, prodscan;
   private String filename;
   private Layout layout;
   private int chunksize, nextblknum, filesize;

   public MultibufferProductScan(Transaction tx, Scan lhsscan, String tblname, Layout layout) {
      this.tx = tx;
      this.lhsscan = lhsscan;
      this.filename = tblname + ".tbl";
      this.layout = layout;
      filesize = tx.size(filename);
      int available = tx.availableBuffs();
      chunksize = BufferNeeds.bestFactor(available, filesize);
      beforeFirst();
   }

   public void beforeFirst() {
      nextblknum = 0;
      useNextChunk();
   }

   public boolean next() {
      while (!prodscan.next()) 
         if (!useNextChunk())
         return false;
      return true;
   }

   public void close() {
      prodscan.close();
   }

   public Constant getVal(String fldname) {
      return prodscan.getVal(fldname);
   }

   public int getInt(String fldname) {
      return prodscan.getInt(fldname);
   }

   public String getString(String fldname) {
      return prodscan.getString(fldname);
   }

   public boolean hasField(String fldname) {
      return prodscan.hasField(fldname);
   }
   
   private boolean useNextChunk() {
      if (nextblknum >= filesize)
         return false;
      if (rhsscan != null)
         rhsscan.close();
      int end = nextblknum + chunksize - 1;
      if (end >= filesize)
         end = filesize - 1;
      rhsscan = new ChunkScan(tx, filename, layout, nextblknum, end);
      lhsscan.beforeFirst();
      prodscan = new ProductScan(lhsscan, rhsscan);
      nextblknum = end + 1;
      return true;
   }
}

