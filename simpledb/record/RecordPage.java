package simpledb.record;

import static java.sql.Types.INTEGER;
import simpledb.file.*;
import simpledb.tx.Transaction;

/**
 * Store a record at a given location in a block. 
 * @author Edward Sciore
 */
public class RecordPage {
   public static final int EMPTY = 0, USED = 1;
   private Transaction tx;
   private BlockId blk;
   private Layout layout;

   public RecordPage(Transaction tx, BlockId blk, Layout layout) {
      this.tx = tx;
      this.blk = blk;
      this.layout = layout;
   }

   /**
    * Return the integer value stored for the
    * specified field of a specified slot.
    * @param fldname the name of the field.
    * @return the integer stored in that field
    */
   public int getInt(int slot, String fldname) {
      tx.pin(blk);  // Pin the block at the beginning
      int fldpos = offset(slot) + layout.offset(fldname);
      int intValue = tx.getInt(blk, fldpos);
      tx.unpin(blk);  // Unpin the block at the end
      return intValue
   }

   /**
    * Return the string value stored for the
    * specified field of the specified slot.
    * @param fldname the name of the field.
    * @return the string stored in that field
    */
   public String getString(int slot, String fldname) {
      tx.pin(blk);  // Pin the block at the beginning
      int fldpos = offset(slot) + layout.offset(fldname);
      String stringValue = tx.getString(blk, fldpos);
      tx.unpin(blk);  // Unpin the block at the end
      return stringValue;
   }

   public String getVarchar(int slot, String fldname) {
      tx.pin(blk);  // Pin the block at the beginning
      int fldpos = offset(slot) + layout.offset(fldname);
      int length = tx.getInt(blk, fldpos); // Read the length of the VARCHAR
      String stringValue =  tx.getString(blk, fldpos + Integer.BYTES, length);
      tx.unpin(blk);  // Unpin the block at the end
      return stringValue
   }

   public void setVarchar(int slot, String fldname, String val) {
      tx.pin(blk);  // Pin the block at the beginning
      int fldpos = offset(slot) + layout.offset(fldname);
      int length = val.length();
      tx.setInt(blk, fldpos, length); // Write the length of the VARCHAR
      tx.setString(blk, fldpos + Integer.BYTES, val); // Write the actual string
      tx.unpin(blk);  // Unpin the block at the end
   }

   /**
    * Store an integer at the specified field
    * of the specified slot.
    * @param fldname the name of the field
    * @param val the integer value stored in that field
    */
   public void setInt(int slot, String fldname, int val) {
      tx.pin(blk);  // Pin the block at the beginning
      int fldpos = offset(slot) + layout.offset(fldname);
      tx.setInt(blk, fldpos, val, true);
      tx.unpin(blk);  // Unpin the block at the end
   }

   /**
    * Store a string at the specified field
    * of the specified slot.
    * @param fldname the name of the field
    * @param val the string value stored in that field
    */
   public void setString(int slot, String fldname, String val) {
      tx.pin(blk);  // Pin the block at the beginning
      int fldpos = offset(slot) + layout.offset(fldname);
      tx.setString(blk, fldpos, val, true);
      tx.unpin(blk);  // Unpin the block at the end
   }
   
   public void delete(int slot) {
      tx.pin(blk);  // Pin the block at the beginning
      setFlag(slot, EMPTY);
      tx.unpin(blk);  // Unpin the block at the end
   }
   
   /** Use the layout to format a new block of records.
    *  These values should not be logged 
    *  (because the old values are meaningless).
    */ 
   public void format() {
      tx.pin(blk);  // Pin the block at the beginning
      int slot = 0;
      while (isValidSlot(slot)) {
         tx.setInt(blk, offset(slot), EMPTY, false); 
         Schema sch = layout.schema();
         for (String fldname : sch.fields()) {
            int fldpos = offset(slot) + layout.offset(fldname);
            if (sch.type(fldname) == INTEGER)
               tx.setInt(blk, fldpos, 0, false);
            else
               tx.setString(blk, fldpos, "", false);
         }
         slot++;
      }
      tx.unpin(blk);  // Unpin the block at the end
   }

   public int nextAfter(int slot) {
      tx.pin(blk);  // Pin the block at the beginning
      int intValue = searchAfter(slot, USED);
      tx.unpin(blk);  // Unpin the block at the end
      return intValue;
   }
 
   public int insertAfter(int slot) {
      tx.pin(blk);  // Pin the block at the beginning
      int newslot = searchAfter(slot, EMPTY);
      if (newslot >= 0)
         setFlag(newslot, USED);
      tx.unpin(blk);  // Unpin the block at the end
      return newslot;
   }
  
   public BlockId block() {
      return blk;
   }
   
   // Private auxiliary methods
   
   /**
    * Set the record's empty/inuse flag.
    */
   private void setFlag(int slot, int flag) {
      tx.setInt(blk, offset(slot), flag, true); 
   }

   private int searchAfter(int slot, int flag) {
      slot++;
      while (isValidSlot(slot)) {
         if (tx.getInt(blk, offset(slot)) == flag)
            return slot;
         slot++;
      }
      return -1;
   }

   private boolean isValidSlot(int slot) {
      return offset(slot+1) <= tx.blockSize();
   }

   private int offset(int slot) {
      return slot * layout.slotSize();
   }

   public void afterLast() {
      currentslot = numSlots;
   }

   public boolean previous() {
      currentslot--;
      while (currentslot >= 0) {
         if (tx.getInt(blk, offset(currentslot)) == USED) {
            return true;
         }
         currentslot--;
      }
      return false;
   }

}








