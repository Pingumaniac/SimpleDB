package simpledb.query;

import java.util.List;
import simpledb.record.RID;

public class ProjectScan implements UpdateScan {
   private Scan s;
   private List<String> fieldlist;

   public ProjectScan(Scan s, List<String> fieldlist) {
      this.s = s;
      this.fieldlist = fieldlist;
   }

   public void beforeFirst() {
      s.beforeFirst();
   }

   public boolean next() {
      return s.next();
   }

   public int getInt(String fldname) {
      if (hasField(fldname))
         return s.getInt(fldname);
      else
         throw new RuntimeException("field " + fldname + " not found.");
   }

   public String getString(String fldname) {
      if (hasField(fldname))
         return s.getString(fldname);
      else
         throw new RuntimeException("field " + fldname + " not found.");
   }

   public Constant getVal(String fldname) {
      if (hasField(fldname))
         return s.getVal(fldname);
      else
         throw new RuntimeException("field " + fldname + " not found.");
   }

   public boolean hasField(String fldname) {
      return fieldlist.contains(fldname);
   }

   public void close() {
      s.close();
   }

   // Implement the UpdateScan methods
   @Override
   public void setVal(String fldname, Constant val) {
      if (hasField(fldname)) {
         s.setVal(fldname, val);
      } else {
         throw new RuntimeException("field " + fldname + " not found.");
      }
   }

   @Override
   public void setInt(String fldname, int val) {
      if (hasField(fldname)) {
         s.setInt(fldname, val);
      } else {
         throw new RuntimeException("field " + fldname + " not found.");
      }
   }

   @Override
   public void setString(String fldname, String val) {
      if (hasField(fldname)) {
         s.setString(fldname, val);
      } else {
         throw new RuntimeException("field " + fldname + " not found.");
      }
   }

   @Override
   public void insert() {
      throw new UnsupportedOperationException("Insert operation not supported in ProjectScan.");
   }

   @Override
   public void delete() {
      throw new UnsupportedOperationException("Delete operation not supported in ProjectScan.");
   }

   @Override
   public RID getRid() {
      return s.getRid();
   }

   @Override
   public void moveToRid(RID rid) {
      s.moveToRid(rid);
   }

   @Override
   public void setShort(String fldname, Short val) {
      if (hasField(fldname)) {
         s.setShort(fldname, val);
      } else {
         throw new RuntimeException("field " + fldname + " not found.");
      }
   }

   @Override
   public void setByteArray(String fldname, byte[] val) {
      if (hasField(fldname)) {
         s.setByteArray(fldname, val);
      } else {
         throw new RuntimeException("field " + fldname + " not found.");
      }
   }

   @Override
   public void setDate(String fldname, Date val) {
      if (hasField(fldname)) {
         s.setDate(fldname, val);
      } else {
         throw new RuntimeException("field " + fldname + " not found.");
      }
   }

   // Implement the UpdateScan methods
   @Override
   public void previous() {
      if (!s.previous()) {
         afterLast();
      }
   }

   @Override
   public void afterLast() {
      s.afterLast();
   }
}

