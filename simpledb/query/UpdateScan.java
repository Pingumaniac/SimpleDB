package simpledb.query;

import simpledb.record.RID;

public interface UpdateScan extends Scan {
   public void setVal(String fldname, Constant val);

   public void setInt(String fldname, int val);

   public void setString(String fldname, String val);

   public void insert();

   public void delete();

   public RID  getRid();

   public void moveToRid(RID rid);

   public void setShort(String fldname, Short val);

   public void setByteArray(String fldname, byte[] val);

   public void setDate(String fldname, Date val);

   default void setVal(String fldname, Constant val) {
      if (val.asInt() != null) {
         setInt(fldname, val.asInt());
      } else if (val.asShort() != null) {
         setShort(fldname, val.asShort());
      } else if (val.asString() != null) {
         setString(fldname, val.asString());
      } else if (val.asByteArray() != null) {
         setByteArray(fldname, val.asByteArray());
      } else if (val.asDate() != null) {
         setDate(fldname, val.asDate());
      }
      else {
         throw new RuntimeException("Unsupported constant type for setting " + fldname);
      }
   }

   public boolean previous();

   public void afterLast();
}
