package simpledb.query;

public class ProductScan implements Scan {
   private Scan s1, s2;
   private boolean firstTime;
   private boolean atEnd;

   public ProductScan(Scan s1, Scan s2) {
      this.s1 = s1;
      this.s2 = s2;
      beforeFirst();
   }

   public void beforeFirst() {
      s1.beforeFirst();
      s2.beforeFirst();
      firstTime = true;
      atEnd = false;
   }

   public boolean next() {
      if (atEnd) {
         return false;
      }
      if (firstTime) {
         firstTime = false;
      } else {
         if (!s2.next()) {
            s2.beforeFirst();
            if (!s2.next()) {
               atEnd = true;
               return false;
            }
         }
      }
      return true;
   }

   public boolean previous() {
      if (atEnd) {
         atEnd = false;
         return true;
      }
      if (!firstTime && s2.previous()) {
         return true;
      }
      if (s1.previous()) {
         s2.beforeFirst();
         if (s2.next()) {
            return true;
         }
      }
      atEnd = true;
      return false;
   }

   public int getInt(String fldname) {
      if (!hasField(fldname)) {
         throw new IllegalArgumentException("Field " + fldname + " not found in ProductScan.");
      }
      if (s1.hasField(fldname)) {
         return s1.getInt(fldname);
      } else {
         return s2.getInt(fldname);
      }
   }

   public String getString(String fldname) {
      if (!hasField(fldname)) {
         throw new IllegalArgumentException("Field " + fldname + " not found in ProductScan.");
      }
      if (s1.hasField(fldname)) {
         return s1.getString(fldname);
      } else {
         return s2.getString(fldname);
      }
   }

   public Constant getVal(String fldname) {
      if (!hasField(fldname)) {
         throw new IllegalArgumentException("Field " + fldname + " not found in ProductScan.");
      }
      if (s1.hasField(fldname)) {
         return s1.getVal(fldname);
      } else {
         return s2.getVal(fldname);
      }
   }

   public boolean hasField(String fldname) {
      return s1.hasField(fldname) || s2.hasField(fldname);
   }

   public void close() {
      s1.close();
      s2.close();
   }

   public void afterLast() {
      atEnd = true;
   }
}
