package simpledb.query;

import java.util.List;

public class ProjectScan implements Scan {
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
}
