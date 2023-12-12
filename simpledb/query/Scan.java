package simpledb.query;

public interface Scan {

   public void beforeFirst();

   public boolean next();

   public int getInt(String fldname);

   public String getString(String fldname);

   public Constant getVal(String fldname);

   public boolean hasField(String fldname);

   public void close();

   public Short getShort(String fldname);

   public byte[] getByteArray(String fldname);

   public Date getDate(String fldname);

   default Constant getVal(String fldname) {
      if (!hasField(fldname)) {
         throw new RuntimeException("Field " + fldname + " not found");
      }
      if (fieldIsInteger(fldname)) {
         return new Constant(getInt(fldname));
      } else if (fieldIsShort(fldname)) {
         return new Constant(getShort(fldname));
      } else if (fieldIsString(fldname)) {
         return new Constant(getString(fldname));
      } else if (fieldIsByteArray(fldname)) {
         return new Constant(getByteArray(fldname));
      } else if (fieldIsDate(fldname)) {
         return new Constant(getDate(fldname));
      }
      throw new RuntimeException("Unsupported field type for " + fldname);
   }
}
