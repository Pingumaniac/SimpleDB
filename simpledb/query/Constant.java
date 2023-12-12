import java.util.Date;

public class Constant implements Comparable<Constant> {
   private Object val;
   private boolean isNull;

   public Constant(Object val) {
      this.val = val;
      this.isNull = (val == null);
   }

   public boolean isNull() {
      return isNull;
   }

   public int asInt() {
      if (isNull) throw new IllegalStateException("Cannot convert null to int");
      return (Integer) val;
   }

   public String asString() {
      if (isNull) throw new IllegalStateException("Cannot convert null to String");
      return (String) val;
   }

   public Short asShort() {
      if (isNull) throw new IllegalStateException("Cannot convert null to Short");
      return (Short) val;
   }

   public byte[] asByteArray() {
      if (isNull) throw new IllegalStateException("Cannot convert null to byte array");
      return (byte[]) val;
   }

   public Date asDate() {
      if (isNull) throw new IllegalStateException("Cannot convert null to Date");
      return (Date) val;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      Constant constant = (Constant) obj;
      if (isNull && constant.isNull) return true;
      if (isNull || constant.isNull) return false;
      return val.equals(constant.val);
   }

   @Override
   public int compareTo(Constant c) {
      if (isNull && c.isNull) return 0;
      if (isNull || c.isNull) throw new IllegalStateException("Cannot compare null values");
      if (val instanceof Comparable && c.val instanceof Comparable) {
         return ((Comparable) val).compareTo(c.val);
      }
      throw new IllegalStateException("Incompatible types for comparison");
   }

   @Override
   public int hashCode() {
      return (isNull) ? 0 : val.hashCode();
   }

   @Override
   public String toString() {
      return (isNull) ? "null" : val.toString();
   }
}
