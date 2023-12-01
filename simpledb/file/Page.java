package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.time.LocalDate;

public class Page {
   private ByteBuffer bb;
   public static Charset CHARSET = StandardCharsets.US_ASCII;

   public Page(int blocksize) {
      bb = ByteBuffer.allocateDirect(blocksize);
   }

   public Page(byte[] b) {
      bb = ByteBuffer.wrap(b);
   }

   // Programming Ex 3.16: Check if the data fits in the buffer
   private void checkCapacity(int offset, int length) {
      if (offset + length > bb.capacity()) {
         throw new IllegalArgumentException("Not enough space in buffer");
      }
   }

   public int getInt(int offset) {
      return bb.getInt(offset);
   }

   public void setInt(int offset, int n) {
      checkCapacity(offset, Integer.BYTES);
      bb.putInt(offset, n);
   }

   // Additional methods for Programming Ex 3.17
   public short getShort(int offset) {
      return bb.getShort(offset);
   }
   public void setShort(int offset, short n) {
      checkCapacity(offset, Short.BYTES);
      bb.putShort(offset, n);
   }
   public boolean getBoolean(int offset) {
      return bb.get(offset) != 0;
   }
   public void setBoolean(int offset, boolean b) {
      checkCapacity(offset, 1);
      bb.put(offset, (byte) (b ? 1 : 0));
   }
   public LocalDate getDate(int offset) {
      return LocalDate.ofEpochDay(bb.getLong(offset));
   }
   public void setDate(int offset, LocalDate date) {
      checkCapacity(offset, Long.BYTES);
      bb.putLong(offset, date.toEpochDay());
   }

   public byte[] getBytes(int offset) {
      bb.position(offset);
      int length = bb.getInt();
      byte[] b = new byte[length];
      bb.get(b);
      return b;
   }

   public void setBytes(int offset, byte[] b) {
      checkCapacity(offset, Integer.BYTES + b.length);
      bb.position(offset);
      bb.putInt(b.length);
      bb.put(b);
   }

   // Programming Ex 3.18: Implementing strings with delimiter
   public String getString(int offset) {
      bb.position(offset);
      StringBuilder sb = new StringBuilder();
      char ch;
      while ((ch = (char) bb.get()) != '\0') {
         sb.append(ch);
      }
      return sb.toString();
   }

   public void setString(int offset, String s) {
      byte[] b = (s + '\0').getBytes(CHARSET);
      checkCapacity(offset, b.length);
      setBytes(offset, b);
   }

   public static int maxLength(int strlen) {
      float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
      return Integer.BYTES + (strlen * (int) bytesPerChar);
   }

   ByteBuffer contents() {
      bb.position(0);
      return bb;
   }
}
