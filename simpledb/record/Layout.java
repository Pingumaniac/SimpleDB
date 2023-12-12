package simpledb.record;

import java.util.*;
import static java.sql.Types.*;
import simpledb.file.Page;


public class Layout {
   private Schema schema;
   private Map<String,Integer> offsets;
   private int slotsize;

   public Layout(Schema schema) {
      this.schema = schema;
      offsets  = new HashMap<>();
      int pos = Integer.BYTES; // leave space for the empty/inuse flag
      for (String fldname : schema.fields()) {
         offsets.put(fldname, pos);
         pos += lengthInBytes(fldname);
      }
      slotsize = pos;
   }

   public Layout(Schema schema, Map<String,Integer> offsets, int slotsize) {
      this.schema    = schema;
      this.offsets   = offsets;
      this.slotsize = slotsize;
   }

   public Schema schema() {
      return schema;
   }

   public int offset(String fldname) {
      return offsets.get(fldname);
   }

   public int slotSize() {
      return slotsize;
   }

   private int lengthInBytes(String fldname) {
      int fldtype = schema.type(fldname);
      if (fldtype == INTEGER)
         return Integer.BYTES;
      else { // fldtype == VARCHAR
         int length = Page.maxLength(schema.length(fldname));
         return padLength(length);
      }
   }

   private int padLength(int length) {
      int remainder = length % 4;
      if (remainder == 0)
         return length;
      else
         return length + 4 - remainder;
   }
   public int bitLocation(String fldname) {
      int index = new ArrayList<>(schema.fields()).indexOf(fldname);
      return index + 1;
   }
}

