package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record.*;

public interface Plan {

   public Scan   open();

   public int    blocksAccessed();

   public int    recordsOutput();

   public int    distinctValues(String fldname);

   public Schema schema();
}
