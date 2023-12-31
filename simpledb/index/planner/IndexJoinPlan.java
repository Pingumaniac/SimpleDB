package simpledb.index.planner;

import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.IndexInfo;
import simpledb.plan.Plan;
import simpledb.index.Index;
import simpledb.index.query.IndexJoinScan;

public class IndexJoinPlan implements Plan {
   private Plan p1, p2;
   private IndexInfo ii;
   private String joinfield;
   private Schema sch = new Schema();

   public IndexJoinPlan(Plan p1, Plan p2, IndexInfo ii, String joinfield) {
      this.p1 = p1;
      this.p2 = p2;
      this.ii = ii;
      this.joinfield = joinfield;
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
   }

   public Scan open() {
      Scan s = p1.open();
      // throws an exception if p2 is not a tableplan
      TableScan ts = (TableScan) p2.open();
      Index idx = ii.open();
      return new IndexJoinScan(s, idx, joinfield, ts);
   }

   public int blocksAccessed() {
      return p1.blocksAccessed()
              + (p1.recordsOutput() * ii.blocksAccessed())
              + recordsOutput();
   }

   public int recordsOutput() {
      return p1.recordsOutput() * ii.recordsOutput();
   }

   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }

   public Schema schema() {
      return sch;
   }
}
