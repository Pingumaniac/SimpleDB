package simpledb.index.planner;

import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.IndexInfo;
import simpledb.plan.Plan;
import simpledb.index.Index;
import simpledb.index.query.IndexSelectScan;

public class IndexSelectPlan implements Plan {
   private Plan p;
   private IndexInfo ii;
   private Constant val;

   public IndexSelectPlan(Plan p, IndexInfo ii, Constant val) {
      this.p = p;
      this.ii = ii;
      this.val = val;
   }

   public Scan open() {
      // throws an exception if p is not a tableplan.
      TableScan ts = (TableScan) p.open();
      Index idx = ii.open();
      return new IndexSelectScan(ts, idx, val);
   }

   public int blocksAccessed() {
      return ii.blocksAccessed() + recordsOutput();
   }

   public int recordsOutput() {
      return ii.recordsOutput();
   }

   public int distinctValues(String fldname) {
      return ii.distinctValues(fldname);
   }

   public Schema schema() {
      return p.schema(); 
   }
}
