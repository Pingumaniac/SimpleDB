package simpledb.multibuffer;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.materialize.*;
import simpledb.plan.Plan;

public class MultibufferProductPlan implements Plan {
   private Transaction tx;
   private Plan lhs, rhs;
   private Schema schema = new Schema();

   public MultibufferProductPlan(Transaction tx, Plan lhs, Plan rhs) {
      this.tx = tx;
      // Ensure the smaller table is on the right side
      if (lhs.recordsOutput() <= rhs.recordsOutput()) {
         this.lhs = lhs;
         this.rhs = rhs;
      } else {
         this.lhs = rhs;
         this.rhs = lhs;
      }
      // Materialize only if necessary
      this.lhs = materializeIfNecessary(this.lhs);
      this.rhs = materializeIfNecessary(this.rhs);
      schema.addAll(lhs.schema());
      schema.addAll(rhs.schema());
   }

   @Override
   public Scan open() {
      Scan leftscan = lhs.open();
      TempTable tt = copyRecordsFrom(rhs);
      return new MultibufferProductScan(tx, leftscan, tt.tableName(), tt.getLayout());
   }

   public int blocksAccessed() {
      // this guesses at the # of chunks
      int avail = tx.availableBuffs();
      int size = new MaterializePlan(tx, rhs).blocksAccessed();
      int numchunks = size / avail;
      return rhs.blocksAccessed() +
            (lhs.blocksAccessed() * numchunks);
   }

   public int recordsOutput() {
      return lhs.recordsOutput() * rhs.recordsOutput();
   }

   public int distinctValues(String fldname) {
      if (lhs.schema().hasField(fldname))
         return lhs.distinctValues(fldname);
      else
         return rhs.distinctValues(fldname);
   }

   public Schema schema() {
      return schema;
   }

   private TempTable copyRecordsFrom(Plan p) {
      Scan src = p.open();
      Schema sch = p.schema();
      TempTable t = new TempTable(tx, sch);
      UpdateScan dest = (UpdateScan) t.open();
      while (src.next()) {
         dest.insert();
         for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
      }
      src.close();
      dest.close();
      return t;
   }

   private Plan materializeIfNecessary(Plan p) {
      if (!(p instanceof MaterializePlan)) {
         return new MaterializePlan(tx, p);
      }
      return p;
   }
}
