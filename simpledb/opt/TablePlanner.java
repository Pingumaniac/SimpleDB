package simpledb.opt;

import java.util.Map;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;


class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String, IndexInfo> indexes;
   private Transaction tx;

   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
      this.tx = tx;
      this.mypred = mypred;
      myplan = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes = mdm.getIndexInfo(tblname, tx);
   }

   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }

   public Plan makeJoinPlan(Plan current) {
      Plan p = makeProductJoinPlan(current);
      if (p != null)
         return p;
      p = makeIndexJoinPlan(current);
      if (p != null)
         return p;
      return makeHashJoinPlan(current);
   }

   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultibufferProductPlan(tx, current, p);
   }
   
   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            System.out.println("index on " + fldname + " used");
            return new IndexSelectPlan(myplan, ii, val);
         }
      }
      return null;
   }
   
   private Plan makeIndexJoin(Plan current, Schema currsch) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }

   private Plan makeHashJoinPlan(Plan current) {
      for (String fldname : myschema.fields()) {
         if (current.schema().hasField(fldname) && mypred.equatesWithField(fldname) != null) {
            String fldname2 = mypred.equatesWithField(fldname);
            if (mypred.equatesWithConstant(fldname) == null) {
               // Hash join is suitable for equi-join conditions
               return new HashJoinPlan(tx, current, myplan, fldname, fldname2);
            }
         }
      }
      return null; // No suitable equi-join condition for hash join found
   }

   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
      return addJoinPred(p, currsch);
   }
   
   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectSubPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }
   
   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }
}
