package simpledb.index.planner;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.metadata.*;
import simpledb.query.*;
import simpledb.parse.*;
import simpledb.plan.*;
import simpledb.index.Index;

public class IndexUpdatePlanner implements UpdatePlanner {
   private MetadataMgr mdm;
   
   public IndexUpdatePlanner(MetadataMgr mdm) {
      this.mdm = mdm;
   }
   
   public int executeInsert(InsertData data, Transaction tx) {
      String tblname = data.tableName();
      Plan p = new TablePlan(tx, tblname, mdm);
      
      // first, insert the record
      UpdateScan s = (UpdateScan) p.open();
      s.insert();
      RID rid = s.getRid();
      
      // then modify each field, inserting an index record if appropriate
      Map<String,IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);
      Iterator<Constant> valIter = data.vals().iterator();
      for (String fldname : data.fields()) {
         Constant val = valIter.next();
         s.setVal(fldname, val);
         
         IndexInfo ii = indexes.get(fldname);
         if (ii != null) {
            Index idx = ii.open();
            idx.insert(val, rid);
            idx.close();
         }
      }
      s.close();
      return 1;
   }
   
   public int executeDelete(DeleteData data, Transaction tx) {
      String tblname = data.tableName();
      Plan p = new TablePlan(tx, tblname, mdm);
      p = new SelectPlan(p, data.pred());

      // Check for an appropriate index for the predicate
      Map<String,IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);
      for (String fldname : indexes.keySet()) {
         if (data.pred().equatesWithField(fldname) != null) {
            IndexInfo ii = indexes.get(fldname);
            p = new IndexSelectPlan(p, ii, data.pred().equatesWithConstant(fldname), tx);
            break;
         }
      }
      
      UpdateScan s = (UpdateScan) p.open();
      int count = 0;
      while(s.next()) {
         // first, delete the record's RID from every index
         RID rid = s.getRid();
         for (String fldname : indexes.keySet()) {
            Constant val = s.getVal(fldname);
            Index idx = indexes.get(fldname).open();
            idx.delete(val, rid);
            idx.close();
         }
         // then delete the record
         s.delete();
         count++;
      }
      s.close();
      return count;
   }

   public int executeModify(ModifyData data, Transaction tx) {
      String tblname = data.tableName();
      String fldname = data.targetField();
      Plan p = new TablePlan(tx, tblname, mdm);
      Predicate pred = data.pred();

      // Check for an appropriate index for the modification field
      Map<String, IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);
      IndexInfo ii = indexes.get(fldname);

      // Use IndexSelectPlan if there's an index on the target field and the predicate allows
      if (ii != null && pred.equatesWithField(fldname) != null) {
         Constant val = pred.equatesWithConstant(fldname);
         p = new IndexSelectPlan(p, ii, val, tx);
      } else {
         p = new SelectPlan(p, pred);
      }

      UpdateScan s = (UpdateScan) p.open();
      int count = 0;
      while (s.next()) {
         // first, update the record
         Constant newval = data.newValue().evaluate(s);
         s.setVal(fldname, newval);

         // then update the appropriate index, if it exists
         if (ii != null) {
            RID rid = s.getRid();
            Constant oldval = s.getVal(fldname);
            Index idx = ii.open();
            idx.delete(oldval, rid);
            idx.insert(newval, rid);
            idx.close();
         }
         count++;
      }
      s.close();
      return count;
   }
   
   public int executeCreateTable(CreateTableData data, Transaction tx) {
      mdm.createTable(data.tableName(), data.newSchema(), tx);
      return 0;
   }
   
   public int executeCreateView(CreateViewData data, Transaction tx) {
      mdm.createView(data.viewName(), data.viewDef(), tx);
      return 0;
   }
   
   public int executeCreateIndex(CreateIndexData data, Transaction tx) {
      mdm.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
      return 0;
   }
}
