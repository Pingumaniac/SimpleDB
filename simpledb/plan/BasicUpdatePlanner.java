package simpledb.plan;

import java.io.*;
import java.util.Base64;
import java.util.Iterator;
import simpledb.tx.Transaction;
import simpledb.parse.*;
import simpledb.query.*;
import simpledb.metadata.MetadataMgr;

public class BasicUpdatePlanner implements UpdatePlanner {
   private MetadataMgr mdm;

   public BasicUpdatePlanner(MetadataMgr mdm) {
      this.mdm = mdm;
   }

   @Override
   public int executeDelete(DeleteData data, Transaction tx) {
      Plan p = new TablePlan(tx, data.tableName(), mdm);
      p = new SelectPlan(p, data.pred());
      UpdateScan us = (UpdateScan) p.open();
      int count = 0;
      while (us.next()) {
         us.delete();
         count++;
      }
      us.close();
      return count;
   }

   @Override
   public int executeModify(ModifyData data, Transaction tx) {
      Plan p = new TablePlan(tx, data.tableName(), mdm);
      p = new SelectPlan(p, data.pred());
      UpdateScan us = (UpdateScan) p.open();

      Constant newVal = data.newValue().evaluate(us);
      String targetField = data.targetField();
      if (!isTypeCorrect(newVal, p.schema(), targetField)) {
         throw new RuntimeException("Type mismatch for field " + targetField);
      }

      int count = 0;
      while (us.next()) {
         us.setVal(targetField, newVal);
         count++;
      }
      us.close();
      return count;
   }

   @Override
   public int executeInsert(InsertData data, Transaction tx) {
      Plan p = new TablePlan(tx, data.tableName(), mdm);
      UpdateScan us = (UpdateScan) p.open();

      if (data.fields().size() != data.vals().size()) {
         throw new RuntimeException("Field list and constant list sizes do not match.");
      }

      Iterator<Constant> iter = data.vals().iterator();
      Schema sch = p.schema();
      us.insert();
      for (String fldname : data.fields()) {
         Constant val = iter.next();
         if (sch.type(fldname) == VARCHAR) {
            if (val.asJavaVal().toString().length() > sch.length(fldname)) {
               throw new RuntimeException("String constant too large for field " + fldname);
            }
         }
         us.setVal(fldname, val);
      }
      us.close();
      return 1;
   }

   @Override
   public int executeCreateTable(CreateTableData data, Transaction tx) {
      mdm.createTable(data.tableName(), data.newSchema(), tx);
      return 0;
   }

   @Override
   public int executeCreateView(CreateViewData data, Transaction tx) {
      try {
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos);
         oos.writeObject(data.queryData());
         oos.close();
         String viewDef = Base64.getEncoder().encodeToString(baos.toByteArray());
         mdm.createView(data.viewName(), viewDef, tx);
         return 0;
      } catch (IOException e) {
         throw new RuntimeException("Error serializing QueryData", e);
      }
   }

   @Override
   public int executeCreateIndex(CreateIndexData data, Transaction tx) {
      mdm.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
      return 0;
   }

   private boolean isTypeCorrect(Constant val, Schema sch, String fldname) {
      int fldType = sch.type(fldname);
      return (val.getType() == fldType) || (fldType == VARCHAR && val.getType() == CHAR);
   }

   private int executeViewUpdate(UpdateData data, Transaction tx) {
      String viewName = data.tableName();
      String viewDef = mdm.getViewDef(viewName, tx);

      if (viewDef == null) {
         throw new UnsupportedOperationException("View not found or not updatable.");
      }

      Parser parser = new Parser(viewDef);
      QueryData viewQueryData = parser.query();

      if (viewQueryData.tables().size() == 1) {
         String baseTableName = viewQueryData.tables().get(0);
         // Create a new UpdateData object for the base table
         UpdateData newUpdateData = new UpdateData(baseTableName, data.targetField(), data.newValue(), data.pred());
         return executeModify(newUpdateData, tx);
      } else {
         throw new UnsupportedOperationException("Complex view updates are not supported.");
      }
   }

}

