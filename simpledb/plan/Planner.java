package simpledb.plan;

import simpledb.tx.Transaction;
import simpledb.parse.*;
import simpledb.metadata.MetadataMgr;

public class Planner {
   private QueryPlanner qplanner;
   private UpdatePlanner uplanner;

   public Planner(QueryPlanner qplanner, UpdatePlanner uplanner) {
      this.qplanner = qplanner;
      this.uplanner = uplanner;
   }

   public Plan createQueryPlan(String qry, Transaction tx) {
      Parser parser = new Parser(qry);
      QueryData data = parser.query();
      verifyQuery(data);
      return qplanner.createPlan(data, tx);
   }

   public int executeUpdate(String cmd, Transaction tx) {
      Parser parser = new Parser(cmd);
      Object data = parser.updateCmd();
      verifyUpdate(data);
      if (data instanceof InsertData)
         return uplanner.executeInsert((InsertData) data, tx);
      else if (data instanceof DeleteData)
         return uplanner.executeDelete((DeleteData) data, tx);
      else if (data instanceof ModifyData)
         return uplanner.executeModify((ModifyData) data, tx);
      else if (data instanceof CreateTableData)
         return uplanner.executeCreateTable((CreateTableData) data, tx);
      else if (data instanceof CreateViewData)
         return uplanner.executeCreateView((CreateViewData) data, tx);
      else if (data instanceof CreateIndexData)
         return uplanner.executeCreateIndex((CreateIndexData) data, tx);
      else
         return 0;
   }

   private void verifyQuery(QueryData data) {
      for (String tblName : data.tables()) {
         if (!tableExists(tblName)) {
            throw new NonexistentTableException(tblName);
         }
      }

      for (String fieldName : data.fields()) {
         if (!fieldExists(fieldName)) {
            throw new NonexistentFieldException(fieldName);
         }
         if (isFieldAmbiguous(fieldName, data.tables())) {
            throw new AmbiguousFieldException(fieldName);
         }
      }
   }

   private void verifyUpdate(Object data) {
      if (data instanceof InsertData) {
         InsertData insertData = (InsertData) data;
         if (!tableExists(insertData.tableName())) {
            throw new NonexistentTableException(insertData.tableName());
         }
      } else if (data instanceof DeleteData) {
         DeleteData deleteData = (DeleteData) data;
         if (!tableExists(deleteData.tableName())) {
            throw new NonexistentTableException(deleteData.tableName());
         }
      } else if (data instanceof ModifyData) {
         ModifyData modifyData = (ModifyData) data;
         if (!tableExists(modifyData.tableName())) {
            throw new NonexistentTableException(modifyData.tableName());
         }
      }
   }

   private boolean tableExists(String tableName) {
      return MetadataMgr.tableExists(tableName);
   }

   private boolean fieldExists(String fieldName) {
      return MetadataMgr.fieldExists(fieldName);
   }

   private boolean isFieldAmbiguous(String fieldName, List<String> tables) {
      int count = 0;
      for (String table : tables) {
         if (MetadataMgr.fieldExistsInTable(fieldName, table)) {
            count++;
         }
      }
      return count > 1;
   }
}

class NonexistentTableException extends RuntimeException {
   public NonexistentTableException(String tableName) {
      super("Table '" + tableName + "' does not exist.");
   }
}

class NonexistentFieldException extends RuntimeException {
   public NonexistentFieldException(String fieldName) {
      super("Field '" + fieldName + "' does not exist.");
   }
}

class AmbiguousFieldException extends RuntimeException {
   public AmbiguousFieldException(String fieldName) {
      super("Field name '" + fieldName + "' is ambiguous.");
   }
}
