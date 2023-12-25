package simpledb.plan;

import simpledb.tx.Transaction;
import simpledb.parse.*;

public interface UpdatePlanner {

   public int executeInsert(InsertData data, Transaction tx);

   public int executeDelete(DeleteData data, Transaction tx);

   public int executeModify(ModifyData data, Transaction tx);

   public int executeCreateTable(CreateTableData data, Transaction tx);

   public int executeCreateView(CreateViewData data, Transaction tx);

   public int executeCreateIndex(CreateIndexData data, Transaction tx);
}
