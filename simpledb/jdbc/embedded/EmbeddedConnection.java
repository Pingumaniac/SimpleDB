package simpledb.jdbc.embedded;

import java.sql.SQLException;
import simpledb.file.FileMgr;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.jdbc.ConnectionAdapter;

class EmbeddedConnection extends ConnectionAdapter {
   private SimpleDB db;
   private Transaction currentTx;
   private Planner planner;

   public EmbeddedConnection(SimpleDB db) {
      this.db = db;
      currentTx = db.newTx();
      planner = db.planner();
   }

   public EmbeddedStatement createStatement() throws SQLException {
      return new EmbeddedStatement(this, planner);
   }

   public void close() throws SQLException {
      currentTx.commit();
      printStats();
   }

   public void commit() throws SQLException {
      currentTx.commit();
      printStats();
      currentTx = db.newTx();
   }

   public void rollback() throws SQLException {
      currentTx.rollback();
      printStats();
      currentTx = db.newTx();
   }

   Transaction getTransaction() {
      return currentTx;
   }

   private void printStats() {
      FileMgr fileMgr = db.getFileMgr();
      System.out.println("Blocks read: " + fileMgr.getBlocksRead());
      System.out.println("Blocks written: " + fileMgr.getBlocksWritten());
   }
}
