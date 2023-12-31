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
   private List<EmbeddedStatement> openStatements;
   private boolean autoCommit = true;

   public EmbeddedConnection(SimpleDB db) {
      this.db = db;
      currentTx = db.newTx();
      planner = db.planner();
      openStatements = new ArrayList<>();
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

   @Override
   public void close() throws SQLException {
      for (EmbeddedStatement stmt : openStatements) {
         if (stmt != null) {
            stmt.close();
         }
      }
      openStatements.clear();
   }

   @Override
   protected void finalize() throws Throwable {
      try {
         close();
      } finally {
         super.finalize();
      }
   }

   public void setAutoCommit(boolean ac) throws SQLException {
      autoCommit = ac;
      if(autoCommit) {
         // Commit any pending transactions
         commit();
      }
   }

   public boolean getAutoCommit() throws SQLException {
      return autoCommit;
   }

   public void commit() throws SQLException {
      currentTx.commit();
      if(autoCommit) {
         // Start a new transaction after the commit
         currentTx = db.newTx();
      }
   }

   public void rollback() throws SQLException {
      currentTx.rollback();
      if(autoCommit) {
         // Start a new transaction after the rollback
         currentTx = db.newTx();
      }
   }
}
