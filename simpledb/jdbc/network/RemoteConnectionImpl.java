package simpledb.jdbc.network;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import simpledb.file.FileMgr;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

class RemoteConnectionImpl extends UnicastRemoteObject implements RemoteConnection {
   private SimpleDB db;
   private Transaction currentTx;
   private Planner planner;
   private List<RemoteStatement> openStatements
   private static final int MAX_CONNECTIONS = 10;
   private static int currentConnections = 0;
   private boolean autoCommit = true;

   RemoteConnectionImpl(SimpleDB db) throws RemoteException {
      this.db = db;
      currentTx = db.newTx();
      planner = db.planner();
      openStatements = new ArrayList<>();
   }

   public RemoteStatement createStatement() throws RemoteException {
      return new RemoteStatementImpl(this, planner);
   }

   public void close() throws RemoteException {
      currentTx.commit();
      printStats();
   }

   Transaction getTransaction() {
      return currentTx;
   }

   void commit() {
      currentTx.commit();
      printStats();
      currentTx = db.newTx();
   }

   void rollback() {
      currentTx.rollback();
      printStats();
      currentTx = db.newTx();
   }

   private void printStats() {
      FileMgr fileMgr = db.getFileMgr();
      System.out.println("Blocks read: " + fileMgr.getBlocksRead());
      System.out.println("Blocks written: " + fileMgr.getBlocksWritten());
   }

   public static synchronized RemoteConnection createNewConnection() throws SQLException {
      if (currentConnections >= MAX_CONNECTIONS) {
         throw new SQLException("Connection limit reached.");
      }
      currentConnections++;
      return new RemoteConnectionImpl();
   }

   // Override close method to decrease the connection count
   @Override
   public void close() throws SQLException {
      // Close resources...
      synchronized (RemoteConnectionImpl.class) {
         currentConnections--;
      }
   }

   @Override
   protected void finalize() throws Throwable {
      try {
         close();
      } finally {
         super.finalize();
      }
   }

   public void setAutoCommit(boolean ac) throws RemoteException {
      autoCommit = ac;
      if (autoCommit) {
         // Commit any pending transactions
         commit();
      }
   }

   public boolean getAutoCommit() throws RemoteException {
      return autoCommit;
   }

   public void commit() throws RemoteException {
      currentTx.commit();
      if (autoCommit) {
         // Start a new transaction after the commit
         currentTx = db.newTx();
      }
   }

   public void rollback() throws RemoteException {
      currentTx.rollback();
      if (autoCommit) {
         // Start a new transaction after the rollback
         currentTx = db.newTx();
      }
   }
}
