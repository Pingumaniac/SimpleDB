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

   RemoteConnectionImpl(SimpleDB db) throws RemoteException {
      this.db = db;
      currentTx = db.newTx();
      planner = db.planner();
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
}
