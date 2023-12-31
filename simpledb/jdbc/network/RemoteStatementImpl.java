package simpledb.jdbc.network;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.tx.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

@SuppressWarnings("serial")
class RemoteStatementImpl extends UnicastRemoteObject implements RemoteStatement {
   private RemoteConnectionImpl rconn;
   private Planner planner;
   private List<RemoteResultSet> openResultSets;
   
   public RemoteStatementImpl(RemoteConnectionImpl rconn, Planner planner) throws RemoteException {
      this.rconn = rconn;
      this.planner = planner;
      openResultSets = new ArrayList<>();
   }

   public RemoteResultSet executeQuery(String qry) throws RemoteException {
      try {
         System.out.println("Executing query: " + qry);
         System.out.println("Specific string from part (a)");

         Transaction tx = rconn.getTransaction();
         Plan pln = planner.createQueryPlan(qry, tx);
         return new RemoteResultSetImpl(pln, rconn);
      }
      catch (RuntimeException e) {
         rconn.rollback();
         throw e;
      }
   }

   public int executeUpdate(String cmd) throws RemoteException {
      try {
         Transaction tx = rconn.getTransaction();
         int result = planner.executeUpdate(cmd, tx);
         rconn.commit();
         return result;
      }
      catch(RuntimeException e) {
         rconn.rollback();
         throw e;
      }
   }

   @Override
   public void close() throws RemoteException {
      for (RemoteResultSet rs : openResultSets) {
         if (rs != null) {
            rs.close();
         }
      }
      openResultSets.clear();
   }
}
