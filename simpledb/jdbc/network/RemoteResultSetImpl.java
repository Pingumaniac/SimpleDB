package simpledb.jdbc.network;

import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.Schema;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

@SuppressWarnings("serial")
class RemoteResultSetImpl extends UnicastRemoteObject implements RemoteResultSet {
   private Scan s;
   private Schema sch;
   private RemoteConnectionImpl rconn;

   public RemoteResultSetImpl(Plan plan, RemoteConnectionImpl rconn) throws RemoteException {
      s = plan.open();
      sch = plan.schema();
      this.rconn = rconn;
   }

   public boolean next() throws RemoteException {
		try {
	      return s.next();
      }
      catch(RuntimeException e) {
         rconn.rollback();
         throw e;
      }
   }

   public int getInt(String fldname) throws RemoteException {
		try {
	      fldname = fldname.toLowerCase(); // to ensure case-insensitivity
	      return s.getInt(fldname);
      }
      catch(RuntimeException e) {
         rconn.rollback();
         throw e;
      }
   }

   public String getString(String fldname) throws RemoteException {
		try {
	      fldname = fldname.toLowerCase(); // to ensure case-insensitivity
	      return s.getString(fldname);
      }
      catch(RuntimeException e) {
         rconn.rollback();
         throw e;
      }
   }

   public RemoteMetaData getMetaData() throws RemoteException {
      return new RemoteMetaDataImpl(sch);
   }

   public void close() throws RemoteException {
      s.close();
      rconn.commit();
   }

   @Override
   public void beforeFirst() throws RemoteException {
      s.beforeFirst();
   }

   @Override
   public boolean absolute(int n) throws RemoteException {
      s.beforeFirst();
      for (int i = 0; i < n; i++) {
         if (!s.next()) {
            return false;
         }
      }
      return true;
   }
}

