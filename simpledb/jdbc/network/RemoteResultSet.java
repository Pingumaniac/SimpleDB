package simpledb.jdbc.network;

import java.rmi.*;
public interface RemoteResultSet extends Remote {
   public boolean next()                   throws RemoteException;
   public int getInt(String fldname)       throws RemoteException;
   public String getString(String fldname) throws RemoteException;
   public RemoteMetaData getMetaData()     throws RemoteException;
   public void close()                     throws RemoteException;
   void beforeFirst() throws RemoteException;
   boolean absolute(int n) throws RemoteException;
}

