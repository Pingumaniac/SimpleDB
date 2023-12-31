package simpledb.jdbc.network;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import simpledb.server.SimpleDB;

@SuppressWarnings("serial")
public class RemoteDriverImpl extends UnicastRemoteObject implements RemoteDriver {
   private SimpleDB db;

   public RemoteDriverImpl(SimpleDB db) throws RemoteException {
      this.db = db;
   }

   public RemoteConnection connect() throws RemoteException {
      // Extract username and password from props
      String username = props.getProperty("user");
      String password = props.getProperty("password");

      // Authenticate user
      if (!authenticateUser(username, password)) {
         throw new RemoteException("User authentication failed.");
      }

      return new RemoteConnectionImpl(db);
   }

   private boolean authenticateUser(String username, String password) {
      try {
         Path path = Paths.get("path/to/credentials.txt");
         List<String> lines = Files.readAllLines(path);

         for (String line : lines) {
            String[] credentials = line.split(":");
            if (credentials[0].equals(username) && credentials[1].equals(password)) {
               return true;
            }
         }
         return false;
      } catch (IOException e) {
         e.printStackTrace();
         return false;
      }
   }
}

