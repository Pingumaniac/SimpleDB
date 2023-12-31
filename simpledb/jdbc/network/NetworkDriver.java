package simpledb.jdbc.network;

import java.rmi.registry.*;
import java.sql.*;
import java.util.Properties;
import simpledb.jdbc.DriverAdapter;

public class NetworkDriver extends DriverAdapter {
   public Connection connect(String url, Properties prop) throws SQLException {
      try {
         // Extract username and password from props
         String username = props.getProperty("user");
         String password = props.getProperty("password");

         // Authenticate user
         if (!authenticateUser(username, password)) {
            throw new SQLException("User authentication failed.");
         }

         String host = url.replace("jdbc:simpledb://", "");  //assumes no port specified
         Registry reg = LocateRegistry.getRegistry(host, 1099);
         RemoteDriver rdvr = (RemoteDriver) reg.lookup("simpledb");
         RemoteConnection rconn = rdvr.connect();
         return new NetworkConnection(rconn);
      }
      catch (Exception e) {
         throw new SQLException(e);
      }
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
