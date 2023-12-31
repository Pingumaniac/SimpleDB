package simpledb.server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import simpledb.remote.RemoteDriver;

public class SimpleDBServer {
    private boolean isShutdown = false;

    public SimpleDBServer() {
        try {
            // Initialize logging
            Logger logger = LoggerFactory.getLogger(SimpleDBServer.class);
            FileHandler fileHandler = new FileHandler("simpledb-server.log");
            logger.addHandler(fileHandler);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void onClientDisconnected(RemoteDriver client) {
        try {
            SimpleDB db = SimpleDB.getInstance();
            db.rollbackTransaction(client.getTransactionId());
            db.releaseLocks(client);
            System.out.println("Client disconnected: " + client);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start() {
        try {
            // RMI registry setup and binding server objects
            SimpleDB db = new SimpleDB("simpledb", SimpleDB.BLOCK_SIZE, SimpleDB.BUFFER_SIZE);
            RemoteDriver d = new SimpleDBRemoteDriverImpl(db);
            RemoteDriver stub = (RemoteDriver) UnicastRemoteObject.exportObject(d, 0);
            Registry reg = LocateRegistry.createRegistry(1099);
            reg.rebind("simpledb", stub);
            System.out.println("SimpleDB server ready");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void shutdown() {
        if (!isShutdown) {
            isShutdown = true;
            // Stop accepting new connections
            // Wait for ongoing transactions to complete
            // Write a quiescent checkpoint to the log
            System.out.println("Server is shutting down.");
        }
    }
}
