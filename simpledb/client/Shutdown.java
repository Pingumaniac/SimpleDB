package simpledb.client;

import simpledb.remote.RemoteDriver;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Shutdown {
    public static void main(String[] args) {
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", 1099);
            RemoteDriver rdvr = (RemoteDriver) registry.lookup("simpledb");

            rdvr.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
