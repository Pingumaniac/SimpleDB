package simpledb.jdbc.network;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

// Programming Ex 2.4
public class NetworkDataSource implements DataSource {
    private String serverName;
    private int portNumber;
    private String databaseName;
    private String user;
    private String password;
    private PrintWriter logWriter;
    private int loginTimeout;

    public NetworkDataSource() {
        // Default Constructor with initial setup
        this.serverName = "localhost"; // Default server name
        this.portNumber = 3306;        // Default port number for SimpleDB
        this.databaseName = "simpledb"; // Default database name
        this.user = ""; // Default user
        this.password = ""; // Default password
        this.logWriter = new PrintWriter(System.out);
        this.loginTimeout = 0; // Default login timeout
    }

    @Override
    public Connection getConnection() throws SQLException {
        String connectionUrl = "jdbc:simpledb://" + serverName + ":" + portNumber + "/" + databaseName;
        return DriverManager.getConnection(connectionUrl, user, password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        setUser(username);
        setPassword(password);
        return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    // Vendor-specific methods
    public void setServerName(String serverName) {
        this.serverName = serverName;
    }
    public void setPortNumber(int portNumber) {
        this.portNumber = portNumber;
    }
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
    public void setUser(String user) {
        this.user = user;
    }
    public void setPassword(String password) {
        this.password = password;
    }
}

