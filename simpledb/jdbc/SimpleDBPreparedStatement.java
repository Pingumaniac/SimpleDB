package simpledb.jdbc;

import java.sql.*;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;

public class SimpleDBPreparedStatement extends StatementAdapter implements PreparedStatement {
    private Plan plan;
    private Scan scan;
    private String query;
    private SimpleDB db;
    private Map<Integer, Object> parameters = new HashMap<>();


    public SimpleDBPreparedStatement(SimpleDB db, String query) {
        this.db = db;
        this.query = query;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (scan != null) {
            scan.close();
        }
        // Apply parameters to the query
        String parameterizedQuery = applyParametersToQuery(query);
        // Create a query plan using the parameterized query
        plan = db.planner().createQueryPlan(parameterizedQuery);
        scan = plan.open();
        return new SimpleDBResultSet(scan);
    }

    private String applyParametersToQuery(String query) {
        // Assuming SimpleDB uses placeholders like '?' for parameters
        StringBuilder parameterizedQuery = new StringBuilder();
        int index = 1;
        for (int i = 0; i < query.length(); i++) {
            char c = query.charAt(i);
            if (c == '?') {
                parameterizedQuery.append(quoteParameter(parameters.get(index)));
                index++;
            } else {
                parameterizedQuery.append(c);
            }
        }
        return parameterizedQuery.toString();
    }


    @Override
    public void setInt(int parameterIndex, int value) throws SQLException {
        parameters.put(parameterIndex, value);
    }

    @Override
    public void setString(int parameterIndex, String value) throws SQLException {
        parameters.put(parameterIndex, value);
    }

    private String quoteParameter(Object parameter) {
        if (parameter instanceof String) {
            return "'" + parameter.toString() + "'";
        } else {
            return parameter.toString();
        }
    }
}
