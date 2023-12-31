package simpledb.jdbc.embedded;

import java.sql.*;
import simpledb.record.Schema;
import simpledb.query.Scan;
import simpledb.plan.Plan;
import simpledb.jdbc.ResultSetAdapter;

public class EmbeddedResultSet extends ResultSetAdapter {
   private Scan s;
   private Schema sch;
   private EmbeddedConnection conn;

   public EmbeddedResultSet(Plan plan, EmbeddedConnection conn) throws SQLException {
      s = plan.open();
      sch = plan.schema();
      this.conn = conn;
   }

   public boolean next() throws SQLException {
      try {
         return s.next();
      }
      catch(RuntimeException e) {
         conn.rollback();
         throw new SQLException(e);
      }
   }

   public int getInt(String fldname) throws SQLException {
      try {
         fldname = fldname.toLowerCase(); // to ensure case-insensitivity
         return s.getInt(fldname);
      }
      catch(RuntimeException e) {
         conn.rollback();
         throw new SQLException(e);
      }
   }

   public String getString(String fldname) throws SQLException {
      try {
         fldname = fldname.toLowerCase(); // to ensure case-insensitivity
         return s.getString(fldname);
      }
      catch(RuntimeException e) {
         conn.rollback();
         throw new SQLException(e);
      }
   }

   public ResultSetMetaData getMetaData() throws SQLException {
      return new EmbeddedMetaData(sch);
   }

   public void close() throws SQLException {
      s.close();
      conn.commit();
   }

   @Override
   public void beforeFirst() throws SQLException {
      s.beforeFirst();
   }

   @Override
   public boolean absolute(int n) throws SQLException {
      s.beforeFirst();
      for (int i = 0; i < n; i++) {
         if (!s.next()) {
            return false;
         }
      }
      return true;
   }
}

