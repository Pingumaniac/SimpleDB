package simpledb.jdbc.embedded;

import java.sql.SQLException;
import simpledb.tx.Transaction;
import simpledb.plan.*;
import simpledb.jdbc.StatementAdapter;

class EmbeddedStatement extends StatementAdapter {
   private EmbeddedConnection conn;
   private Planner planner;
   private List<EmbeddedResultSet> openResultSets;
   
   public EmbeddedStatement(EmbeddedConnection conn, Planner planner) {
      this.conn = conn;
      this.planner = planner;
      openResultSets = new ArrayList<>();
   }

   public EmbeddedResultSet executeQuery(String qry) throws SQLException {
      try {
         Transaction tx = conn.getTransaction();
         Plan pln = planner.createQueryPlan(qry, tx);
         return new EmbeddedResultSet(pln, conn);
      }
      catch(RuntimeException e) {
         conn.rollback();
         throw new SQLException(e);
      }
   }

   public int executeUpdate(String cmd) throws SQLException {
      try {
         Transaction tx = conn.getTransaction();
         int result = planner.executeUpdate(cmd, tx);
         conn.commit();
         return result;
      }
      catch(RuntimeException e) {
         conn.rollback();
         throw new SQLException(e);
      }
   }

   @Override
   public void close() throws SQLException {
      for (EmbeddedResultSet rs : openResultSets) {
         if (rs != null) {
            rs.close();
         }
      }
      openResultSets.clear();
   }
}
