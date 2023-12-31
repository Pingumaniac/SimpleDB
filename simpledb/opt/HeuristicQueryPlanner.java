package simpledb.opt;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.*;

public class HeuristicQueryPlanner implements QueryPlanner {
   private Collection<TablePlanner> tableplanners = new ArrayList<>();
   private MetadataMgr mdm;

   public HeuristicQueryPlanner(MetadataMgr mdm) {
      this.mdm = mdm;
   }

   @Override
   public Plan createPlan(QueryData data, Transaction tx) {
      // Step 1: Create a TablePlanner object for each mentioned table
      for (String tblname : data.tables()) {
         TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mdm);
         tableplanners.add(tp);
      }

      // Step 2: Choose the order of table processing
      Plan currentplan = null;
      while (!tableplanners.isEmpty()) {
         TablePlanner besttp = getLowestCostPlan();
         if (currentplan == null)
            currentplan = besttp.makeSelectPlan();
         else
            currentplan = besttp.makeJoinPlan(currentplan);
         tableplanners.remove(besttp);
      }

      // Step 3: Add a groupby plan if specified
      if (data.groupFields() != null)
         currentplan = new GroupByPlan(tx, currentplan, data.groupFields(), data.aggregates(), mdm);

      // Step 4: Project on the field names
      return new ProjectPlan(currentplan, data.fields());
   }

   private TablePlanner getLowestCostPlan() {
      TablePlanner lowestTp = null;
      double lowestCost = Double.MAX_VALUE;
      for (TablePlanner tp : tableplanners) {
         double cost = tp.estimatedBlockAccesses();
         if (cost < lowestCost) {
            lowestCost = cost;
            lowestTp = tp;
         }
      }
      return lowestTp;
   }

   public void setPlanner(Planner p) {
      // for use in planning views, which
      // for simplicity this code doesn't do.
   }
}
