package simpledb.opt;

import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import simpledb.plan.*;

public class SelingerStylePlanner implements QueryPlanner {
    private MetadataMgr mdm;

    public SelingerStylePlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    @Override
    public Plan createPlan(QueryData data, Transaction tx) {
        // Step 1: Create initial plans for each table
        List<Plan> plans = new ArrayList<>();
        for (String tblname : data.tables()) {
            plans.add(new TablePlan(tx, tblname, mdm));
        }

        // Step 2: Generate all permutations of table plans
        List<List<Plan>> permutations = generatePermutations(plans);

        // Step 3: For each permutation, calculate the cost and keep track of the lowest cost plan
        Plan bestPlan = null;
        double lowestCost = Double.MAX_VALUE;
        for (List<Plan> perm : permutations) {
            Plan permPlan = buildJoinPlan(perm, data.pred());
            double cost = estimateCost(permPlan);
            if (cost < lowestCost) {
                bestPlan = permPlan;
                lowestCost = cost;
            }
        }

        // Step 4: Apply selection and projection
        if (bestPlan != null) {
            bestPlan = new SelectPlan(bestPlan, data.pred());
            bestPlan = new ProjectPlan(bestPlan, data.fields());
        }

        return bestPlan;
    }

    private List<List<Plan>> generatePermutations(List<Plan> plans) {
        if (plans.isEmpty()) {
            List<List<Plan>> result = new ArrayList<>();
            result.add(new ArrayList<>());
            return result;
        }

        List<List<Plan>> permutations = new ArrayList<>();
        Plan firstPlan = plans.remove(0);

        List<List<Plan>> recursivePerms = generatePermutations(plans);
        for (List<Plan> smallerPermed : recursivePerms) {
            for (int index = 0; index <= smallerPermed.size(); index++) {
                List<Plan> temp = new ArrayList<>(smallerPermed);
                temp.add(index, firstPlan);
                permutations.add(temp);
            }
        }
        return permutations;
    }

    private Plan buildJoinPlan(List<Plan> plans, Predicate pred) {
        Plan result = plans.get(0);
        for (int i = 1; i < plans.size(); i++) {
            Plan nextPlan = plans.get(i);
            result = new HashJoinPlan(tx, result, nextPlan, /* join fields */, pred);
        }
        return result;
    }


    private double estimateCost(Plan plan) {
        // Simple estimation based on block accesses and record output
        int blockAccesses = plan.blocksAccessed();
        int numRecords = plan.recordsOutput();
        return blockAccesses + numRecords; // Simplistic cost measure
    }
}
