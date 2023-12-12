package simpledb.query;

import java.util.*;

import simpledb.plan.Plan;
import simpledb.record.*;

public class Predicate {
   private List<Term> terms = new ArrayList<Term>();
   private Predicate left, right;
   private String connective;

   public Predicate() {}

   public Predicate(Term t) {
      this.terms = Collections.singletonList(t);
   }

   public Predicate(Predicate left, String connective, Predicate right) {
      this.left = left;
      this.connective = connective;
      this.right = right;
   }

   public void conjoinWith(Predicate pred) {
      terms.addAll(pred.terms);
   }

   public boolean isSatisfied(Scan s) {
      boolean leftResult = left != null ? left.isSatisfied(s) : true;
      boolean rightResult = right != null ? right.isSatisfied(s) : true;
      switch (connective) {
         case "and": return leftResult && rightResult;
         case "or": return leftResult || rightResult;
         case "not": return !leftResult; // 'not' only applies to the left part
         default: return terms.stream().allMatch(t -> t.isSatisfied(s));
      }
   }

   public int reductionFactor(Plan p) {
      int factor = 1;
      for (Term t : terms)
         factor *= t.reductionFactor(p);
      return factor;
   }

   public Predicate selectSubPred(Schema sch) {
      Predicate result = new Predicate();
      for (Term t : terms)
         if (t.appliesTo(sch))
            result.terms.add(t);
      if (result.terms.size() == 0)
         return null;
      else
         return result;
   }

   public Predicate joinSubPred(Schema sch1, Schema sch2) {
      Predicate result = new Predicate();
      Schema newsch = new Schema();
      newsch.addAll(sch1);
      newsch.addAll(sch2);
      for (Term t : terms)
         if (!t.appliesTo(sch1)  &&
               !t.appliesTo(sch2) &&
               t.appliesTo(newsch))
            result.terms.add(t);
      if (result.terms.size() == 0)
         return null;
      else
         return result;
   }

   public Constant equatesWithConstant(String fldname) {
      for (Term t : terms) {
         Constant c = t.equatesWithConstant(fldname);
         if (c != null)
            return c;
      }
      return null;
   }

   public String equatesWithField(String fldname) {
      for (Term t : terms) {
         String s = t.equatesWithField(fldname);
         if (s != null)
            return s;
      }
      return null;
   }

   public String toString() {
      Iterator<Term> iter = terms.iterator();
      if (!iter.hasNext()) 
         return "";
      String result = iter.next().toString();
      while (iter.hasNext())
         result += " and " + iter.next().toString();
      return result;
   }
}
