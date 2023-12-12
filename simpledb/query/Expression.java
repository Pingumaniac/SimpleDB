package simpledb.query;

import simpledb.record.*;

public class Expression {
   private Constant val = null;
   private String fldname = null;
   private Expression left, right;
   private String operator;
   
   public Expression(Constant val) {
      this.val = val;
   }
   
   public Expression(String fldname) {
      this.fldname = fldname;
   }

   public Expression(Expression left, String operator, Expression right) {
      this.left = left;
      this.operator = operator;
      this.right = right;
   }

   public Constant evaluate(Scan s) {
      return (val != null) ? val : s.getVal(fldname);
   }

   public boolean isFieldName() {
      return fldname != null;
   }

   public Constant asConstant() {
      return val;
   }

   public String asFieldName() {
      return fldname;
   }

   public boolean appliesTo(Schema sch) {
      return (val != null) ? true : sch.hasField(fldname);
   }
   
   public String toString() {
      return (val != null) ? val.toString() : fldname;
   }

   public Constant evaluate(Scan s) {
      if (val != null) {
         return val;
      } else if (fldname != null) {
         return s.getVal(fldname);
      } else {
         // Perform arithmetic operation
         int leftVal = left.evaluate(s).asInt();
         int rightVal = right.evaluate(s).asInt();
         switch (operator) {
            case "+": return new Constant(leftVal + rightVal);
            case "-": return new Constant(leftVal - rightVal);
            case "*": return new Constant(leftVal * rightVal);
            case "/": return new Constant(leftVal / rightVal);
            default: throw new RuntimeException("Invalid operator");
         }
      }
   }
}
