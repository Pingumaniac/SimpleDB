package simpledb.parse;

public class PredParser {
   private Lexer lex;

   public PredParser(String s) {
      lex = new Lexer(s);
      System.out.println("Starting parsing with input: " + s);
   }

   public String field() {
      String fld = lex.eatId();
      System.out.println("Field: " + fld);
      return fld;
   }

   public void constant() {
      if (lex.matchStringConstant()) {
         String strConst = lex.eatStringConstant();
         System.out.println("String Constant: " + strConst);
      } else {
         int intConst = lex.eatIntConstant();
         System.out.println("Integer Constant: " + intConst);
      }
   }

   public void expression() {
      if (lex.matchId()) {
         System.out.println("Expression is a field");
         field();
      } else {
         System.out.println("Expression is a constant");
         constant();
      }
   }

   public void term() {
      System.out.println("Parsing Term");
      expression();
      lex.eatDelim('=');
      System.out.println("Operator: =");
      expression();
   }

   public void predicate() {
      System.out.println("Parsing Predicate");
      term();
      while (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         System.out.println("Logical Connector: AND");
         predicate();
      }
   }
}
