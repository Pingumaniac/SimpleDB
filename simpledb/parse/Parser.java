package simpledb.parse;

import java.util.*;

import simpledb.query.*;
import simpledb.record.*;

public class Parser {
   private Lexer lex;

   public Parser(String s) {
      lex = new Lexer(s);
   }

   // Methods for parsing predicates, terms, expressions, constants, and fields

   public String field() {
      return lex.eatId();
   }

   public Constant constant() {
      if (lex.matchStringConstant())
         return new Constant(lex.eatStringConstant());
      else
         return new Constant(lex.eatIntConstant());
   }

   public Expression expression() {
      Expression expr = null;
      if (lex.matchId()) {
         expr = new Expression(field());
      } else {
         expr = new Expression(constant());
      }
      while (lex.matchDelim('+') || lex.matchDelim('-') || lex.matchDelim('*') || lex.matchDelim('/')) {
         char op = lex.eatDelim();
         Expression expr2 = expression();
         expr = new Expression(expr, op, expr2);
      }
      return expr;
   }

   public Term term() {
      Expression lhs = expression();
      RelationalOp rop = relationalOp();
      Expression rhs = expression();
      return new Term(lhs, rop, rhs);
   }

   private RelationalOp relationalOp() {
      if (lex.matchDelim('=')) {
         lex.eatDelim('=');
         return RelationalOp.EQ;
      } // Add other relational operators here
      throw new IllegalStateException("Syntax error in relational operation");
   }

   public Predicate predicate() {
      Predicate pred = new Predicate(term());
      while (lex.matchKeyword("and") || lex.matchKeyword("or")) {
         boolean isAnd = lex.matchKeyword("and");
         if (isAnd) lex.eatKeyword("and");
         else lex.eatKeyword("or");
         Predicate pred2 = predicate();
         if (isAnd) {
            pred.conjoinWith(pred2);
         } else {
            pred.disjoinWith(pred2);
         }
      }
      return pred;
   }

   // Methods for parsing queries

   public QueryData query() {
      lex.eatKeyword("select");
      List<String> fields = selectList();
      lex.eatKeyword("from");
      Collection<String> tables = tableList();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new QueryData(fields, tables, pred);
   }

   private List<String> selectList() {
      List<String> L = new ArrayList<String>();
      do {
         L.add(field());
         if (!lex.matchDelim(',')) {
            break;
         }
         lex.eatDelim(',');
      } while (true);
      return L;
   }

   private Collection<String> tableList() {
      Collection<String> L = new ArrayList<String>();
      do {
         L.add(lex.eatId());
         if (!lex.matchDelim(',')) {
            break;
         }
         lex.eatDelim(',');
      } while (true);
      return L;
   }

   // Methods for parsing the various update commands

   public Object updateCmd() {
      if (lex.matchKeyword("insert"))
         return insert();
      else if (lex.matchKeyword("delete"))
         return delete();
      else if (lex.matchKeyword("update"))
         return modify();
      else
         return create();
   }

   // Method for parsing delete commands

   public DeleteData delete() {
      lex.eatKeyword("delete");
      lex.eatKeyword("from");
      String tblname = lex.eatId();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new DeleteData(tblname, pred);
   }

   // Methods for parsing insert commands

   public InsertData insert() {
      lex.eatKeyword("insert");
      lex.eatKeyword("into");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      List<String> flds = fieldList();
      lex.eatDelim(')');
      lex.eatKeyword("values");
      lex.eatDelim('(');
      List<Constant> vals = constList();
      lex.eatDelim(')');
      return new InsertData(tblname, flds, vals);
   }

   private List<String> fieldList() {
      List<String> L = new ArrayList<String>();
      do {
         L.add(field());
         if (!lex.matchDelim(',')) {
            break;
         }
         lex.eatDelim(',');
      } while (true);
      return L;
   }

   private List<Constant> constList() {
      List<Constant> L = new ArrayList<Constant>();
      do {
         L.add(constant());
         if (!lex.matchDelim(',')) {
            break;
         }
         lex.eatDelim(',');
      } while (true);
      return L;
   }

   // Method for parsing modify commands

   public ModifyData modify() {
      lex.eatKeyword("update");
      String tblname = lex.eatId();
      lex.eatKeyword("set");
      String fldname = field();
      lex.eatDelim('=');
      Expression newval = expression();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new ModifyData(tblname, fldname, newval, pred);
   }

   // Method for parsing create table commands

   public CreateTableData createTable() {
      lex.eatKeyword("table");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      Schema sch = fieldDefs();
      lex.eatDelim(')');
      return new CreateTableData(tblname, sch);
   }

   private Schema fieldDefs() {
      Schema schema = new Schema();
      do {
         Schema schema2 = fieldDef();
         schema.addAll(schema2);
         if (!lex.matchDelim(',')) {
            break;
         }
         lex.eatDelim(',');
      } while (true);
      return schema;
   }

   private Schema fieldDef() {
      String fldname = field();
      return fieldType(fldname);
   }

   private Schema fieldType(String fldname) {
      Schema schema = new Schema();
      if (lex.matchKeyword("int")) {
         lex.eatKeyword("int");
         schema.addIntField(fldname);
      } else {
         lex.eatKeyword("varchar");
         lex.eatDelim('(');
         int strLen = lex.eatIntConstant();
         lex.eatDelim(')');
         schema.addStringField(fldname, strLen);
      }
      return schema;
   }

   // Method for parsing create view commands

   public CreateViewData createView() {
      lex.eatKeyword("view");
      String viewname = lex.eatId();
      lex.eatKeyword("as");
      QueryData qd = query();
      return new CreateViewData(viewname, qd);
   }

   // Method for parsing create index commands

   public CreateIndexData createIndex() {
      lex.eatKeyword("index");
      String idxname = lex.eatId();
      lex.eatKeyword("on");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      String fldname = field();
      lex.eatDelim(')');
      return new CreateIndexData(idxname, tblname, fldname);
   }
}
