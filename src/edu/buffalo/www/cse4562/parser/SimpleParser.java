package edu.buffalo.www.cse4562.parser;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.www.cse4562.Main;
import edu.buffalo.www.cse4562.TableSchema;

/**
 * Gives a basic unoptimized Relational Algebra tree
 */
public class SimpleParser {
	/**
	 * parses an SQL statement and builds a relational algebra operator tree
	 * @param statement a single full SQL Statement
	 * @return boolean false if there was any error while parsing; true otherwise
	 */
	public boolean parse(Statement statement) {
		if (statement instanceof Select) {
			Select selectStmnt = (Select)statement;
			SelectBody selectBody = selectStmnt.getSelectBody();

			if (selectBody instanceof PlainSelect ) {
				PlainSelect select = (PlainSelect)selectBody;
				return parseSelectStatement(select);
			} else if (selectBody instanceof Union) {
				return false;
			}

		} else if (statement instanceof CreateTable) {
			CreateTable createStmnt = (CreateTable)statement;
			/* False in case of tablename already exits*/
			return parseCreateStatement(createStmnt);

		} else {
			return false;
		}
		return false;
	}

	private boolean parseCreateStatement(CreateTable createStmnt) {

		/* Getting Table Specific information from createStmnt*/
		String tabName = (createStmnt.getTable()).getName();
		List<ColumnDefinition> 	tabColumns = createStmnt.getColumnDefinitions();
		/* Instantiating the TableSchema based on create and assign to hash*/
		TableSchema tabObj = new TableSchema();
		// setting the values
		tabObj.setTableName(tabName);
		tabObj.setTabColumns(tabColumns);
		if (Main.dataObjects.containsKey(tabName))
		{
			return false;
		}
		/* Assigning the tab oject value to the hash*/
		Main.dataObjects.put(tabName, tabObj);
		return true;
	}

	private boolean parseSelectStatement(PlainSelect select) {
		List<SelectItem> selectItems = select.getSelectItems();
		FromItem fromItem = select.getFromItem();
		Expression where = select.getWhere();

		// DEBUG INFO block
		System.out.println("Scan: " + fromItem);
		System.out.println("Selection: " + where);
		System.out.print("Projection: ");
		for (SelectItem selectItem : selectItems) {
			System.out.print(selectItem + ", ");
		}
		System.out.println();

		if (fromItem instanceof SubSelect) {
			System.out.println("NESTED SELECT");
			System.out.println("Nested relation's alias: " + fromItem.getAlias());

			SelectBody nestedSelectBody = ((SubSelect) fromItem).getSelectBody();
			if (nestedSelectBody instanceof PlainSelect ) {

				// Make recursive call for nested select parsing
				PlainSelect nestedSelect = (PlainSelect)nestedSelectBody;
				System.out.println("RECURSE");
				if(!parseSelectStatement(nestedSelect)) {
					return false;
				}
			} else if (nestedSelectBody instanceof Union) {
				return false;
			}
		}

		// Add a ScanOperator if fromItem aint a nested query else set alias for previous operator
		if (fromItem instanceof SubSelect) {
			// head.addRelationAlias(fromItem)
			System.out.println("* add Alias to last operator: " + fromItem.getAlias());
		} else {
			System.out.println("+ ScanOperator: " + fromItem);
		}
		// Add a SelectionOperator
		if (where != null) {
			System.out.println("+ SelectionOperator: " + where);
		}
		System.out.print("+ ProjectionOperator: ");
		for (SelectItem selectItem : selectItems) {
			System.out.print(selectItem + ", ");
		}
		System.out.println();

		return true;
	}

	public static void main(String[] main) {
		String[] queries = {
				"SELECT age, name, dob FROM MyData",
				"SELECT age, name, dob FROM MyData WHERE pin LIKE '%226'",
				"SELECT a+b as c, d FROM MyData",
				"SELECT * from MyData",
				"SELECT r.a, r.b as c, r.d+r.e as f FROM r",
				"SELECT Q.c, Q.b FROM (SELECT c,b from MyData WHERE a < 10) Q WHERE Q.c > Q.b"
		};
		for (String query : queries) {
			Reader input = new StringReader(query);
			CCJSqlParser jSQLParser = new CCJSqlParser(input);
			try {
				Statement statement = jSQLParser.Statement();
				System.out.println(statement);
				SimpleParser parser = new SimpleParser();
				parser.parse(statement);
				System.out.println("-----");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}