package edu.buffalo.www.cse4562.parser;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.www.cse4562.TableSchema;
import edu.buffalo.www.cse4562.operator.BaseOperator;
import edu.buffalo.www.cse4562.operator.JoinOperator;
import edu.buffalo.www.cse4562.operator.LimitOperator;
import edu.buffalo.www.cse4562.operator.ProjectionOperator;
import edu.buffalo.www.cse4562.operator.ScanOperator;
import edu.buffalo.www.cse4562.operator.SelectionOperator;
import edu.buffalo.www.cse4562.operator.SortOperator;

/**
 * Gives a basic unoptimized Relational Algebra tree
 */
public class SimpleParser {

	private BaseOperator head;
	private HashMap<String, TableSchema> schemaRegister;


	/**
	 * @param schemaRegister
	 */
	public SimpleParser(HashMap<String, TableSchema> schemaRegister) {
		super();
		this.head = null;
		this.schemaRegister = schemaRegister;
	}

	public BaseOperator getOperatorRoot() {
		// TODO Auto-generated method stub
		return head;
	}

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
		tabObj.setTabAlias(tabName);
		if (this.schemaRegister.containsKey(tabName))
		{
			return false;
		}
		/* Assigning the tab object value to the hash*/
		this.schemaRegister.put(tabName, tabObj);
		return true;
	}

	private boolean parseSelectStatement(PlainSelect select) {
		List<SelectItem> selectItems = select.getSelectItems();
		FromItem fromItem = select.getFromItem();
		Expression where = select.getWhere();
		List<OrderByElement> orderByList = select.getOrderByElements();
		Limit limit = select.getLimit();
		List<Join> joinItems = select.getJoins();


		/* DEBUG INFO block

		System.out.println("Scan: " + fromItem);
		System.out.println("Selection: " + where);
		System.out.print("Projection: ");
		for (SelectItem selectItem : selectItems) {
			System.out.print(selectItem + ", ");
		}
		System.out.println();

		 */
		/* Parsing the tree in the bottom up fashion*/
		/*Case when there is no join operator */
		if(joinItems == null)
		{
			this.head = parseFromStmnt(fromItem);
		}
		else
		{

			BaseOperator newJoinOperator = new JoinOperator(parseFromStmnt(fromItem), parseJoinStmnt(joinItems), joinItems.get(0).getOnExpression());
			this.head = newJoinOperator;
		}




		assert(head != null);

		/* Adding a SelectionOperator */
		if (where != null) {
			BaseOperator newOperator = new SelectionOperator(this.head, where);
			this.head = newOperator;
		}


		/* Adding a ProjectionOperator */
		BaseOperator newOperator = new ProjectionOperator(this.head, selectItems);
		this.head = newOperator;

		// Add a sort operator if an ORDER BY clause is present in the query.
		if(orderByList != null) {
			BaseOperator sortOperator = new SortOperator(this.head, orderByList);
			this.head = sortOperator;
		}

		// Add a limit operator if a LIMIT clause is present in the query.
		if(limit != null) {
			BaseOperator limitOperator = new LimitOperator(this.head, limit);
			this.head = limitOperator;
		}



		return true;
	}


	/* Method to parse the Join Operator*/
	private BaseOperator parseJoinStmnt(List<Join> joinItems)
	{
		if (joinItems.size() > 1 )
		{
			Join joinItem = joinItems.get(0);
			joinItems.remove(0);
			FromItem fromItem = joinItem.getRightItem();

			BaseOperator newJoinOperator = new JoinOperator(parseFromStmnt(fromItem), parseJoinStmnt(joinItems), joinItems.get(0).getOnExpression());

			return newJoinOperator;
		}
		else
		{
			Join joinItem = joinItems.get(0);

			FromItem fromItem = joinItem.getRightItem();

			return parseFromStmnt(fromItem);

		}

	}

	/*Method to parse the from Item Statement */
	private BaseOperator parseFromStmnt(FromItem fromItem)
	{

		/* Case of Recursive Calls */
		if (fromItem  instanceof SubSelect)
		{
			SelectBody nestedSelectBody = ((SubSelect) fromItem).getSelectBody();
			PlainSelect nestedSelect = (PlainSelect)nestedSelectBody;
			if (nestedSelectBody instanceof PlainSelect ) {
				parseSelectStatement(nestedSelect);
				/* logic to set the Alias of the Table*/
				if (fromItem.getAlias() != null)
				{
					this.head.setAlias(fromItem.getAlias().toString());

				}
				return this.head;
			}
			else
			{
				return null;
			}
		}
		/* Case we are directly supplied with table name*/
		else
		{
			BaseOperator newOperator;
			try {
				Table tabCol = (Table) fromItem;
				String tabName = tabCol.getName().toString();
				TableSchema schema = this.schemaRegister.get(tabName);
				// ScanOperator is the bottom-most, doesn't have a childOperator
				BaseOperator childOperator = null;

				newOperator = new ScanOperator(childOperator, tabName, schema);
				/* logic to set the Alias of the Table*/
				if (fromItem.getAlias() != null)
				{
					newOperator.setAlias(fromItem.getAlias().toString());

				}
				return newOperator;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
	}

	public static void main(String[] main) {
		HashMap<String, TableSchema> schemaRegister = new HashMap<String, TableSchema>();
		Reader input = new StringReader("CREATE TABLE MyData (age int, name varchar, date date);");
		CCJSqlParser jSQLParser = new CCJSqlParser(input);
		try {
			Statement statement = jSQLParser.Statement();
			System.out.println(statement);
			SimpleParser parser = new SimpleParser(schemaRegister);
			parser.parse(statement);
			System.out.println("-----1");


		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String[] queries = {
				//				"SELECT * FROM MyData",
				//				"SELECT * FROM MyData JOIN MyData ON MyData.name = MyData.name",
				"Select * FROM (SELECT NAME,AGE FROM MyData B) A Join MyData ON A.name = MyData.name where age > 10 "
				//"SELECT NAME FROM MyData,Tab2,Tab3 "
		};
		for (String query : queries) {
			input = new StringReader(query);
			jSQLParser = new CCJSqlParser(input);
			try {
				Statement statement = jSQLParser.Statement();
				System.out.println(statement);
				SimpleParser parser = new SimpleParser(schemaRegister);
				parser.parse(statement);
				System.out.println("-----2");
				BaseOperator headOperator = parser.getOperatorRoot();
				System.out.println("Optimizing it");
				//parser.optimizeTree();

				//System.out.println("head: " + headOperator.getClass().toString());
				while(headOperator.hasNext()) {
					Object[] row = headOperator.next();
					for (Object item : row) {
						System.out.print(item + ", ");
					}
					System.out.println();
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
