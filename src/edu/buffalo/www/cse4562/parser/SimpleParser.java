package edu.buffalo.www.cse4562.parser;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.buffalo.www.cse4562.TableSchema;
import edu.buffalo.www.cse4562.operator.BaseOperator;
import edu.buffalo.www.cse4562.operator.GroupByOperator;
import edu.buffalo.www.cse4562.operator.JoinOperator;
import edu.buffalo.www.cse4562.operator.LimitOperator;
import edu.buffalo.www.cse4562.operator.ProjectionOperator;
import edu.buffalo.www.cse4562.operator.ScanOperator;
import edu.buffalo.www.cse4562.operator.SelectionOperator;
import edu.buffalo.www.cse4562.operator.SortOperator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

/**
 * Gives a basic unoptimized Relational Algebra tree
 */
public class SimpleParser {

	private BaseOperator head;
	private HashMap<String, TableSchema> schemaRegister;
	private BaseOperator insertPtr;
	private List<Function> groupByFunctions = new ArrayList<Function>(5);

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
	 *
	 * @param statement
	 *            a single full SQL Statement
	 * @return boolean false if there was any error while parsing; true otherwise
	 */
	public boolean parse(Statement statement) {
		if (statement instanceof Select) {
			Select selectStmnt = (Select) statement;
			SelectBody selectBody = selectStmnt.getSelectBody();

			if (selectBody instanceof PlainSelect) {
				PlainSelect select = (PlainSelect) selectBody;
				return parseSelectStatement(select);
			} else if (selectBody instanceof Union) {
				return false;
			}

		} else if (statement instanceof CreateTable) {
			CreateTable createStmnt = (CreateTable) statement;
			/* False in case of tablename already exits */
			return parseCreateStatement(createStmnt);

		} else {
			return false;
		}
		return false;
	}

	private boolean parseCreateStatement(CreateTable createStmnt) {

		/* Getting Table Specific information from createStmnt */
		String tabName = (createStmnt.getTable()).getName();
		List<ColumnDefinition> tabColumns = createStmnt.getColumnDefinitions();
		/* Instantiating the TableSchema based on create and assign to hash */
		TableSchema tabObj = new TableSchema();
		// setting the values
		tabObj.setTableName(tabName);
		tabObj.setTabColumns(tabColumns);
		tabObj.setTabAlias(tabName);
		if (this.schemaRegister.containsKey(tabName)) {
			return false;
		}
		/* Assigning the tab object value to the hash */
		this.schemaRegister.put(tabName, tabObj);
		return true;
	}

	private boolean parseSelectStatement(PlainSelect select) {
		List<SelectItem> oldSelectItems = new ArrayList<SelectItem>(10);
		List<SelectItem> selectItems = select.getSelectItems();
		FromItem fromItem = select.getFromItem();
		Expression where = select.getWhere();
		List<OrderByElement> orderByList = select.getOrderByElements();
		List<Column> groupByList = select.getGroupByColumnReferences();
		Limit limit = select.getLimit();
		List<Join> joinItems = select.getJoins();

		// Save the old select items as a reference for the group by operator.
		oldSelectItems = selectItems;
		// Get the new select items for the projection operator to perform correctly.
		selectItems = prepSelectItems(selectItems);

		/*
		 * DEBUG INFO block
		 *
		 * System.out.println("Scan: " + fromItem); System.out.println("Selection: " +
		 * where); System.out.print("Projection: "); for (SelectItem selectItem :
		 * selectItems) { System.out.print(selectItem + ", "); } System.out.println();
		 *
		 */
		/* Parsing the tree in the bottom up fashion */
		/* Case when there is no join operator */
		if (joinItems == null) {
			this.head = parseFromStmnt(fromItem);
		} else {

			BaseOperator newJoinOperator = new JoinOperator(parseFromStmnt(fromItem), parseJoinStmnt(joinItems),
					joinItems.get(0).getOnExpression());
			this.head = newJoinOperator;
		}

		assert (head != null);

		/* Adding a SelectionOperator */
		if (where != null) {
			BaseOperator newOperator = new SelectionOperator(this.head, where);
			this.head = newOperator;
		}

		/* Adding a ProjectionOperator */
		BaseOperator newOperator = new ProjectionOperator(this.head, selectItems);
		this.head = newOperator;

		// Add a group by operator if a GROUP BY clause is present in the query.
		if (groupByList != null) {
			BaseOperator groupByOperator = new GroupByOperator(this.head, groupByList, groupByFunctions,
					oldSelectItems);
			this.head = groupByOperator;
		}

		// Add a sort operator if an ORDER BY clause is present in the query.
		if (orderByList != null) {
			BaseOperator sortOperator = new SortOperator(this.head, orderByList);
			this.head = sortOperator;
		}

		// Add a limit operator if a LIMIT clause is present in the query.
		if (limit != null) {
			BaseOperator limitOperator = new LimitOperator(this.head, limit);
			this.head = limitOperator;
		}

		return true;
	}

	/* Function that returns true if the expression is on a single clause */
	private boolean checkClause(Expression whereItem) {

		boolean leftColumn = false;
		boolean rightColumn = false;
		Expression leftExp = ((BinaryExpression) whereItem).getLeftExpression();
		Expression rightExp = ((BinaryExpression) whereItem).getRightExpression();
		if (leftExp instanceof Column) {
			leftColumn = true;
		}
		if (rightExp instanceof Column) {
			rightColumn = true;
		}
		if (leftColumn && rightColumn) {
			return false;
		} else if (rightColumn) {
			return true;
		} else if (leftColumn) {
			return true;
		}
		return false;
	}

	/*
	 * Function that recursively parses the where clause and breaks them into
	 * separate And clauses
	 */
	private List<Expression> getWhereClause(Expression whereItem) {
		List<Expression> whereExpList = new ArrayList<>(1);
		boolean rFlag = true;
		while (rFlag) {
			if (whereItem instanceof AndExpression) {
				whereExpList.add(((AndExpression) whereItem).getRightExpression());
				whereItem = ((BinaryExpression) whereItem).getLeftExpression();
			} else {
				whereExpList.add(whereItem);
				rFlag = false;
			}
		}
		return whereExpList;
	}

	/* Method that gets you the first selection operator */
	private BaseOperator getSelectionOp(BaseOperator ptr) {

		if (ptr == null) {
			return ptr;
		}
		/* Getting the first selection clause */
		BaseOperator prevRoot = null;
		boolean mFlag = true;
		while (ptr != null && mFlag) {
			if (ptr instanceof SelectionOperator) {
				mFlag = false;
				return ptr;
			} else {
				prevRoot = ptr;
				BaseOperator temp = ptr.getChildOperator();
				if (temp != null) {
					ptr = ptr.getChildOperator();
				} else {
					return ptr;
				}

			}
		}
		return ptr;
	}

	/* Function that checks if a given operator has a column or not */
	private Boolean checkColumn(BaseOperator ptr, Column col) {
		String columnVal = col.toString().toUpperCase();

		if (col.toString().contains(".")) {
			String tabName = columnVal.split("\\.")[0];
			String selectOp = columnVal.split("\\.")[1].toUpperCase();
			for (int i = 0; i < ptr.getTableSchema().getTabColumns().size(); i++) {
				String tabColumn = ptr.getTableSchema().getTabColumns().get(i).toString().toUpperCase().split(" ")[0];
				String refTabName = ptr.getRefTableName().get(i).toString().toUpperCase();
				if (tabColumn.equals(selectOp) && refTabName.equals(tabName)) {

					return true;
				}
			}
		} else {
			for (int i = 0; i < ptr.getTableSchema().getTabColumns().size(); i++) {
				String tabColumn = ptr.getTableSchema().getTabColumns().get(i).toString().toUpperCase().split(" ")[0];

				if (tabColumn.equals(col.toString().toUpperCase())) {
					return true;
				}
			}
		}
		return false;
	}

	/*
	 * A recursive function that sets the value of a static variable via recursively
	 * searching in the tree
	 */
	private Boolean searchCondition(BaseOperator parent, Column col) {

		Boolean lFlag = false;
		Boolean rFlag = false;
		BaseOperator leftChild = parent.getChildOperator();
		BaseOperator rightChild = parent.getSecondChildOperator();
		if (leftChild != null) {
			if (checkColumn(leftChild, col)) {
				this.insertPtr = parent;
				lFlag = true;
				searchCondition(leftChild, col);
			}
		}
		if (!lFlag) {
			if (rightChild != null) {
				if (checkColumn(rightChild, col)) {

					this.insertPtr = parent;

					rFlag = true;
					searchCondition(rightChild, col);
				}
			}
		}

		return true;
	}

	/* Method to Optimize the Tree and push down selections */
	public Boolean optimizeTree() {
		BaseOperator root = this.head;
		/* STEP 1 : Get the first selection operator in the tree */
		BaseOperator selectPtr = getSelectionOp(root);
		if (selectPtr instanceof SelectionOperator) {
			/*
			 * STEP 2 : Get the list of where clauses and breaking the and clauses by list
			 * of selection clauses
			 */
			Expression whereClause = ((SelectionOperator) selectPtr).getWhere();
			List<Expression> whereItems = getWhereClause(whereClause);

			/*
			 * Step 3 : for each where clause find the parent till where it can be pushed
			 * down
			 */
			for (Expression whereItem : whereItems) {

				/* STEP 4 : that it is a condition on a single column */
				if (checkClause(whereItem)) {
					/*
					 * STEP 5 : Get the column name and tablename for the item for the where clause
					 */
					Expression leftExp = ((BinaryExpression) whereItem).getLeftExpression();
					Expression rightExp = ((BinaryExpression) whereItem).getRightExpression();
					Column col = null;
					if (leftExp instanceof Column) {
						col = (Column) leftExp;
					} else {
						col = (Column) rightExp;
					}

					/*
					 * STEP 6 : Search for the operator till where we have to Traverse in the tree
					 */

					insertPtr = selectPtr;
					searchCondition(selectPtr, col);

					/*
					 * Step 7 : Insert a new selection clause if it is a value other than the
					 * current operator
					 */
					if (insertPtr != selectPtr) {

						/* Case to be inserted in left */
						if (checkColumn(insertPtr.getChildOperator(), col)) {
							BaseOperator newSelOperator = new SelectionOperator(insertPtr.getChildOperator(),
									whereItem);
							insertPtr.setChildOperator(newSelOperator);
						}
						/* Case to be inserted in right */
						else {
							BaseOperator newSelOperator = new SelectionOperator(insertPtr.getSecondChildOperator(),
									whereItem);
							insertPtr.setSecondChildOperator(newSelOperator);
						}

					}

				}
			}

		}

		return true;
	}

	/* Method to parse the Join Operator */
	private BaseOperator parseJoinStmnt(List<Join> joinItems) {
		if (joinItems.size() > 1) {
			Join joinItem = joinItems.get(0);
			joinItems.remove(0);
			FromItem fromItem = joinItem.getRightItem();

			BaseOperator newJoinOperator = new JoinOperator(parseFromStmnt(fromItem), parseJoinStmnt(joinItems),
					joinItems.get(0).getOnExpression());

			return newJoinOperator;
		} else {
			Join joinItem = joinItems.get(0);

			FromItem fromItem = joinItem.getRightItem();

			return parseFromStmnt(fromItem);

		}

	}

	/* Method to parse the from Item Statement */
	private BaseOperator parseFromStmnt(FromItem fromItem) {

		/* Case of Recursive Calls */
		if (fromItem instanceof SubSelect) {
			SelectBody nestedSelectBody = ((SubSelect) fromItem).getSelectBody();
			PlainSelect nestedSelect = (PlainSelect) nestedSelectBody;
			if (nestedSelectBody instanceof PlainSelect) {
				parseSelectStatement(nestedSelect);
				/* logic to set the Alias of the Table */
				if (fromItem.getAlias() != null) {
					this.head.setAlias(fromItem.getAlias().toString());

				}
				return this.head;
			} else {
				return null;
			}
		}
		/* Case we are directly supplied with table name */
		else {
			BaseOperator newOperator;
			try {
				Table tabCol = (Table) fromItem;
				String tabName = tabCol.getName().toString();
				TableSchema schema = this.schemaRegister.get(tabName);
				// ScanOperator is the bottom-most, doesn't have a childOperator
				BaseOperator childOperator = null;

				newOperator = new ScanOperator(childOperator, tabName, schema);
				/* logic to set the Alias of the Table */
				if (fromItem.getAlias() != null) {
					newOperator.setAlias(fromItem.getAlias().toString());

				}
				return newOperator;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
	}

	/* Function to prepare the projection and group by selectItems. */
	private List<SelectItem> prepSelectItems(List<SelectItem> selectItems) {
		// New select items that should be passed to projection so as to work with the
		// design.
		List<SelectItem> newSelectItems = new ArrayList<SelectItem>(10);
		// Parse all the select items to separate out the aggregate functions and
		// non-aggregate columns.
		for (int i = 0; i < selectItems.size(); i++) {
			// If has * as the projection.
			if (!(selectItems.get(i) instanceof AllColumns)) {
				SelectExpressionItem selectItem = (SelectExpressionItem) selectItems.get(i);
				// Get the expression.
				Expression selectExpression = selectItem.getExpression();
				// Check if an aggregate function.
				if (selectExpression instanceof Function) {
					// Create a function object.
					Function function = (Function) selectExpression;
					// Add to the list of functions. This list will be used by the group by operator
					// for reference.
					groupByFunctions.add(function);
					if (function.isAllColumns()) {
						// Whatever the aggregation is, includes all the columns in the schema.
						AllColumns allColumns = new AllColumns();
						newSelectItems.add(allColumns);
					} else {
						// Whatever the aggregation is, includes one specific column from the schema.
						ExpressionList expressionList = function.getParameters();
						List<Expression> expressions = expressionList.getExpressions();
						for (int j = 0; j < expressions.size(); j++) {
							// Get the expression which is a parameter to the aggregate function.
							SelectExpressionItem projectionExpression = new SelectExpressionItem();
							// Set the projection as the expression so that it is evaluated by EvalOperator
							// on the projection level.
							projectionExpression.setExpression(expressions.get(j));
							// Add to the new schema list.
							newSelectItems.add(projectionExpression);
						}
					}
				} else {
					// The select item is not a function, surely should be a column that is to be
					// projected. Add to the projection list straight away.
					newSelectItems.add(selectItems.get(i));
				}
			} else {
				// The select item is all column meaning only '*'. Cannot be an aggragation, add
				// to the projection list straight away.
				newSelectItems.add(selectItems.get(i));
			}
		}
		// Return the new list of the select items.
		return newSelectItems;
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
				// "SELECT * FROM MyData",
				// "SELECT * FROM MyData JOIN MyData ON MyData.name = MyData.name",
				"Select * FROM (SELECT NAME,AGE FROM MyData B) A Join MyData ON A.name = MyData.name where age > 10 "
				// "SELECT NAME FROM MyData,Tab2,Tab3 "
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
				// parser.optimizeTree();

				// System.out.println("head: " + headOperator.getClass().toString());
				while (headOperator.hasNext()) {
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
