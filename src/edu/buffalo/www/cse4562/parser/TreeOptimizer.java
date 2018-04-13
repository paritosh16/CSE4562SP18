package edu.buffalo.www.cse4562.parser;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.www.cse4562.operator.BaseOperator;
import edu.buffalo.www.cse4562.operator.JoinOperator;
import edu.buffalo.www.cse4562.operator.SelectionOperator;

public class TreeOptimizer {

	private  BaseOperator insertPtr;

	/* Function that recursively parses the where clause and breaks them
	 * into separate And clauses*/
	private List<Expression> getWhereClause(Expression whereItem)
	{
		List<Expression> whereExpList = new ArrayList<>(1);
		boolean rFlag = true;
		while(rFlag)
		{
			if (whereItem instanceof AndExpression)
			{
				whereExpList.add(((AndExpression) whereItem).getRightExpression());
				whereItem = ( (BinaryExpression) whereItem).getLeftExpression();
			}
			else
			{
				whereExpList.add(whereItem);
				rFlag = false;
			}
		}
		return whereExpList;
	}

	/* Method that gets you the first selection operator*/
	private BaseOperator getSelectionOp(BaseOperator ptr)
	{
		if (ptr == null)
		{
			return ptr;
		}
		/* Getting the  first selection clause*/
		BaseOperator prevRoot = null ;
		boolean mFlag = true;
		while(ptr != null && mFlag)
		{
			if (ptr instanceof SelectionOperator) {
				mFlag = false;
				return ptr;
			}
			else
			{
				prevRoot = ptr;
				BaseOperator temp = ptr.getChildOperator();
				if (temp != null)
				{
					ptr = ptr.getChildOperator();
				}
				else
				{
					return ptr;
				}
			}
		}
		return ptr;
	}

	/* Method that gets the parent of the first selection operator from root
	 * This would return Null if there are no more selections to be pushdown*/
	private BaseOperator getParentSelectionOp(BaseOperator ptr)
	{
		BaseOperator parent = ptr;
		BaseOperator child = null;
		BaseOperator secondChild = null;
		boolean mFlag = true;
		child = ptr.getChildOperator();
		if(parent instanceof JoinOperator)
		{
			child = ptr.getSecondChildOperator();
		}
		secondChild = ptr.getSecondChildOperator();
		while( child != null && mFlag)
		{
			if(child instanceof SelectionOperator)
			{
				mFlag = false;
				return parent;
			}
			else{
				parent = child;
				if(child instanceof JoinOperator)
				{
					child = child.getSecondChildOperator();
				}
				else
				{
					child = child.getChildOperator();
				}

			}
		}
		return null;
	}



	/* Function that checks if a given operator has a column or not*/
	private Boolean checkColumn(BaseOperator ptr,List<Column> columns)
	{
		if(columns.isEmpty())
		{
			return false;
		}
		List<String> refTableName = ptr.getRefTableName();
		List<ColumnDefinition> columnList = (ptr.getTableSchema().getTabColumns());
		for(Column selColumn : columns)
		{
			String selColumnName = selColumn.getColumnName().toUpperCase();
			boolean mFlag = false;
			String tabName = selColumn.getTable().getName();
			if(tabName == null)
			{
				for (int i = 0; i < columnList.size(); i++) {
					String schemaColName = columnList.get(i).getColumnName().toUpperCase();
					if (schemaColName.equals(selColumnName)) {
						mFlag = true;
					}
				}
			}
			else
			{
				/* Case of tablename.columnname*/
				for (int i = 0; i < columnList.size(); i++)
				{
					String schemaColName = columnList.get(i).getColumnName().toUpperCase();
					if (schemaColName.equals(selColumnName) && refTableName.get(i).equals(tabName)) {
						mFlag = true;
					}
				}
				/*Case when no match is found after looking at the schema */
				if(!mFlag)
				{
					return false;
				}
			}
		}

		return true;
	}

	/*
	 *  Function that gets all the column names from the whereItem */
	private List<Column> getColumnsSelection(Expression whereItem)
	{
		List<Column> columns = new ArrayList<>(2);
		Expression leftExp = ((BinaryExpression)whereItem).getLeftExpression();
		Expression rightExp = ((BinaryExpression)whereItem).getRightExpression();
		if (leftExp instanceof Column)
		{
			columns.add((Column) leftExp);
		}
		else if(leftExp instanceof BinaryExpression)
		{
			List<Column> leftcolumns = new ArrayList<>(2);
			leftcolumns = getColumnsSelection(leftExp);
			columns.addAll(leftcolumns);

		}
		if (rightExp instanceof Column)
		{
			columns.add((Column) rightExp);
		}
		else if(rightExp instanceof BinaryExpression)
		{
			List<Column> rightcolumns = new ArrayList<>(2);
			rightcolumns = getColumnsSelection(rightExp);
			columns.addAll(rightcolumns);

		}

		return columns;
	}

	/* A recursive function that sets the value of a static variable
	 * via recursively searching in the tree
	 * The value that is set is the parent of the operator below which selection is
	 * to be implemented*/
	private Boolean searchCondition(BaseOperator parent,List<Column> col)
	{
		Boolean lFlag = false;
		Boolean rFlag = false;
		BaseOperator leftChild = parent.getChildOperator();
		BaseOperator rightChild = parent.getSecondChildOperator();
		if(leftChild != null)
		{
			if(checkColumn(leftChild, col))
			{
				this.insertPtr = parent;
				lFlag = true;
				searchCondition(leftChild, col);
			}
		}
		if(!lFlag)
		{
			if(rightChild!= null)
			{
				if(checkColumn(rightChild, col))
				{

					this.insertPtr = parent;

					rFlag = true;
					searchCondition(rightChild, col);
				}
			}
		}
		return true;
	}

	/* Method that recursively splits the tree*/
	public Boolean splitTreeSelections(BaseOperator rootTree)
	{
		if(rootTree == null)
		{
			return true;
		}
		BaseOperator root = rootTree;
		/* STEP 1 : Get the first selection operator in the tree*/
		BaseOperator selectPtr = getSelectionOp(root);
		if (selectPtr instanceof SelectionOperator)
		{
			/* STEP 2 : Get the list of where clauses
			 * and breaking the and clauses by list of selection clauses*/
			Expression whereClause = ((SelectionOperator) selectPtr).getWhere();
			List<Expression> whereItems = getWhereClause(whereClause);
			/* STEP 3 : Split the expression and sequentially insert them one after another */
			BaseOperator ptrTrav = selectPtr;
			Expression firstWhere = whereItems.get(0);
			((SelectionOperator) ptrTrav).setWhere(firstWhere);
			for(Expression whereItem : whereItems )
			{
				if(whereItem == firstWhere)continue;
				BaseOperator child = ptrTrav.getChildOperator();
				BaseOperator newSelOperator = new SelectionOperator(child, whereItem);
				ptrTrav.setChildOperator(newSelOperator);
				ptrTrav = newSelOperator;

			}
			return splitTreeSelections(ptrTrav.getChildOperator());
		}
		return true;
	}

	/* Method to Optimize the Tree and push down selections*/
	public Boolean optimizeSelectionPushdown(BaseOperator rootTree)
	{
		boolean rFlag = false;
		if(rootTree == null)
		{
			return true;
		}
		/* Step 1 : Get the parent of the first selection Operator*/
		BaseOperator parentSelection = getParentSelectionOp(rootTree);
		if(parentSelection == null)
		{
			return true;
		}

		BaseOperator selectOpr = parentSelection.getChildOperator();
		if(parentSelection instanceof JoinOperator)
		{
			selectOpr = parentSelection.getSecondChildOperator();
			rFlag = true;
		}
		if(selectOpr.isOptimzed())
		{
			return true;
		}
		if(selectOpr == null)
		{
			return true;
		}
		BaseOperator childSelection = selectOpr.getChildOperator();
		Expression whereItem = ((SelectionOperator) selectOpr).getWhere();
		/* STEP 2 : Get the list of columns it has*/
		List<Column> columns = getColumnsSelection(whereItem);
		/*STEP 3 : Get the parent until where we can insert the selection operator  */
		insertPtr = selectOpr;
		searchCondition(selectOpr, columns);

		/* Step 4 : Insert a new selection clause if it is a value other
		 * than the current operator*/
		boolean insertSelect = false;
		if (insertPtr != selectOpr)
		{
			insertSelect = true;
			/* Case to be inserted in left*/
			if (checkColumn(insertPtr.getChildOperator(),columns))
			{
				BaseOperator newSelOperator = new SelectionOperator(insertPtr.getChildOperator(), whereItem);
				newSelOperator.setOptimzed(true);
				insertPtr.setChildOperator(newSelOperator);
			}
			/* Case to be inserted in right*/
			else
			{
				BaseOperator newSelOperator = new SelectionOperator(insertPtr.getSecondChildOperator(), whereItem);
				newSelOperator.setOptimzed(true);
				insertPtr.setSecondChildOperator(newSelOperator);
			}

		}
		else
		{
			selectOpr.setOptimzed(true);
		}

		/* STEP 5 : Remove the selection operator at top if inserted at bottom*/
		if(insertSelect)
		{
			parentSelection.setChildOperator(childSelection);
			return optimizeSelectionPushdown(rootTree);
		}


		return optimizeSelectionPushdown(parentSelection.getChildOperator());
	}

	/* Method that converts all the selections sitting on top of the Crossproduct and convert it to
	 * Hash join*/
	public boolean optimizeJoin(BaseOperator rootTree)
	{
		if(rootTree == null)
		{
			return true;
		}

		/* STEP 1 : get the parent of the first selection operator*/
		BaseOperator parentSelection = getParentSelectionOp(rootTree);
		boolean rFlag = false;
		if(parentSelection == null)
		{
			return true;
		}

		BaseOperator selectOpr = parentSelection.getChildOperator();
		if(parentSelection instanceof JoinOperator)
		{
			selectOpr = parentSelection.getSecondChildOperator();
			rFlag = true;
		}
		if(selectOpr == null)
		{
			return true;
		}
		BaseOperator childSelection = selectOpr.getChildOperator();
		Expression whereItem = ((SelectionOperator) selectOpr).getWhere();
		if (childSelection instanceof JoinOperator)
		{
			// TODO : check it is an equality operator
			if(((JoinOperator) childSelection).isHashJoin())
			{
				return optimizeJoin(childSelection) ;
			}
			else
			{
				((JoinOperator) childSelection).setJoinClause(whereItem);
				((JoinOperator) childSelection).setHashJoin(true);
				/* Removing the selection item*/
				if(!rFlag)
				{
					parentSelection.setChildOperator(childSelection);
				}
				else
				{
					parentSelection.setSecondChildOperator(childSelection);
				}

				return optimizeJoin(rootTree) ;
			}

		}
		else
		{
			return optimizeJoin(selectOpr);
		}


	}

}
