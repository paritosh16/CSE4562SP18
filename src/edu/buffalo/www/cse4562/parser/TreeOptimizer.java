package edu.buffalo.www.cse4562.parser;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.www.cse4562.operator.BaseOperator;
import edu.buffalo.www.cse4562.operator.SelectionOperator;

public class TreeOptimizer {

	private  BaseOperator insertPtr;

	/*Function that returns true if the expression is on a single clause */
	private boolean checkClause(Expression whereItem)
	{

		boolean leftColumn = false;
		boolean rightColumn = false;
		Expression leftExp = ((BinaryExpression)whereItem).getLeftExpression();
		Expression rightExp = ((BinaryExpression)whereItem).getRightExpression();
		if (leftExp instanceof Column)
		{
			leftColumn = true;
		}
		if (rightExp instanceof Column)
		{
			rightColumn = true;
		}
		if (leftColumn && rightColumn)
		{
			return false;
		}
		else if (rightColumn) {
			return true;
		}
		else if (leftColumn) {
			return true;
		}
		return false;
	}

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
		/* Getting the first selection clause*/
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

	/* Function that checks if a given operator has a column or not*/
	private Boolean checkColumn(BaseOperator ptr,Column col)
	{
		String columnVal = col.toString().toUpperCase();

		if(col.toString().contains("."))
		{
			String tabName = columnVal.split("\\.")[0];
			String selectOp = columnVal.split("\\.")[1].toUpperCase();
			for(int i = 0 ; i < ptr.getTableSchema().getTabColumns().size();i++)
			{
				String tabColumn = ptr.getTableSchema().getTabColumns().get(i).toString().toUpperCase().split(" ")[0];
				String refTabName = ptr.getRefTableName().get(i).toString().toUpperCase();
				if(tabColumn.equals(selectOp) && refTabName.equals(tabName))
				{

					return true;
				}
			}
		}
		else
		{
			for(int i = 0 ; i < ptr.getTableSchema().getTabColumns().size();i++)
			{
				String tabColumn = ptr.getTableSchema().getTabColumns().get(i).toString().toUpperCase().split(" ")[0];

				if(tabColumn.equals(col.toString().toUpperCase()))
				{
					return true;
				}
			}
		}
		return false;
	}
	/* A recursive function that sets the value of a static variable
	 * via recursively searching in the tree*/
	private Boolean searchCondition(BaseOperator parent,Column col)
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

	/* Method to Optimize the Tree and push down selections*/
	public Boolean optimizeSelectionPushdown(BaseOperator rootTree)
	{
		BaseOperator root = rootTree;
		/* STEP 1 : Get the first selection operator in the tree*/
		BaseOperator selectPtr = getSelectionOp(root);
		if (selectPtr instanceof SelectionOperator)
		{
			/* STEP 2 : Get the list of where clauses
			 * and breaking the and clauses by list of selection clauses*/
			Expression whereClause = ((SelectionOperator) selectPtr).getWhere();
			List<Expression> whereItems = getWhereClause(whereClause);

			/* Step 3 : for each where clause find the parent
			 * till where it can be pushed down*/
			for(Expression whereItem:whereItems)
			{

				/* STEP 4 : that it is a condition on a single column*/
				if(checkClause(whereItem))
				{
					/* STEP 5 : Get the column name and tablename for the item for the where clause*/
					Expression leftExp = ((BinaryExpression)whereItem).getLeftExpression();
					Expression rightExp = ((BinaryExpression)whereItem).getRightExpression();
					Column col = null;
					if (leftExp instanceof Column)
					{
						col = (Column) leftExp;
					}
					else
					{
						col = (Column) rightExp;
					}

					/* STEP 6 : Search for the operator till where we have to
					 * Traverse in the tree */

					insertPtr = selectPtr;
					searchCondition(selectPtr,col);

					/* Step 7 : Insert a new selection clause if it is a value other
					 * than the current operator*/
					if (insertPtr != selectPtr)
					{

						/* Case to be inserted in left*/
						if (checkColumn(insertPtr.getChildOperator(),col))
						{
							BaseOperator newSelOperator = new SelectionOperator(insertPtr.getChildOperator(), whereItem);
							insertPtr.setChildOperator(newSelOperator);
						}
						/* Case to be inserted in right*/
						else
						{
							BaseOperator newSelOperator = new SelectionOperator(insertPtr.getSecondChildOperator(), whereItem);
							insertPtr.setSecondChildOperator(newSelOperator);
						}

					}

				}
			}

		}

		return true;
	}


}
