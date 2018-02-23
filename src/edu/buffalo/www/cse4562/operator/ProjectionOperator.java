package edu.buffalo.www.cse4562.operator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.www.cse4562.TableSchema;
import edu.buffalo.www.cse4562.evaluator.evalOperator;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOperator extends BaseOperator implements Iterator<Object[]> {

	Object[] record;
	Object[] prevRecord;
	private TableSchema prevSchema;
	boolean starFlag = false;
	List<SelectExpressionItem> selectExp;

	public ProjectionOperator(BaseOperator prevOperator, List<SelectItem> selectItems) {
		super(prevOperator, prevOperator.getTableSchema());
		// Gets the previous schema from the child operator.
		this.prevSchema = prevOperator.getTableSchema();
		// New schema for the projection and its parents.
		TableSchema newSchema = new TableSchema();
		/* Case of no change in schema and set current operator's schema to child schema*/
		if (selectItems.size() == 1 && selectItems.get(0).toString().equals("*"))
		{
			newSchema = prevSchema;
			super.setTableSchema(newSchema);
			int recSize = (newSchema.getTabColumns().size());
			record = new Object[recSize];
			starFlag = true;
		}
		else
		{
			/* Setting the record size*/
			int recSize = selectItems.size();
			record = new Object[recSize];
			List<ColumnDefinition> newColumnDefn = new ArrayList<ColumnDefinition>(recSize);
			/* Cleaning and assigning the select expression item*/
			this.selectExp = new ArrayList<SelectExpressionItem>(recSize);
			SelectExpressionItem selectExpItem;
			for(int i=0; i < recSize;i++)
			{
				selectExpItem = (SelectExpressionItem)selectItems.get(i);
				this.selectExp.add(selectExpItem);
			}

			boolean aliasFlag = false;
			String selectOp, colName, newColumn;
			ColumnDefinition tempColumn;
			/* Logic to create the new Schema */
			for(int i=0; i < recSize;i++)
			{
				aliasFlag = false;
				selectOp = selectExp.get(i).toString().toUpperCase();
				for(int j=0;j < prevSchema.getTabColumns().size();j++ )
				{
					tempColumn = prevSchema.getTabColumns().get(j);
					colName = tempColumn.toString().split(" ")[0].toUpperCase();
					if(selectOp.equals(colName))
					{
						aliasFlag = true;
						newColumnDefn.add(tempColumn);
					}
				}
				if(!aliasFlag) {
					newColumn = this.selectExp.get(i).getAlias();
					//tempColumn = prevSchema.getTabColumns().get(i);
					ColumnDefinition aliasColumn = new ColumnDefinition();
					aliasColumn.setColumnName(newColumn.toUpperCase());
					newColumnDefn.add(aliasColumn);
				}
			}
			/* Setting the new Schema and record size*/
			newSchema.setTabColumns(newColumnDefn);
			newSchema.setTableName(prevSchema.getTableName());
			newSchema.setTabAlias(prevSchema.getTabAlias());
			super.setTableSchema(newSchema);
		}

	}


	@Override
	public boolean hasNext() {
		if(childOperator.hasNext())
		{
			prevRecord = this.childOperator.next();
			/* Case of (*) in SELECT */
			if(starFlag)
			{
				record = prevRecord;
				return true;
			}
			/* Case of Exp : Use Eval to evaluate*/
			evalOperator evalQuery = new evalOperator(this.prevRecord, this.prevSchema);
			for(int i = 0; i < this.selectExp.size();i++)
			{
				try {
					record[i] = evalQuery.eval(this.selectExp.get(i).getExpression());
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			return true;
		}
		else
		{
			return false;
		}


	}

	@Override
	public Object[] next() {

		return this.record;
	}

	@Override
	public void setAlias(String tabAlias) {

		super.getTableSchema().setTabAlias(tabAlias);
	}

}
