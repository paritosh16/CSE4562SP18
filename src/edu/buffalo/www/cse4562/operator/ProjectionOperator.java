package edu.buffalo.www.cse4562.operator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.www.cse4562.TableSchema;
import edu.buffalo.www.cse4562.evaluator.evalOperator;

public class ProjectionOperator extends BaseOperator implements Iterator<Object[]> {

	Object[] record;
	Object[] prevRecord;
	private TableSchema prevSchema;
	boolean starFlag = false;
	List<SelectExpressionItem> selectExp;

	public ProjectionOperator(BaseOperator prevOperator, List<SelectItem> selectItems) {
		super(prevOperator, prevOperator.getTableSchema());
		this.prevSchema = prevOperator.getTableSchema();
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

			/* Logic to create the new Schema */
			for(int i=0; i < recSize;i++)
			{
				String selectOp = selectExp.get(i).toString();

				for(int j=0;j < prevSchema.getTabColumns().size();j++ )
				{
					ColumnDefinition tempColumn = prevSchema.getTabColumns().get(j);
					String colName = tempColumn.toString().split(" ")[0];
					if(selectOp.equals(colName))
					{

						newColumnDefn.add(tempColumn);
					}
				}
			}
			/* Setting the new Schema and record size*/
			newSchema.setTabColumns(newColumnDefn);
			newSchema.setTableName(prevSchema.getTableName());
			newSchema.setTabAlias(prevSchema.getTabAlias());


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
