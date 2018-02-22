package edu.buffalo.www.cse4562.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.www.cse4562.TableSchema;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOperator extends BaseOperator implements Iterator<Object[]> {

	private List<SelectItem> selectItems;
	private Integer[] mappingArr;
	Object[] record;
	Object[] prevRecord;
	TableSchema testSchema;

	public ProjectionOperator(BaseOperator prevOperator, List<SelectItem> selectItems) {
		super(prevOperator, prevOperator.getTableSchema());
		this.selectItems = selectItems;
		TableSchema newSchema = new TableSchema();
		TableSchema prevSchema = prevOperator.getTableSchema();
		this.testSchema = prevOperator.getTableSchema();
		/* Case of no change in schema and set current operator's schema to child schema*/
		if (this.selectItems.size() == 1 && this.selectItems.get(0).toString().equals("*"))
		{
			newSchema = prevSchema;
			super.setTableSchema(newSchema);
			int recSize = (newSchema.getTabColumns().size());
			mappingArr = new Integer[recSize];
			for (int i=0;i<recSize;i++)
			{
				mappingArr[i] = i;
			}
			record = new Object[recSize];
		}
		/* Case of schema change
		 * Take the intersection of the list and assign in same order of the child operator schema*/
		else
		{
			int recSize = selectItems.size();
			mappingArr = new Integer[recSize];
			/* Converting the list of Select-items to a list for each of search*/
			List<String> selectItemStr = new ArrayList<String>(recSize);
			List<ColumnDefinition> newColumnDefn = new ArrayList<ColumnDefinition>(recSize);
			for(SelectItem item : selectItems)
			{
				String [] tempVar = item.toString().split("\\.");
				if (tempVar.length == 1)
				{
					selectItemStr.add(tempVar[0]);
				}
				else
				{
					selectItemStr.add(tempVar[1]);
				}
			}


			/* Logic to create the new Schema and mapping of the records*/
			for(int i=0; i < selectItemStr.size();i++)
			{
				String selectOp = selectItemStr.get(i);

				for(int j=0;j < prevSchema.getTabColumns().size();j++ )
				{
					ColumnDefinition tempColumn = prevSchema.getTabColumns().get(j);
					String colName = tempColumn.toString().split(" ")[0];
					if(selectOp.equals(colName))
					{
						mappingArr[i] = j;
						newColumnDefn.add(tempColumn);
					}
				}
			}
			/* Setting the new Schema and record size*/
			newSchema.setTabColumns(newColumnDefn);
			newSchema.setTableName(prevSchema.getTableName());
			newSchema.setTabAlias(prevSchema.getTabAlias());
			//newSchema.setTabAlias("A");
			super.setTableSchema(newSchema);
			record = new Object[recSize];

		}

	}

	@Override
	public boolean hasNext() {
		if(childOperator.hasNext())
		{

			prevRecord = this.childOperator.next();
			//----------TYPECAST FOR THE EXPRESSION TO BE PASSED TO EVAL-------------
			// Type cast to select expression item.
			//			SelectExpressionItem selectExpression = (SelectExpressionItem) this.selectItems.get(0);
			//			// Execute with eval.
			//			evalOperator testEval = new evalOperator(prevRecord, this.testSchema);
			//			try {
			//				System.out.println(testEval.eval(selectExpression.getExpression()));
			//			} catch (SQLException e) {
			//				// TODO Auto-generated catch block
			//				e.printStackTrace();
			//			}
			//------------------------------------------------------------------------
			for(int i = 0; i < mappingArr.length;i++)
			{
				record[i] = prevRecord[mappingArr[i]];
			}
			return true;
		}

		return false;
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
