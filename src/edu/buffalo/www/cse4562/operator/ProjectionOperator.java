package edu.buffalo.www.cse4562.operator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.www.cse4562.TableSchema;
import edu.buffalo.www.cse4562.evaluator.evalOperator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOperator extends BaseOperator implements Iterator<Object[]> {

	Object[] record;
	Object[] prevRecord;
	private TableSchema prevSchema;
	boolean starFlag = false;
	List<SelectExpressionItem> selectExp;
	//private List<Integer> indicesToProject = new ArrayList<Integer>(10);

	public ProjectionOperator(BaseOperator prevOperator, List<SelectItem> selectItems) {
		super(prevOperator, prevOperator.getTableSchema());
		// Gets the previous schema from the child operator.
		this.prevSchema = prevOperator.getTableSchema();
		List<String> newRefTableName = new ArrayList<String>(10);
		// New schema for the projection and its parents.
		TableSchema newSchema = new TableSchema();
		/*
		 * Case of no change in schema and set current operator's schema to child schema
		 */
		if (selectItems.size() == 1 && selectItems.get(0).toString().equals("*")) {
			newSchema = prevSchema;
			newRefTableName = this.childOperator.getRefTableName();
			super.setTableSchema(newSchema);
			int recSize = (newSchema.getTabColumns().size());
			record = new Object[recSize];
			starFlag = true;
		} else {
			SelectExpressionItem selectExpItem;
			ColumnDefinition tempColumn;
			String colName, tabName;
			List<ColumnDefinition> newColumnDefn = new ArrayList<ColumnDefinition>(10);
			/* Cleaning and assigning the select expression item */
			this.selectExp = new ArrayList<SelectExpressionItem>(10);
			for (int i = 0; i < selectItems.size(); i++) {
				if (selectItems.get(i) instanceof net.sf.jsqlparser.statement.select.AllTableColumns) {
					// Whatever the item of the projection list is, it is all the columns for the
					// table.
					AllTableColumns projectAll = (AllTableColumns) selectItems.get(i);
					tabName = projectAll.getTable().getName().toString();
					for (int j = 0; j < this.prevSchema.getTabColumns().size(); j++) {
						if (this.childOperator.getRefTableName().get(j).equals(tabName)) {
							newColumnDefn.add(this.prevSchema.getTabColumns().get(j));
							newRefTableName.add(tabName);
							//indicesToProject.add(j);
						}
					}
				} else {
					// The select item here is a specific column of a table. Add the column to the
					// schema and the table to the ref column.
					this.selectExp.add((SelectExpressionItem) selectItems.get(i));
					selectExpItem = (SelectExpressionItem) selectItems.get(i);
					String aliasName = selectExpItem.getAlias();
					String selectOp = selectExpItem.toString();
					if (aliasName != null) {
						// The new column that is to be returned should be named the alias column.
						ColumnDefinition aliasColumn = new ColumnDefinition();
						aliasColumn.setColumnName(aliasName.toUpperCase());
						newColumnDefn.add(aliasColumn);
						// The column to be captured for the alias column.
						for (int j = 0; j < prevSchema.getTabColumns().size(); j++) {
							tempColumn = prevSchema.getTabColumns().get(j);
							colName = tempColumn.getColumnName();
							selectOp = selectOp.split(" ")[0];
							Expression selectExpression = selectExpItem.getExpression();
							Boolean flagForExpressionProject = false;
							if (selectExpression instanceof BinaryExpression) {
								BinaryExpression binExp = (BinaryExpression) selectExpression;
								if (binExp.getLeftExpression() instanceof Column) {
									// Left operator is the column that should be in the schema for the reference.
									selectOp = ((Column) binExp.getLeftExpression()).getColumnName();
									flagForExpressionProject = true;
								} else if (binExp.getRightExpression() instanceof Column) {
									// Right operator is the column that should be in the schema for the reference.
									selectOp = ((Column) binExp.getRightExpression()).getColumnName();
									flagForExpressionProject = true;
								}
							}
							if (selectOp.contains(".")) {
								// TableName.columnName
								selectOp = selectOp.split("\\.")[1];
							}
							if (selectOp.toUpperCase().equals(colName.toUpperCase())) {
								newRefTableName.add(this.childOperator.getRefTableName().get(j));
								//indicesToProject.add(j);
							}
						}
					} else {
						if (selectOp.contains(".")) {
							// The projected item contains a ., meaning it needs to be separated based on
							// the TableName.ColumnName
							// FIXME Needs to be generic. Handles very very limited cases right now.
							Boolean columnFound = false;
							tabName = selectOp.split("\\.")[0];
							String expressionDesc = selectOp;
							selectOp = selectOp.split("\\.")[1];
							// The new column is not an alias.
							for (int j = 0; j < prevSchema.getTabColumns().size(); j++) {
								tempColumn = prevSchema.getTabColumns().get(j);
								colName = tempColumn.getColumnName();
								Expression selectExpression = selectExpItem.getExpression();
								Boolean flagForExpressionProject = false;
								if (selectExpression instanceof BinaryExpression) {
									BinaryExpression binExp = (BinaryExpression) selectExpression;
									if (binExp.getLeftExpression() instanceof Column) {
										// Left operator is the column that should be in the schema for the reference.
										selectOp = ((Column) binExp.getLeftExpression()).getColumnName();
										flagForExpressionProject = true;
									} else if (binExp.getRightExpression() instanceof Column) {
										// Right operator is the column that should be in the schema for the reference.
										selectOp = ((Column) binExp.getRightExpression()).getColumnName();
										flagForExpressionProject = true;
									}
								}
								if (selectOp.toUpperCase().equals(colName.toUpperCase())
										&& this.childOperator.getRefTableName().get(j).equals(tabName)) {
									if (flagForExpressionProject) {
										ColumnDefinition newtempColumn = new ColumnDefinition();
										newtempColumn.setColDataType(tempColumn.getColDataType());
										newtempColumn.setColumnName(
												selectExpression.toString().toUpperCase());
										tempColumn = newtempColumn;
									}
									ColumnDefinition newtempColumn = new ColumnDefinition();
									newtempColumn.setColDataType(tempColumn.getColDataType());
									newtempColumn.setColumnName(expressionDesc);
									newColumnDefn.add(newtempColumn);
									newRefTableName.add(tabName);
									//indicesToProject.add(j);
									break;
								}
							}
							//							// Did not find the column in the schema and it isn't an alias either.
							//							// Surely, it looks to be like an expression. Need to store in the schema as is.
							//							if(!columnFound) {
							//								ColumnDefinition newtempColumn = new ColumnDefinition();
							//								newtempColumn.setColumnName(expressionDesc);
							//								newColumnDefn.add(newtempColumn);
							//								newRefTableName.add(tabName);
							//							}
						} else {
							for (int j = 0; j < prevSchema.getTabColumns().size(); j++) {
								tempColumn = prevSchema.getTabColumns().get(j);
								colName = tempColumn.getColumnName();
								Expression selectExpression = selectExpItem.getExpression();
								Boolean flagForExpressionProject = false;
								if (selectExpression instanceof BinaryExpression) {
									BinaryExpression binExp = (BinaryExpression) selectExpression;
									if (binExp.getLeftExpression() instanceof Column) {
										// Left operator is the column that should be in the schema for the reference.
										selectOp = ((Column) binExp.getLeftExpression()).getColumnName();
										flagForExpressionProject = true;
									} else if (binExp.getRightExpression() instanceof Column) {
										// Right operator is the column that should be in the schema for the reference.
										selectOp = ((Column) binExp.getRightExpression()).getColumnName();
										flagForExpressionProject = true;
									}
								}
								if (selectOp.toUpperCase().equals(colName.toUpperCase())) {
									if (flagForExpressionProject) {
										ColumnDefinition newtempColumn = new ColumnDefinition();
										newtempColumn.setColDataType(tempColumn.getColDataType());
										newtempColumn.setColumnName(selectExpression.toString().toUpperCase());
										tempColumn = newtempColumn;
									}
									ColumnDefinition newtempColumn = new ColumnDefinition();
									newtempColumn.setColDataType(tempColumn.getColDataType());
									newtempColumn.setColumnName(selectOp);
									tempColumn = newtempColumn;
									newColumnDefn.add(tempColumn);
									newRefTableName.add(this.childOperator.getRefTableName().get(j));
									//indicesToProject.add(j);
									break;
								}
							}
						}
					}
				}
			}
			/* Setting the record size */
			record = new Object[selectItems.size()];
			/* Setting the new Schema and record size */
			newSchema.setTabColumns(newColumnDefn);
			newSchema.setTableName(prevSchema.getTableName());
			newSchema.setTabAlias(prevSchema.getTabAlias());
			super.setTableSchema(newSchema);
			super.setRefTableName(newRefTableName);
		}
	}

	@Override
	public boolean hasNext() {
		if (childOperator.hasNext()) {
			prevRecord = this.childOperator.next();
			/* Case of (*) in SELECT */
			if (starFlag) {
				record = prevRecord;
				return true;
			}
			/* Case of Exp : Use Eval to evaluate */
			evalOperator evalQuery = new evalOperator(this.prevRecord, this.childOperator.getTableSchema(),
					this.childOperator.getRefTableName());
			for (int i = 0; i < this.selectExp.size(); i++) {
				try {
					record[i] = evalQuery.eval(this.selectExp.get(i).getExpression());
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			// int j = 0;
			// for (int i : indicesToProject) {
			// record[j++] = this.prevRecord[i];
			// }
			return true;
		} else {
			return false;
		}

	}

	@Override
	public Object[] next() {

		return this.record;
	}
}
