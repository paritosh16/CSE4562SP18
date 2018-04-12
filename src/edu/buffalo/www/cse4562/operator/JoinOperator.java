package edu.buffalo.www.cse4562.operator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.www.cse4562.TableSchema;
import edu.buffalo.www.cse4562.evaluator.evalOperator;
import edu.buffalo.www.cse4562.operator.join.BaseJoin;
import edu.buffalo.www.cse4562.operator.join.BlockNestedLoopJoin;
import edu.buffalo.www.cse4562.operator.join.HashEquiJoin;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class JoinOperator extends BaseOperator implements Iterator<Object[]> {
	private Expression joinClause;
	private Object[] currentRow;
	private BaseJoin joiner;
	private boolean isEvalRequired;

	public JoinOperator(BaseOperator childOperator, BaseOperator secondChildOperator, Expression joinClause) {
		super(childOperator, secondChildOperator, childOperator.getTableSchema());
		this.joinClause = joinClause;

		// TODO: verify if this schema would work well for HashEquiJoin too
		this.setTableSchema(
				this.createCrossProductSchema(childOperator.getTableSchema(), secondChildOperator.getTableSchema()));


		this.joiner = null;

		this.isEvalRequired = true;
		this.setRefTableName(createRefTableList());
	}

	/**
	 *
	 * @param childSchema
	 * @param secondChildSchema
	 * @return schema for the cross product relation over the two children schemas
	 */
	private TableSchema createCrossProductSchema(TableSchema childSchema, TableSchema secondChildSchema) {
		TableSchema crossProdSchema = new TableSchema();
		crossProdSchema.setTableName(childSchema.getTableName() + " JOIN " + secondChildSchema.getTableName());

		List<ColumnDefinition> cols = new ArrayList<ColumnDefinition>(
				this.childOperator.getTableSchema().getNumColumns()
				+ this.secondChildOperator.getTableSchema().getNumColumns());


		for (ColumnDefinition col : childSchema.getTabColumns()) {
			cols.add(col);
			// System.out.println("child col added: " + col.toString());
		}

		for (ColumnDefinition col : secondChildSchema.getTabColumns()) {
			cols.add(col);
			// System.out.println("2nd child col added: " + col.toString());
		}

		crossProdSchema.setTabColumns(cols);
		return crossProdSchema;
	}

	private List<String> createRefTableList() {
		List<String> refs = new ArrayList<String>(
				this.childOperator.getRefTableName().size() + this.secondChildOperator.getRefTableName().size());

		for(String tabName : this.childOperator.getRefTableName()) {
			refs.add(tabName);
		}

		for(String tabName : this.secondChildOperator.getRefTableName()) {
			refs.add(tabName);
		}

		return refs;
	}

	private boolean testJoinClauseIsEqui() {
		// return false if joinClause anything other than a.b = c.d
		return this.joinClause instanceof EqualsTo;
	}

	public int getColIndex(String operand, List<ColumnDefinition> colList,BaseOperator child) {
		String tabName = null;
		String colName = null;
		if (operand.contains(".")) {
			// The clause item contains a ., i.e. TableName.ColumnName
			tabName = operand.split("\\.")[0];
			colName = operand.split("\\.")[1];

			// The new column is not an alias.
			// Above comment comes from ProjectionOperator -- where this block was copied from
			for (int j = 0; j < colList.size(); j++) {
				if (colName.toUpperCase().equals(colList.get(j).getColumnName().toUpperCase())) {
					assert(child.getRefTableName().get(j).equals(tabName));
					return j;

				}
			}
		} else {
			colName = operand;
			// The new column is not an alias.
			// Above comment comes from ProjectionOperator -- where this block was copied from
			for (int j = 0; j < colList.size(); j++) {
				if (colName.toUpperCase().equals(colList.get(j).getColumnName().toUpperCase())) {
					return j;
				}
			}
		}
		return -1;
	}

	public boolean enableHashEquiJoin() {
		// Expression must be of type EqualsTo
		if (!testJoinClauseIsEqui()) {
			return false;
		}

		String lhs = ((EqualsTo)this.joinClause).getLeftExpression().toString();
		String rhs = ((EqualsTo)this.joinClause).getRightExpression().toString();
		int leftJoinerColIndex = -1;
		int rightJoinerColIndex = -1;

		List<ColumnDefinition> lhsCols = this.secondChildOperator.getTableSchema().getTabColumns();
		List<ColumnDefinition> rhsCols = this.childOperator.getTableSchema().getTabColumns();

		leftJoinerColIndex = getColIndex(lhs, lhsCols, secondChildOperator);

		if (leftJoinerColIndex == -1) {
			leftJoinerColIndex = getColIndex(rhs, lhsCols, secondChildOperator);
			rightJoinerColIndex = getColIndex(lhs, rhsCols, childOperator);
		} else {
			rightJoinerColIndex = getColIndex(rhs, rhsCols, childOperator);
		}

		this.joiner = new HashEquiJoin(this.secondChildOperator, this.childOperator,
				this.secondChildOperator.getTableSchema().getNumColumns(),
				this.childOperator.getTableSchema().getNumColumns(),
				leftJoinerColIndex, rightJoinerColIndex);
		this.isEvalRequired = false;
		return true;
	}

	public void setJoinClause(Expression joinClause) {
		this.joinClause = joinClause;
	}

	@Override
	public boolean hasNext() {

		if (this.joiner == null) {
			this.joiner = new BlockNestedLoopJoin(this.secondChildOperator, this.childOperator,
					this.secondChildOperator.getTableSchema().getNumColumns(),
					this.childOperator.getTableSchema().getNumColumns());
		}
		while (this.joiner.hasNext()) {
			this.currentRow = this.joiner.next();

			if (!this.isEvalRequired) {
				// current row is good to consume, no more eval required
				return true;
			}
			evalOperator evaluator = new evalOperator(this.currentRow, this.getTableSchema(), this.getRefTableName());
			PrimitiveValue conditionStatus = null;
			try {
				// Evaluate the row for the specific condition.
				// if (this.joinClause == null) {
				// System.out.println("Got NULL joinClause");
				// }
				if (this.joinClause == null) {
					return true;
				}
				conditionStatus = evaluator.eval(this.joinClause);
				if (conditionStatus == null) {
					System.out.println("Null returns on eval()");
				}
				if (conditionStatus.toBool()) {
					return true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}
		}
		return false;
	}

	@Override
	public Object[] next() {
		// Return the row that has been read and evaluated on the where condition.
		return currentRow;
	}

	@Override
	public void setAlias(String tabAlias) {
		super.getTableSchema().setTabAlias(tabAlias);
	}
}