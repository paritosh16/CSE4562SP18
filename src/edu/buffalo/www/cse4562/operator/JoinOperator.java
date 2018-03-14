package edu.buffalo.www.cse4562.operator;

import java.sql.SQLException;
import java.util.Iterator;

import edu.buffalo.www.cse4562.TableSchema;
import edu.buffalo.www.cse4562.evaluator.evalOperator;
import edu.buffalo.www.cse4562.operator.join.BlockNestedLoopJoin;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;

public class JoinOperator extends BaseOperator implements Iterator<Object[]> {
	private Expression joinClause;
	private Object[] currentRow;
	private BlockNestedLoopJoin joiner;

	public JoinOperator(BaseOperator childOperator, BaseOperator secondChildOperator, Expression joinClause) {
		super(childOperator, secondChildOperator, childOperator.getTableSchema());
		this.joinClause = joinClause;
		this.setTableSchema(this.createCrossProductSchema(
				childOperator.getTableSchema(),
				secondChildOperator.getTableSchema()
				));
		this.joiner = new BlockNestedLoopJoin(
				this.childOperator,
				this.secondChildOperator,
				this.childOperator.getTableSchema().getNumColumns(),
				this.secondChildOperator.getTableSchema().getNumColumns()
				);
	}

	/**
	 *
	 * @param childSchema
	 * @param secondChildSchema
	 * @return schema for the cross product relation over the two children schemas
	 */
	private TableSchema createCrossProductSchema(TableSchema childSchema, TableSchema secondChildSchema) {
		// TODO: actually create the cross product schema
		return childSchema;
	}

	@Override
	public boolean hasNext() {

		while(this.joiner.hasNext()) {
			this.currentRow = this.joiner.next();

			evalOperator evaluator = new evalOperator(this.currentRow, this.getTableSchema());
			PrimitiveValue conditionStatus = null;
			try {
				// Evaluate the row for the specific condition.
				conditionStatus = evaluator.eval(this.joinClause);
				assert(conditionStatus != null);
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