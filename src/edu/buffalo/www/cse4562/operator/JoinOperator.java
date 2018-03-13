package edu.buffalo.www.cse4562.operator;

import java.util.Iterator;

import edu.buffalo.www.cse4562.TableSchema;
import net.sf.jsqlparser.expression.Expression;

public class JoinOperator extends BaseOperator implements Iterator<Object[]> {
	private Expression joinClause;
	private Object[] currentRow;

	public JoinOperator(BaseOperator childOperator, BaseOperator secondChildOperator, Expression joinClause) {
		super(childOperator, secondChildOperator, childOperator.getTableSchema());
		this.joinClause = joinClause;
		this.setTableSchema(this.createCrossProductSchema(
				childOperator.getTableSchema(),
				secondChildOperator.getTableSchema()
				));
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

		//		Nested Block Join pesudo-code:

		//		while leftTuples = readBlock(childOperator) {
		//			while rightTuples = readBlockCache(secondChildOperator) {
		//				for leftTuple in leftTuples {
		//					for rightTuple in rightTuples {
		//						emit(leftTuple, rightTuple);
		//					}
		//				}
		//			}
		//		}

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