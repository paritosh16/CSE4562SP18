package edu.buffalo.www.cse4562.operator;

import java.sql.SQLException;
import java.util.Iterator;

import edu.buffalo.www.cse4562.evaluator.evalOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;

public class SelectionOperator extends BaseOperator implements Iterator<Object[]> {
	// The where expression which will be used to evaluate the select condition.
	private Expression where;
	private Object[] currentRow;

	public SelectionOperator(BaseOperator childOperator, Expression selectExpression) {
		// Call the constructor of the parent class to set the absolutely basic
		// attributes.
		super(childOperator, childOperator.getTableSchema());
		// Assign where the expression from the query statement.
		this.where = selectExpression;
	}

	@Override
	public boolean hasNext() {
		Object[] readRow = null;
		PrimitiveValue conditionStatus = null;
		try {
			do {
				readRow = this.childOperator.next();
				evalOperator evaluator = new evalOperator(readRow, childOperator.getTableSchema());
				try {
					conditionStatus = evaluator.eval(this.where);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					// FIXME Handle the exception gracefully.
					e.printStackTrace();
				}
			} while (!(conditionStatus.toBool()) && this.childOperator.hasNext());
		} catch (InvalidPrimitive e) {
			// TODO Auto-generated catch block
			// TODO Handle the exception gracefully.
			e.printStackTrace();
		}
		// TODO Handle the case where all the rows do not match the condition. Current
		// implementation returns the last row in this case.
		try {
			if(conditionStatus.toBool()){
				this.currentRow = readRow;
				return true;
			} else {
				return false;
			}
		} catch (InvalidPrimitive e) {
			// TODO Auto-generated catch block
			// Handle the exception gracefully.
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public Object[] next() {
		return currentRow;
	}
}