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
		// Initialize the row to be read.
		Object[] readRow = null;
		// Initialize the boolean variable to be set based on the where condition.
		PrimitiveValue conditionStatus = null;
		try {
			do {
				// Read the row.
				readRow = this.childOperator.next();
				// Instantiate the operator.
				evalOperator evaluator = new evalOperator(readRow, childOperator.getTableSchema());
				try {
					// Evaluate the row for the specific condition.
					conditionStatus = evaluator.eval(this.where);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}
			} while (!(conditionStatus.toBool()) && this.childOperator.hasNext());
		} catch (InvalidPrimitive e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		try {
			if(conditionStatus.toBool()){
				// The row matched the where condition.
				// Assign the rorw to the member variable so that it can be returned by next function.
				this.currentRow = readRow;
				return true;
			} else {
				// Condition did not match on any rows and hence next() shouldn't be called after this call of hasNext.
				// If at all hasNext is called, null value will be returned.
				this.currentRow = null;
				return false;
			}
		} catch (InvalidPrimitive e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public Object[] next() {
		// Return the row that has been read and evaluated on the where condition.
		return currentRow;
	}
}