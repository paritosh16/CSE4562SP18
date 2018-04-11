package edu.buffalo.www.cse4562.operator;

import java.sql.SQLException;
import java.util.Iterator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import edu.buffalo.www.cse4562.evaluator.evalOperator;

public class SelectionOperator extends BaseOperator implements Iterator<Object[]> {
	// The where expression which will be used to evaluate the select condition.
	private Expression where;


	private Object[] currentRow;

	public Expression getWhere() {
		return where;
	}

	public void setWhere(Expression where) {
		this.where = where;
	}

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
		boolean flag = false;

		while (this.childOperator.hasNext()) {
			// Read the row.
			readRow = this.childOperator.next();
			// Instantiate the operator.
			evalOperator evaluator = new evalOperator(readRow, this.getTableSchema(), this.getRefTableName());
			try {
				// Evaluate the row for the specific condition.
				conditionStatus = evaluator.eval(this.where);
				if (conditionStatus == null) {
					// Eval has returned null. Need to check why it has done that.
					flag = false;
				} else {
					// Assign the value in case eval did not return null.
					flag = conditionStatus.toBool();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
			if (flag) {
				this.currentRow = readRow;
				return true;
			}
		}
		return false;
	}

	@Override
	public Object[] next() {
		// Return the row that has been read and evaluated on the where condition.
		return currentRow;
	}
}