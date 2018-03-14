package edu.buffalo.www.cse4562.operator;

import java.util.Iterator;

import net.sf.jsqlparser.statement.select.Limit;

public class LimitOperator extends BaseOperator implements Iterator<Object[]> {
	// Limit clause.
	private Limit limit;
	private Object[] row;
	private long currentCount = 1;

	public LimitOperator(BaseOperator childOperator, Limit limit) {
		super(childOperator, childOperator.getTableSchema());
		this.limit = limit;
	}

	@Override
	public boolean hasNext() {
		if(currentCount <= limit.getRowCount()) {
			if(this.childOperator.hasNext()) {
				currentCount++;
				this.row = this.childOperator.next();
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	@Override
	public Object[] next() {
		// Return the row.
		return row;
	}
}
