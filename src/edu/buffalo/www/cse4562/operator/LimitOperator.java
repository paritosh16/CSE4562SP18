package edu.buffalo.www.cse4562.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.statement.select.Limit;

public class LimitOperator extends BaseOperator implements Iterator<Object[]> {
	// Limit clause.
	private Limit limit;
	// Rows that should be limited to the count.
	private List<Object[]> rows = new ArrayList<Object[]>(10);
	private boolean firstHasNextCall = true;
	private int nextRowIndex = 0;

	public LimitOperator(BaseOperator childOperator, Limit limit) {
		super(childOperator, childOperator.getTableSchema());
		this.limit = limit;
	}

	@Override
	public boolean hasNext() {
		if (firstHasNextCall) {
			// First call done. Should never come here after this interation.
			firstHasNextCall = false;
			// Get the row count to slice the list.
			long rowCount = limit.getRowCount();
			while (childOperator.hasNext()) {
				// Copy and add the row to the list which will be sliced later.
				Object[] readRow = new Object[this.childOperator.getTableSchema().getTabColumns().size()];
				Object[] tempRow = new Object[this.childOperator.getTableSchema().getTabColumns().size()];
				readRow = childOperator.next();
				for (int i = 0; i < readRow.length; i++) {
					tempRow[i] = readRow[i];
				}
				rows.add(tempRow);
			}
			// Slice the list.
			List<Object[]> tempRows = rows.subList(0, (int) rowCount);
			// Replace the list with the new sliced list.
			rows = tempRows;
			return true;
		} else {
			if (nextRowIndex == rows.size() - 1) {
				// Already returned all the rows, has to stop the iteration.
				return false;
			} else {
				// Set the next index to return.
				nextRowIndex++;
				return true;
			}
		}
	}

	@Override
	public Object[] next() {
		// Return the row.
		return rows.get(nextRowIndex);
	}
}
