package edu.buffalo.www.cse4562.operator;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortOperator extends BaseOperator implements Iterator<Object[]> {
	// The collection that contains all the rows to be sorted on the ORDER BY
	// clause.
	private List<Object[]> rows = new ArrayList<Object[]>(10);
	// The list that stores all the ORDER BY clauses.
	private List<OrderByElement> orderByList;
	// Boolean to track first ever call for hasNext as sort works with all rows at
	// once but needs to be competent with the design to return one row for every
	// next call.
	private boolean firstHasNextCall = true;
	// Keep a track of which row next should return.
	private int nextRowIndex = 0;

	public SortOperator(BaseOperator childOperator, List<OrderByElement> orderByList) {
		// Call the constructor of the parent class to set the absolutely basic
		// attributes.
		super(childOperator, childOperator.getTableSchema());
		this.orderByList = orderByList;
	}

	@Override
	public boolean hasNext() {
		if (firstHasNextCall) {
			// First ever call to hasNext.
			firstHasNextCall = false;
			int colIndex;
			int prevIndex = -1;
			// Schema to refer while finding the ORDER BY column.
			List<ColumnDefinition> columns = this.getTableSchema().getTabColumns();
			while (childOperator.hasNext()) {
				// Grab all the rows from the underlying operator.
				Object[] readRow = new Object[this.childOperator.getTableSchema().getTabColumns().size()];
				Object[] tempRow = new Object[this.childOperator.getTableSchema().getTabColumns().size()];
				readRow = childOperator.next();
				for (int i = 0; i < readRow.length; i++) {
					// Copy the row to add to the list. Necessary because Java passes array by
					// reference and not by value.
					tempRow[i] = readRow[i];
				}
				rows.add(tempRow);
			}
			for (int i = 0; i < orderByList.size(); i++) {
				// Get the order by clause.
				OrderByElement orderBy = orderByList.get(i);
				// ASEC or DESC.
				Boolean asec = orderBy.isAsc();
				Column orderByColumnName = (Column) orderBy.getExpression();
				// Column Definition of the ORDER BY column.
				ColumnDefinition orderByColumn = new ColumnDefinition();
				// Find the index of the column to be sorted from the schema.
				for (ColumnDefinition column : columns) {
					if (column.getColumnName().toString().equals(orderByColumnName.getColumnName().toString())) {
						// Got the column for ORDER BY.
						orderByColumn = column;
						break;
					}
				}
				// The column index, value at which should be used to order the rows.
				colIndex = columns.indexOf(orderByColumn);
				// Sort the collection.
				sort(rows, colIndex, prevIndex, asec);
				if (i != 0) {
					// If there is next iteration at all, current colIndex will be used as the
					// prevIndex in the Comparator.
					prevIndex = colIndex;
				}
			}
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

	private List<Object[]> sort(List<Object[]> rows, int colIndex, int prevIndex, boolean ascending) {
		List<Object[]> sortedRows = rows;
		Collections.sort(sortedRows, new Comparator<Object[]>() {
			@Override
			public int compare(Object[] arg0, Object[] arg1) {
				if (prevIndex == -1 || (Comparator(arg0[prevIndex], arg1[prevIndex]) == 0)) {
					// prevIndex value -1 ensures that this is the first ORDER BY clause and the
					// values should be compared. If values at prevIndex(not -1) are equal, then
					// compare the values at colIndex.
					if (!ascending) {
						// The ordering is descending, so the return the opposite of what the natural
						// comparison is.
						return -1 * Comparator(arg0[colIndex], arg1[colIndex]);
					} else {
						// The ordering is ascending, so return the natural comparison.
						return Comparator(arg0[colIndex], arg1[colIndex]);
					}
				} else {
					// The values at prevIndex aren't equal, so the whatever the current sequence
					// is, should be preserved.
					return 0;
				}
			}
		});
		// Sorted list of rows.
		return sortedRows;
	}

	private int Comparator(Object a, Object b) {
		if (a instanceof StringValue && b instanceof StringValue) {
			// Compare the two string lexicographically.
			return a.toString().compareTo(b.toString());
		} else if (a instanceof LongValue && b instanceof LongValue) {
			// Convert to JSQLParser LongValue.
			LongValue first = new LongValue(a.toString());
			LongValue second = new LongValue(b.toString());
			if (first.toLong() < second.toLong()) {
				// Less than.
				return -1;
			} else if (first.toLong() > second.toLong()) {
				// Greater than.
				return 1;
			} else {
				// Equal.
				return 0;
			}
		} else if (a instanceof DoubleValue && b instanceof DoubleValue) {
			// Convert to JSQLParser DoubleValue.
			DoubleValue first = new DoubleValue(a.toString());
			DoubleValue second = new DoubleValue(b.toString());
			if (first.toDouble() < second.toDouble()) {
				// Less than.
				return -1;
			} else if (first.toDouble() > second.toDouble()) {
				// Greater than.
				return 1;
			} else {
				// Equal.
				return 0;
			}
		} else if (a instanceof DateValue && b instanceof DateValue) {
			// Convert to JSQLParser DateValue.
			DateValue first = new DateValue(a.toString());
			DateValue second = new DateValue(b.toString());
			// Convert to Java Date.
			Date operand1 = first.getValue();
			Date operand2 = second.getValue();
			if (operand1.before(operand2)) {
				// Less than.
				return -1;
			} else if (operand1.after(operand2)) {
				// Greater than.
				return 1;
			} else {
				// Equal.
				return 0;
			}
		} else {
			// Cannot compare because the type of the argument was out of scope of the
			// global schema. Return o and keep the rows in whatever sequence they were.
			return 0;
		}
	}
}
