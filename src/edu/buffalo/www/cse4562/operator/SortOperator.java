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
	// The collection that contains all the rows to be sorted on the ORDER BY clause.
	private List<Object[]> rows = new ArrayList<Object[]>(10);
	// The list that stores all the ORDER BY clauses.
	private List<OrderByElement> orderByList;
	// Boolean to track first ever call for hasNext as sort works with all rows at once but needs to be competent with the design to return one row for every next call.
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
		if(firstHasNextCall) {
			// First ever call to hasNext.
			firstHasNextCall = false;
			List<ColumnDefinition> columns = this.getTableSchema().getTabColumns();
			while (childOperator.hasNext()) {
				// Grab all the rows from the underlying operator.
				Object[] readRow = new Object[this.childOperator.getTableSchema().getTabColumns().size()];
				Object[] tempRow = new Object[this.childOperator.getTableSchema().getTabColumns().size()];
				readRow = childOperator.next();
				for (int i = 0; i < readRow.length; i++) {
					// Copy the row to add to the list. Necessary because Java passes array by reference and not by value.
					tempRow[i] = readRow[i];
				}
				rows.add(tempRow);
			}
			int colIndex;
			int prevIndex = -1;
			for (int i = 0; i < orderByList.size(); i++) {
				OrderByElement orderBy = orderByList.get(i);
				Boolean asec = orderBy.isAsc();
				Column orderByColumnName = (Column) orderBy.getExpression();
				ColumnDefinition orderByColumn = new ColumnDefinition();
				// Find the index of the column to be sorted from the schema.
				for (ColumnDefinition column : columns) {
					if (column.getColumnName().toString().equals(orderByColumnName.getColumnName().toString())) {
						orderByColumn = column;
						break;
					}
				}
				colIndex = columns.indexOf(orderByColumn);
				sort(rows, colIndex, prevIndex, asec);
				if(i != 0) {
					prevIndex = colIndex;
				}
			}
			return true;
		} else {
			if(nextRowIndex == rows.size() - 1) {
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
				if(prevIndex == -1 || (Comparator(arg0[prevIndex], arg1[prevIndex]) == 0)) {
					if(!ascending) {
						return -1 * Comparator(arg0[colIndex], arg1[colIndex]);
					} else {
						return Comparator(arg0[colIndex], arg1[colIndex]);
					}
				} else {
					return 0;
				}
			}
		});
		return sortedRows;
	}

	private int Comparator(Object a, Object b) {
		if (a instanceof StringValue && b instanceof StringValue) {
			return a.toString().compareTo(b.toString());
		} else if(a instanceof LongValue && b instanceof LongValue) {
			LongValue first = new LongValue(a.toString());
			LongValue second = new LongValue(b.toString());
			if(first.toLong() < second.toLong()) {
				return -1;
			} else if (first.toLong() > second.toLong()) {
				return 1;
			} else {
				return 0;
			}
		} else if(a instanceof DoubleValue && b instanceof DoubleValue) {
			DoubleValue first = new DoubleValue(a.toString());
			DoubleValue second = new DoubleValue(b.toString());
			if(first.toDouble() < second.toDouble()) {
				return -1;
			} else if (first.toDouble() > second.toDouble()) {
				return 1;
			} else {
				return 0;
			}
		}else if(a instanceof DateValue && b instanceof DateValue) {
			DateValue first = new DateValue(a.toString());
			DateValue second = new DateValue(b.toString());
			Date operand1 = first.getValue();
			Date operand2 = second.getValue();
			if(operand1.before(operand2)) {
				return -1;
			} else if (operand1.after(operand2)) {
				return 1;
			} else {
				return 0;
			}
		} else {
			return 0;
		}
	}
}
