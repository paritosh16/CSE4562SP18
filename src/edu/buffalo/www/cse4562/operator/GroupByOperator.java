package edu.buffalo.www.cse4562.operator;

import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class GroupByOperator extends BaseOperator implements Iterator<Object[]> {
	// The list that keeps the track of all the columns that the result should be
	// grouped by.
	private List<Column> groupByList;
	// The current row that should be returned by the next function to maintain the
	// iterator design of the project.
	private Object[] currentRow;
	// The list that keeps a track of all the functions on all the columns.
	private List<Function> groupByFunctions;
	// The list which contains all the rows after the processing.
	private List<Object[]> rows;
	// Boolean to keep a track of first ever group by call.
	private boolean firstHasNextCall = true;
	// Keep a track of which row next should return.
	private int nextRowIndex = 0;

	public GroupByOperator(BaseOperator childOperator, List<Column> groupBy, List<Function> groupByFunction) {
		super(childOperator, childOperator.getTableSchema());
		this.groupByList = groupBy;
		this.groupByFunctions = groupByFunction;
	}

	@Override
	public boolean hasNext() {
		if (firstHasNextCall) {
			// First ever has next call. Need to process all the rows by exhausting all the
			// underlying operators.
			firstHasNextCall = false;
			// Schema to refer for the GROUP BY processing.
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
			// TODO The group by logic starts from this point. All the rows have been pulled
			// from underlying operators. The group by columns are in groupByList and all
			// the aggregate function details are in the groupByFunction.
			if(rows.size() > 0) {
				return true;
			} else {
				return false;
			}
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
		return this.currentRow;
	}

}
