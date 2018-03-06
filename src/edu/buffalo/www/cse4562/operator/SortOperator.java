package edu.buffalo.www.cse4562.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortOperator extends BaseOperator implements Iterator<Object[]> {
	// The collection that contains all the rows to be sorted on the criteria.
	private List<Object[]> rows = new ArrayList<Object[]>(10);
	private List<OrderByElement> orderByList;
	private boolean firstHasNextCall = true;
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
			firstHasNextCall = false;
			OrderByElement orderBy = orderByList.get(0);
			List<ColumnDefinition> columns = this.getTableSchema().getTabColumns();
			while (childOperator.hasNext()) {
				Object[] readRow = new Object[this.childOperator.getTableSchema().getTabColumns().size()];
				Object[] tempRow = new Object[this.childOperator.getTableSchema().getTabColumns().size()];
				readRow = childOperator.next();
				for (int i = 0; i < readRow.length; i++) {
					tempRow[i] = readRow[i];
				}
				rows.add(tempRow);
			}
			Boolean asec = orderBy.isAsc();
			Column orderByColumnName = (Column) orderBy.getExpression();
			ColumnDefinition orderByColumn = new ColumnDefinition();
			for (ColumnDefinition column : columns) {
				if (column.getColumnName().toString().equals(orderByColumnName.getColumnName().toString())) {
					orderByColumn = column;
					break;
				}
			}
			int colIndex = columns.indexOf(orderByColumn);
			Collections.sort(this.rows, new Comparator<Object[]>() {
				@Override
				public int compare(Object[] arg0, Object[] arg1) {
					if (arg0[1] instanceof StringValue && arg1[1] instanceof StringValue) {
						return arg0[colIndex].toString().compareTo(arg1[colIndex].toString());
					}
					return 0;
				}
			});
			if (!asec) {
				Collections.reverse(this.rows);
			}
			return true;
		} else {
			if(nextRowIndex == rows.size() - 1) {
				return false;
			} else {
				nextRowIndex++;
				return true;
			}
		}
	}

	@Override
	public Object[] next() {
		return rows.get(nextRowIndex);
	}
}
