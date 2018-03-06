package edu.buffalo.www.cse4562.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.statement.select.Limit;

public class LimitOperator  extends BaseOperator implements Iterator<Object[]> {
	private Limit limit;
	private List<Object[]> rows = new ArrayList<Object[]>(10);
	private boolean firstHasNextCall = true;
	private int nextRowIndex = 0;

	public LimitOperator(BaseOperator childOperator, Limit limit) {
		super(childOperator, childOperator.getTableSchema());
		this.limit = limit;
	}

	@Override
	public boolean hasNext() {
		if(firstHasNextCall) {
			firstHasNextCall = false;
			while (childOperator.hasNext()) {
				Object[] readRow = new Object[this.childOperator.getTableSchema().getTabColumns().size()];
				Object[] tempRow = new Object[this.childOperator.getTableSchema().getTabColumns().size()];
				readRow = childOperator.next();
				for (int i = 0; i < readRow.length; i++) {
					tempRow[i] = readRow[i];
				}
				rows.add(tempRow);
			}
			long rowCount = limit.getRowCount();
			List<Object[]> tempRows = rows.subList(0, (int) rowCount);
			rows = tempRows;
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
