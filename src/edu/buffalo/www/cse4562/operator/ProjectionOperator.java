package edu.buffalo.www.cse4562.operator;

import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOperator extends BaseOperator implements Iterator<Object[]> {

	private List<SelectItem> selectItems;

	public ProjectionOperator(BaseOperator childOperator, List<SelectItem> selectItems) {
		super(childOperator, childOperator.getTableSchema());
		this.selectItems = selectItems;
	}

	@Override
	public boolean hasNext() {
		// FIXME: Implement this correctly
		return this.childOperator.hasNext();
	}

	@Override
	public Object[] next() {
		Object[] readRow = this.childOperator.next();
		// TODO: actual projection logic goes here
		// make use of tableSchema and selectItems members
		return readRow;
	}

	@Override
	public void setAlias(String string) {
		// TODO: delegate this to TableSchema class
		// remember this maybe called by an operator above this operator..
	}

}
