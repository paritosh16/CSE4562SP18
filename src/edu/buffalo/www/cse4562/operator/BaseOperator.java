package edu.buffalo.www.cse4562.operator;

import java.util.Iterator;

import edu.buffalo.www.cse4562.TableSchema;

// TODO make BaseOperator an abstract class if possible
public class BaseOperator implements Iterator<Object[]> {
	protected BaseOperator childOperator;
	protected BaseOperator secondChildOperator;
	private TableSchema tableSchema;

	/**
	 * @param childOperator
	 */
	public BaseOperator(BaseOperator childOperator, BaseOperator secondChildOperator, TableSchema tableSchema) {
		super();
		this.childOperator = childOperator;
		this.secondChildOperator = secondChildOperator;
		this.tableSchema = tableSchema;
		this.tableSchema.setTabAlias(this.tableSchema.getTableName());
	}

	public BaseOperator(BaseOperator childOperator, TableSchema tableSchema) {
		this(childOperator, null, tableSchema);
	}

	public TableSchema getTableSchema() {
		return tableSchema;
	}

	public void setTableSchema(TableSchema tableSchema) {
		this.tableSchema = tableSchema;
	}

	@Override
	public boolean hasNext() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] next() {
		throw new UnsupportedOperationException();
	}

	/**
	 * called by parser during operator chain creation.. whenever an alias is encountered
	 * @param string
	 */
	public void setAlias(String string) {
		// TODO concrete implementation in child classes
		throw new UnsupportedOperationException();
	}

}
