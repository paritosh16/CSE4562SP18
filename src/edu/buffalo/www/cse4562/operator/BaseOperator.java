package edu.buffalo.www.cse4562.operator;

import java.util.Iterator;

import edu.buffalo.www.cse4562.TableSchema;

// TODO make BaseOperator an abstract class if possible
public class BaseOperator implements Iterator<Object[]> {
<<<<<<< HEAD
	protected BaseOperator childOperator;
	protected BaseOperator secondChildOperator;
=======
	protected BaseOperator leftOperator;
	protected BaseOperator rightOperator;
>>>>>>> c0e3df404e12d9d4944852baf284109e336ad800
	private TableSchema tableSchema;

	/**
	 * @param childOperator
	 */
<<<<<<< HEAD
	public BaseOperator(BaseOperator childOperator, BaseOperator secondChildOperator, TableSchema tableSchema) {
		super();
		this.childOperator = childOperator;
		this.secondChildOperator = secondChildOperator;
=======
	public BaseOperator(BaseOperator leftOperator, TableSchema tableSchema) {
		super();
		this.leftOperator = leftOperator;
		this.tableSchema = tableSchema;
		this.tableSchema.setTabAlias(this.tableSchema.getTableName());
	}

	public BaseOperator(BaseOperator leftOperator, BaseOperator rightOperator,TableSchema tableSchema) {
		super();
		this.leftOperator = leftOperator;
		this.rightOperator = leftOperator;
>>>>>>> c0e3df404e12d9d4944852baf284109e336ad800
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
