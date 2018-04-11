package edu.buffalo.www.cse4562.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.www.cse4562.TableSchema;

// TODO make BaseOperator an abstract class if possible
public class BaseOperator implements Iterator<Object[]> {
	protected BaseOperator childOperator;
	protected BaseOperator secondChildOperator;
	private TableSchema tableSchema;
	private List<String> refTableName = new ArrayList<String>(10);
	private boolean isOptimzed = false;
	public boolean isOptimzed() {
		return isOptimzed;
	}

	public void setOptimzed(boolean isOptimzed) {
		this.isOptimzed = isOptimzed;
	}

	/**
	 * @param childOperator
	 */
	public BaseOperator(BaseOperator childOperator, BaseOperator secondChildOperator, TableSchema tableSchema) {
		super();
		this.childOperator = childOperator;
		this.secondChildOperator = secondChildOperator;
		this.tableSchema = tableSchema;
		if(childOperator != null) {
			for(int i = 0; i < this.tableSchema.getTabColumns().size(); i++) {
				this.refTableName.add(childOperator.getRefTableName().get(i));
			}
		} else {
			for(int i = 0; i < this.tableSchema.getTabColumns().size(); i++) {
				this.refTableName.add(tableSchema.getTableName().toString());
			}
		}
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

	public List<String> getRefTableName() {
		return refTableName;
	}

	public void setRefTableName(List<String> refTable) {
		this.refTableName = refTable;
	}

	public BaseOperator getChildOperator() {
		return childOperator;
	}

	public void setChildOperator(BaseOperator childOperator) {
		this.childOperator = childOperator;
	}

	public BaseOperator getSecondChildOperator() {
		return secondChildOperator;
	}

	public void setSecondChildOperator(BaseOperator secondChildOperator) {
		this.secondChildOperator = secondChildOperator;
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
	public void setAlias(String aliasName) {
		for(int i = 0 ; i < this.refTableName.size(); i++)
		{
			refTableName.set(i, aliasName);
		}

	}

}
