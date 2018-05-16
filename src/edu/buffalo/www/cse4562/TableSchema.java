package edu.buffalo.www.cse4562;

import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class TableSchema {
	String tableName;
	List<ColumnDefinition> tabColumns;
	String tabAlias;
	List<String> primaryKeys;
	HashMap<String, String> foreignKeyMap;
	long cardinality;

	HashMap<Object, RecordLocation> pkIndexMap;
	HashMap<Object, List<RecordLocation>> fkIndexMap;

	public long getCardinality() {
		return cardinality;
	}
	public void setCardinality(long cardinality) {
		this.cardinality = cardinality;
	}
	public String getTableName() {
		return tableName;
	}
	public List<String> getPrimaryKeys() {
		return primaryKeys;
	}
	public void setPrimaryKeys(List<String> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}
	public HashMap<String, String> getForeignKeyMap() {
		return foreignKeyMap;
	}
	public void setForeignKeyMap(HashMap<String, String> foreignKeyMap) {
		this.foreignKeyMap = foreignKeyMap;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public List<ColumnDefinition> getTabColumns() {
		return tabColumns;
	}
	public void setTabColumns(List<ColumnDefinition> tabColumns) {
		this.tabColumns = tabColumns;
	}
	public String getTabAlias() {
		return tabAlias;
	}
	public void setTabAlias(String tabAlias) {
		this.tabAlias = tabAlias;
	}

	public int getNumColumns() {
		return tabColumns.size();
	}

	public HashMap<Object, RecordLocation> getPKIndexMap() {
		return pkIndexMap;
	}

	public HashMap<Object, List<RecordLocation>> getFKIndexMap() {
		return fkIndexMap;
	}

	public void setPkIndexMap(HashMap<Object, RecordLocation> pkIndexMap) {
		this.pkIndexMap = pkIndexMap;
	}

	public void setFkIndexMap(HashMap<Object, List<RecordLocation>> fkIndexMap) {
		this.fkIndexMap = fkIndexMap;
	}

	/*
	 * return 0 if rightJoinColIndex is index of a pk
	 * return 1 if rightJoinColIndex is index of a fk
	 * return -1 otherwise
	 */
	public int checkIndex(int rightJoinColIndex) {
		for(String pk : primaryKeys) {
			if (pk.equals(tabColumns.get(rightJoinColIndex).getColumnName())) {
				return 0;
			}
		}
		if (foreignKeyMap.containsKey(tabColumns.get(rightJoinColIndex).getColumnName())) {
			return 1;
		}
		return -1;
	}

	public int getPKRecordIndex() {
		String pk = primaryKeys.get(0);
		for (int i = 0; i<tabColumns.size(); i++) {
			if (pk.equals(tabColumns.get(i).getColumnName())) {
				return i;
			}
		}
		assert(false);
		return -1;
	}
	public int getFKRecordIndex() {
		String fk = foreignKeyMap.keySet().iterator().next();
		for (int i = 0; i<tabColumns.size(); i++) {
			if (fk.equals(tabColumns.get(i).getColumnName())) {
				return i;
			}
		}
		assert(false);
		return -1;
	}


}
