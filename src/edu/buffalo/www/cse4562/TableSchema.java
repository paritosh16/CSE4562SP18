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


}
