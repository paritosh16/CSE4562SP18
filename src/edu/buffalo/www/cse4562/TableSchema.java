package edu.buffalo.www.cse4562;

import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class TableSchema {
	String tableName;
	List<ColumnDefinition> tabColumns;
	String tabAlias;

	public String getTableName() {
		return tableName;
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
