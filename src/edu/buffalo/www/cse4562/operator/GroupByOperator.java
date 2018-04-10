package edu.buffalo.www.cse4562.operator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import edu.buffalo.www.cse4562.evaluator.evalOperator;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class GroupByOperator extends BaseOperator implements Iterator<Object[]> {
	// The list that keeps the track of all the columns that the result should be
	// grouped by.
	private List<Column> groupByList;
	// The list that keeps a track of all the functions on all the columns.
	private List<Function> groupByFunctions;
	// The list which contains all the rows after the processing.
	private List<Object[]> rows = new ArrayList<Object[]>(10);
	// Boolean to keep a track of first ever group by call.
	private boolean firstHasNextCall = true;
	// Keep a track of which row next should return.
	private int nextRowIndex = 0;
	// The sequence of projections.
	private List<SelectItem> oldSelectItems = new ArrayList<SelectItem>(10);

	public GroupByOperator(BaseOperator childOperator, List<Column> groupBy, List<Function> groupByFunction,
			List<SelectItem> oldSelectItems) {
		super(childOperator, childOperator.getTableSchema());
		this.groupByList = groupBy;
		this.groupByFunctions = groupByFunction;
		this.oldSelectItems = oldSelectItems;
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
			List<Integer> groupByIndexList = getGroupByIndices();
			List<LinkedHashMap<String, PrimitiveValue>> processedRowList = new ArrayList<LinkedHashMap<String, PrimitiveValue>>(
					10);
			for (int i = 0; i < this.groupByFunctions.size(); i++) {
				String aggregationName = this.groupByFunctions.get(i).getName();
				if (aggregationName.equals("MAX")) {
					processedRowList.add(max(groupByIndexList, groupByFunctions.get(i)));
				} else if (aggregationName.equals("MIN")) {
					processedRowList.add(min(groupByIndexList, groupByFunctions.get(i)));
				} else if (aggregationName.equals("SUM")) {
					processedRowList.add(sum(groupByIndexList, groupByFunctions.get(i)));
				} else if (aggregationName.equals("AVG")) {
					processedRowList.add(avg(groupByIndexList, groupByFunctions.get(i)));
				} else if (aggregationName.equals("COUNT")) {
					processedRowList.add(count(groupByIndexList, groupByFunctions.get(i)));
				}
			}
			prepareOutputRowCollection(processedRowList);
			// TODO The group by logic starts from this point. All the rows have been pulled
			// from underlying operators. The group by columns are in groupByList and all
			// the aggregate function details are in the groupByFunction.
			if (rows.size() > 0) {
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
		return this.rows.get(nextRowIndex);
	}

	private List<Integer> getGroupByIndices() {
		List<Integer> groupByIndices = new ArrayList<Integer>(5);
		for (int i = 0; i < this.groupByList.size(); i++) {
			String columnName = groupByList.get(i).getColumnName();
			for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
				if (this.getTableSchema().getTabColumns().get(j).getColumnName().equals(columnName)) {
					groupByIndices.add(j);
				}
			}
		}
		return groupByIndices;
	}

	private LinkedHashMap<String, PrimitiveValue> count(List<Integer> groupByIndexList, Function groupByFunction) {
		LinkedHashMap<String, PrimitiveValue> finalRowList = new LinkedHashMap<String, PrimitiveValue>();
		return finalRowList;
	}

	private LinkedHashMap<String, PrimitiveValue> max(List<Integer> groupByIndexList, Function groupByFunction) {
		LinkedHashMap<String, PrimitiveValue> finalRowList = new LinkedHashMap<String, PrimitiveValue>();
		for (int i = 0; i < this.rows.size(); i++) {
			evalOperator evalObject = new evalOperator(this.rows.get(i), this.getTableSchema(), this.getRefTableName());
			PrimitiveValue currentValue = null;
			Column col = new Column();
			// Prepare the key for HashMap.
			String[] hashKey = new String[groupByIndexList.size()];
			for(int j = 0; j < groupByIndexList.size(); j++) {
				hashKey[j] = rows.get(i)[groupByIndexList.get(j)].toString();
			}
			Arrays.sort(hashKey);
			String key = "";
			for (int j = 0; j < hashKey.length; j++) {
				key = key + hashKey[j] + ";";
			}
			key = (String) key.subSequence(0, key.length() - 1);
			PrimitiveValue maxValue = finalRowList.get(key);
			col.setColumnName(groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase());
			for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
				if (groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase()
						.equals(this.getTableSchema().getTabColumns().get(j).getColumnName().toString().toUpperCase())) {
					col.setTable(new Table(this.getRefTableName().get(j)));
				}
			}
			currentValue = evalObject.eval(col);
			if (maxValue != null) {
				// Key is present in the HashMap. Need to check if we found the max value.
				try {
					if (maxValue.toDouble() < currentValue.toDouble()) {
						// Found a new maximum value.
						finalRowList.put(key, currentValue);
					}
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				// Key is not present in the HashMap. Need to update the HashMap.
				finalRowList.put(key, currentValue);
			}
		}
		return finalRowList;
	}

	private LinkedHashMap<String, PrimitiveValue> min(List<Integer> groupByIndexList, Function groupByFunction) {
		LinkedHashMap<String, PrimitiveValue> finalRowList = new LinkedHashMap<String, PrimitiveValue>();
		for (int i = 0; i < this.rows.size(); i++) {
			evalOperator evalObject = new evalOperator(this.rows.get(i), this.getTableSchema(), this.getRefTableName());
			PrimitiveValue currentValue = null;
			Column col = new Column();
			// Prepare the key for HashMap.
			String[] hashKey = new String[groupByIndexList.size()];
			for(int j = 0; j < groupByIndexList.size(); j++) {
				hashKey[j] = rows.get(i)[groupByIndexList.get(j)].toString();
			}
			Arrays.sort(hashKey);
			String key = "";
			for (int j = 0; j < hashKey.length; j++) {
				key = key + hashKey[j] + ";";
			}
			key = (String) key.subSequence(0, key.length() - 1);
			PrimitiveValue maxValue = finalRowList.get(key);
			col.setColumnName(groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase());
			for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
				if (groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase()
						.equals(this.getTableSchema().getTabColumns().get(j).getColumnName().toString().toUpperCase())) {
					col.setTable(new Table(this.getRefTableName().get(j)));
				}
			}
			currentValue = evalObject.eval(col);
			if (maxValue != null) {
				// Key is present in the HashMap. Need to check if we found the max value.
				try {
					if (maxValue.toDouble() > currentValue.toDouble()) {
						// Found a new maximum value.
						finalRowList.put(key, currentValue);
					}
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				// Key is not present in the HashMap. Need to update the HashMap.
				finalRowList.put(key, currentValue);
			}
		}
		return finalRowList;
	}

	private LinkedHashMap<String, PrimitiveValue> avg(List<Integer> groupByIndexList, Function groupByFunction) {
		LinkedHashMap<String, PrimitiveValue> finalRowList = new LinkedHashMap<String, PrimitiveValue>();
		return finalRowList;
	}

	private LinkedHashMap<String, PrimitiveValue> sum(List<Integer> groupByIndexList, Function groupByFunction) {
		LinkedHashMap<String, PrimitiveValue> finalRowList = new LinkedHashMap<String, PrimitiveValue>();
		for (int i = 0; i < this.rows.size(); i++) {
			evalOperator evalObject = new evalOperator(this.rows.get(i), this.getTableSchema(), this.getRefTableName());
			PrimitiveValue currentValue = null;
			Column col = new Column();
			// Prepare the key for HashMap.
			String[] hashKey = new String[groupByIndexList.size()];
			for(int j = 0; j < groupByIndexList.size(); j++) {
				hashKey[j] = rows.get(i)[groupByIndexList.get(j)].toString();
			}
			Arrays.sort(hashKey);
			String key = "";
			for (int j = 0; j < hashKey.length; j++) {
				key = key + hashKey[j] + ";";
			}
			key = (String) key.subSequence(0, key.length() - 1);
			PrimitiveValue sumValue = finalRowList.get(key);
			col.setColumnName(groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase());
			for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
				if (groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase()
						.equals(this.getTableSchema().getTabColumns().get(j).getColumnName().toString().toUpperCase())) {
					col.setTable(new Table(this.getRefTableName().get(j)));
				}
			}
			currentValue = evalObject.eval(col);
			if (sumValue != null) {
				// Key is present in the HashMap. Need to check if we found the max value.
				try {
					if(currentValue instanceof DoubleValue) {
						Addition add = new Addition();
						add.setLeftExpression(sumValue);
						add.setRightExpression(currentValue);
						sumValue = evalObject.eval(add);
					} else if(currentValue instanceof LongValue) {
						Addition add = new Addition();
						add.setLeftExpression(sumValue);
						add.setRightExpression(currentValue);
						sumValue = evalObject.eval(add);
					}
					finalRowList.put(key, sumValue);
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				// Key is not present in the HashMap. Need to update the HashMap.
				finalRowList.put(key, currentValue);
			}
		}
		return finalRowList;
	}

	private void prepareOutputRowCollection(List<LinkedHashMap<String, PrimitiveValue>> processedRowList) {
		int finalRowSize = processedRowList.size() + groupByList.size();
		Set<String> keySet = processedRowList.get(0).keySet();
		String[] keySetArray = keySet.toArray(new String[keySet.size()]);
		List<Object[]> tempRows = new ArrayList<Object[]>(10);
		for (int i = 0; i < keySetArray.length; i++) {
			String[] keyValues = keySetArray[i].split(";");
			Object[] tempRow = new Object[finalRowSize];
			int functionValueIndex = 0;
			int keyValueIndex = 0;
			int tempRowIndex = 0;
			for (int j = 0; j < this.oldSelectItems.size(); j++) {
				if (((SelectExpressionItem) this.oldSelectItems.get(j)).getExpression() instanceof Function) {
					tempRow[tempRowIndex] = processedRowList.get(functionValueIndex).get(keySetArray[i]);
					functionValueIndex++;
				} else {
					tempRow[tempRowIndex] = keyValues[keyValueIndex];
					keyValueIndex++;
				}
				tempRowIndex++;
			}
			tempRows.add(tempRow);
		}
		this.rows = tempRows;
	}
}
