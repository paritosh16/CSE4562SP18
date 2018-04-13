package edu.buffalo.www.cse4562.operator;

import java.sql.SQLException;
import java.util.ArrayList;
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
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
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
	// Boolean to keep a track if there are any group by columns.
	private Boolean isGroupByNull = false;

	public GroupByOperator(BaseOperator childOperator, List<Column> groupBy, List<Function> groupByFunction,
			List<SelectItem> oldSelectItems, Boolean groupByFlag) {
		super(childOperator, childOperator.getTableSchema());
		this.groupByList = groupBy;
		this.groupByFunctions = groupByFunction;
		this.oldSelectItems = oldSelectItems;
		this.isGroupByNull = groupByFlag;
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
			if (this.isGroupByNull) {
				// The query is vanilla aggregation query. It has no grouping by.
				List<PrimitiveValue> processedRowList = new ArrayList<PrimitiveValue>(10);
				for (int i = 0; i < this.groupByFunctions.size(); i++) {
					// Name of the aggregation function.
					String aggregationName = this.groupByFunctions.get(i).getName();
					if (aggregationName.equals("MAX")) {
						// MAX function.
						processedRowList.add(max(groupByFunctions.get(i)));
					} else if (aggregationName.equals("MIN")) {
						// MIN function.
						processedRowList.add(min(groupByFunctions.get(i)));
					} else if (aggregationName.equals("SUM")) {
						// SUM function.
						processedRowList.add(sum(groupByFunctions.get(i)));
					} else if (aggregationName.equals("AVG")) {
						// AVG function.
						processedRowList.add(avg(groupByFunctions.get(i)));
					} else if (aggregationName.equals("COUNT")) {
						// COUNT function.
						processedRowList.add(count(groupByFunctions.get(i)));
					}
				}
				// Create the output row collection from all the aggregated HashMaps.
				prepareOutputRowCollectionNoGrouping(processedRowList);
			} else {
				// The query contains aggregation along with grouping by one or more column names.
				List<Integer> groupByIndexList = getGroupByIndices();
				List<LinkedHashMap<String, PrimitiveValue>> processedRowList = new ArrayList<LinkedHashMap<String, PrimitiveValue>>(
						10);
				for (int i = 0; i < this.groupByFunctions.size(); i++) {
					// Name of the aggregation function.
					String aggregationName = this.groupByFunctions.get(i).getName();
					if (aggregationName.equals("MAX")) {
						// MAX function.
						processedRowList.add(max(groupByIndexList, groupByFunctions.get(i)));
					} else if (aggregationName.equals("MIN")) {
						// MIN function.
						processedRowList.add(min(groupByIndexList, groupByFunctions.get(i)));
					} else if (aggregationName.equals("SUM")) {
						// SUM function.
						processedRowList.add(sum(groupByIndexList, groupByFunctions.get(i)));
					} else if (aggregationName.equals("AVG")) {
						// AVG function.
						processedRowList.add(avg(groupByIndexList, groupByFunctions.get(i)));
					} else if (aggregationName.equals("COUNT")) {
						// COUNT function.
						processedRowList.add(count(groupByIndexList, groupByFunctions.get(i)));
					}
				}
				// Create the output row collection from all the aggregated HashMaps.
				prepareOutputRowCollection(processedRowList);
			}
			if (rows.size() > 0) {
				return true;
			} else {
				// No rows to return even after performing all the aggregation. Need to quit.
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
	public String toString() {
		return "GroupByOperator [groupByList=" + groupByList + "]";
	}

	@Override
	public Object[] next() {
		// Return the next row.
		return this.rows.get(nextRowIndex);
	}

	/*
	 * Function to get the indices of all the columns that the result needs to be
	 * grouped by. This will be used by the aggregate functions as a reference.
	 */
	private List<Integer> getGroupByIndices() {
		List<Integer> groupByIndices = new ArrayList<Integer>(5);
		for (int i = 0; i < this.groupByList.size(); i++) {
			String columnName = groupByList.get(i).getWholeColumnName();
			for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
				if (this.getTableSchema().getTabColumns().get(j).getColumnName().equals(columnName)) {
					// Add the index.
					groupByIndices.add(j);
				}
			}
		}
		// Return the list of indices.
		return groupByIndices;
	}

	private LinkedHashMap<String, PrimitiveValue> count(List<Integer> groupByIndexList, Function groupByFunction) {
		// Final collection that will contain all the keys and the aggregated values for
		// all the keys.
		LinkedHashMap<String, PrimitiveValue> finalRowList = new LinkedHashMap<String, PrimitiveValue>();
		// The column object to grab the value from the current row.
		Column col = new Column();
		// Set the table for the column.
		col.setColumnName(groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase());
		for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
			if (groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase().equals(
					this.getTableSchema().getTabColumns().get(j).getColumnName().toString().toUpperCase())) {
				col.setTable(new Table(this.getRefTableName().get(j)));
			}
		}
		// Process all the rows in the for loop.
		for (int i = 0; i < this.rows.size(); i++) {
			evalOperator evalObject = new evalOperator(this.rows.get(i), this.getTableSchema(), this.getRefTableName());
			// Current Value for the column (that needs to be aggregated) in the current
			// row.
			PrimitiveValue currentValue = null;
			// Prepare the list which will help building keys for HashMap.
			String[] hashKey = new String[groupByIndexList.size()];
			for (int j = 0; j < groupByIndexList.size(); j++) {
				hashKey[j] = rows.get(i)[groupByIndexList.get(j)].toString();
			}
			// Prepare the key.
			String key = "";
			for (int j = 0; j < hashKey.length; j++) {
				key = key + hashKey[j] + ";";
			}
			// Strip the last ; from the key.
			key = (String) key.subSequence(0, key.length() - 1);
			// Get the value from the HashMap for the current key. Will be null if the key
			// doesn't exist.
			PrimitiveValue countValue = finalRowList.get(key);
			// Get the value from the row.
			currentValue = evalObject.eval(col);
			if (countValue != null) {
				// Key is present in the HashMap. Need to check if we found the max value.
				try {
					Addition add = new Addition();
					add.setLeftExpression(countValue);
					add.setRightExpression(new LongValue(1));
					countValue = evalObject.eval(add);
				} catch (InvalidPrimitive e) {
					e.printStackTrace();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				finalRowList.put(key, countValue);
			} else {
				// Key is not present in the HashMap. Need to update the HashMap.
				finalRowList.put(key, new LongValue(1));
			}
		}
		// Return the processed list for all the rows.
		return finalRowList;
	}

	private PrimitiveValue count(Function groupByFunction) {
		LongValue count = new LongValue(this.rows.size());
		return count;
	}

	private LinkedHashMap<String, PrimitiveValue> max(List<Integer> groupByIndexList, Function groupByFunction) {
		// Final collection that will contain all the keys and the aggregated values for
		// all the keys.
		LinkedHashMap<String, PrimitiveValue> finalRowList = new LinkedHashMap<String, PrimitiveValue>();
		// The column object to grab the value from the current row.
		Column col = new Column();
		// Set the table for the column.
		col.setColumnName(groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase());
		for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
			if (groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase().equals(
					this.getTableSchema().getTabColumns().get(j).getColumnName().toString().toUpperCase())) {
				col.setTable(new Table(this.getRefTableName().get(j)));
			}
		}
		// Process all the rows in the for loop.
		for (int i = 0; i < this.rows.size(); i++) {
			evalOperator evalObject = new evalOperator(this.rows.get(i), this.getTableSchema(), this.getRefTableName());
			// Current Value for the column (that needs to be aggregated) in the current
			// row.
			PrimitiveValue currentValue = null;
			// Prepare the list which will help building keys for HashMap.
			String[] hashKey = new String[groupByIndexList.size()];
			for (int j = 0; j < groupByIndexList.size(); j++) {
				hashKey[j] = rows.get(i)[groupByIndexList.get(j)].toString();
			}
			// Prepare the key.
			String key = "";
			for (int j = 0; j < hashKey.length; j++) {
				key = key + hashKey[j] + ";";
			}
			// Strip the last ; from the key.
			key = (String) key.subSequence(0, key.length() - 1);
			// Get the value from the HashMap for the current key. Will be null if the key
			// doesn't exist.
			PrimitiveValue maxValue = finalRowList.get(key);
			// Get the value from the row.
			currentValue = evalObject.eval(col);
			if (maxValue != null) {
				// Key is present in the HashMap. Need to check if we found the max value.
				try {
					if (currentValue instanceof DoubleValue) {
						if (maxValue.toDouble() < currentValue.toDouble()) {
							// Found a new maximum value.
							finalRowList.put(key, currentValue);
						}
					} else if(currentValue instanceof LongValue) {
						if (maxValue.toLong() < currentValue.toLong()) {
							// Found a new maximum value.
							finalRowList.put(key, currentValue);
						}
					}
				} catch (InvalidPrimitive e) {
					e.printStackTrace();
				}
			} else {
				// Key is not present in the HashMap. Need to update the HashMap.
				finalRowList.put(key, currentValue);
			}
		}
		// Return the processed list for all the rows.
		return finalRowList;
	}

	private PrimitiveValue max(Function groupByFunction) {
		DoubleValue result = new DoubleValue(-999);
		// The column object to grab the value from the current row.
		Column col = new Column();
		// Set the table for the column.
		col.setColumnName(groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase());
		for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
			if (groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase().equals(
					this.getTableSchema().getTabColumns().get(j).getColumnName().toString().toUpperCase())) {
				col.setTable(new Table(this.getRefTableName().get(j)));
			}
		}
		// Process all the rows in the for loop.
		for (int i = 0; i < this.rows.size(); i++) {
			evalOperator evalObject = new evalOperator(this.rows.get(i), this.getTableSchema(), this.getRefTableName());
			// Get the value from the row.
			PrimitiveValue currentValue = evalObject.eval(col);
			try {
				if (currentValue.toDouble() > result.toDouble()) {
					result = (DoubleValue) currentValue;
				}
			} catch (InvalidPrimitive e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return result;
	}

	private LinkedHashMap<String, PrimitiveValue> min(List<Integer> groupByIndexList, Function groupByFunction) {
		// Final collection that will contain all the keys and the aggregated values for
		// all the keys.
		LinkedHashMap<String, PrimitiveValue> finalRowList = new LinkedHashMap<String, PrimitiveValue>();
		// The column object to grab the value from the current row.
		Column col = new Column();
		// Set the table for the column.
		col.setColumnName(groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase());
		for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
			if (groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase().equals(
					this.getTableSchema().getTabColumns().get(j).getColumnName().toString().toUpperCase())) {
				col.setTable(new Table(this.getRefTableName().get(j)));
			}
		}
		// Process all the rows in the for loop.
		for (int i = 0; i < this.rows.size(); i++) {
			evalOperator evalObject = new evalOperator(this.rows.get(i), this.getTableSchema(), this.getRefTableName());
			// Current Value for the column (that needs to be aggregated) in the current
			// row.
			PrimitiveValue currentValue = null;
			// Prepare the list which will help building keys for HashMap.
			String[] hashKey = new String[groupByIndexList.size()];
			for (int j = 0; j < groupByIndexList.size(); j++) {
				hashKey[j] = rows.get(i)[groupByIndexList.get(j)].toString();
			}
			// Prepare the key.
			String key = "";
			for (int j = 0; j < hashKey.length; j++) {
				key = key + hashKey[j] + ";";
			}
			// Strip the last ; from the key.
			key = (String) key.subSequence(0, key.length() - 1);
			// Get the value from the HashMap for the current key. Will be null if the key
			// doesn't exist.
			PrimitiveValue minValue = finalRowList.get(key);
			// Get the value from the row.
			currentValue = evalObject.eval(col);
			if (minValue != null) {
				// Key is present in the HashMap. Need to check if we found the max value.
				try {
					if (currentValue instanceof DoubleValue) {
						if (minValue.toDouble() > currentValue.toDouble()) {
							// Found a new minimum value.
							finalRowList.put(key, currentValue);
						}
					} else if(currentValue instanceof LongValue) {
						if (minValue.toLong() > currentValue.toLong()) {
							// Found a new minimum value.
							finalRowList.put(key, currentValue);
						}
					}
				} catch (InvalidPrimitive e) {
					e.printStackTrace();
				}
			} else {
				// Key is not present in the HashMap. Need to update the HashMap.
				finalRowList.put(key, currentValue);
			}
		}
		// Return the processed list for all the rows.
		return finalRowList;
	}

	private PrimitiveValue min(Function groupByFunction) {
		DoubleValue result = new DoubleValue(999999999);
		// The column object to grab the value from the current row.
		Column col = new Column();
		// Set the table for the column.
		col.setColumnName(groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase());
		for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
			if (groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase().equals(
					this.getTableSchema().getTabColumns().get(j).getColumnName().toString().toUpperCase())) {
				col.setTable(new Table(this.getRefTableName().get(j)));
			}
		}
		// Process all the rows in the for loop.
		for (int i = 0; i < this.rows.size(); i++) {
			evalOperator evalObject = new evalOperator(this.rows.get(i), this.getTableSchema(), this.getRefTableName());
			// Get the value from the row.
			PrimitiveValue currentValue = evalObject.eval(col);
			try {
				if (currentValue.toDouble() < result.toDouble()) {
					result = (DoubleValue) currentValue;
				}
			} catch (InvalidPrimitive e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return result;
	}

	private LinkedHashMap<String, PrimitiveValue> avg(List<Integer> groupByIndexList, Function groupByFunction) {
		// Final collection that will contain all the keys and the aggregated values for
		// all the keys.
		LinkedHashMap<String, PrimitiveValue> finalRowList = new LinkedHashMap<String, PrimitiveValue>();
		// HashMap to keep the count of each key. Need the count to calculate the
		// average.
		LinkedHashMap<String, PrimitiveValue> countRowList = new LinkedHashMap<String, PrimitiveValue>();
		// The column object to grab the value from the current row.
		Column col = new Column();
		// Set the table for the column.
		col.setColumnName(groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase());
		for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
			if (groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase().equals(
					this.getTableSchema().getTabColumns().get(j).getColumnName().toString().toUpperCase())) {
				col.setTable(new Table(this.getRefTableName().get(j)));
			}
		}
		// Process all the rows in the for loop.
		for (int i = 0; i < this.rows.size(); i++) {
			evalOperator evalObject = new evalOperator(this.rows.get(i), this.getTableSchema(), this.getRefTableName());
			// Current Value for the column (that needs to be aggregated) in the current
			// row.
			PrimitiveValue currentValue = null;
			// Prepare the list which will help building keys for HashMap.
			String[] hashKey = new String[groupByIndexList.size()];
			for (int j = 0; j < groupByIndexList.size(); j++) {
				hashKey[j] = rows.get(i)[groupByIndexList.get(j)].toString();
			}
			// Prepare the key.
			String key = "";
			for (int j = 0; j < hashKey.length; j++) {
				key = key + hashKey[j] + ";";
			}
			// Strip the last ; from the key.
			key = (String) key.subSequence(0, key.length() - 1);
			// Get the value from the HashMap for the current key. Will be null if the key
			// doesn't exist.
			PrimitiveValue countValue = countRowList.get(key);
			PrimitiveValue averageValue = finalRowList.get(key);
			// Get the value from the row.
			currentValue = evalObject.eval(col);
			try {
				currentValue = new DoubleValue(currentValue.toDouble());
			} catch (InvalidPrimitive e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			if (averageValue != null) {
				// Key is present in the HashMap. Need to check if we found the max value.
				try {
					//if (currentValue instanceof DoubleValue) {
					// The column value is DoubleValue. Need to perform double datatype addition.
					// Multiplication object.
					Multiplication multiply = new Multiplication();
					// Set Operands.
					multiply.setLeftExpression(averageValue);
					multiply.setRightExpression(countValue);
					// Get the previous accumulation.
					PrimitiveValue sumValue = evalObject.eval(multiply);
					Addition add = new Addition();
					// Set operands.
					add.setLeftExpression(sumValue);
					add.setRightExpression(currentValue);
					// Calculate the new sum.
					PrimitiveValue newSumValue = evalObject.eval(add);
					// Division object.
					Division divide = new Division();
					Addition addCount = new Addition();
					// Calculate the new count.
					addCount.setLeftExpression(countValue);
					addCount.setRightExpression(new DoubleValue(1));
					countValue = evalObject.eval(addCount);
					// Get the new average.
					divide.setLeftExpression(newSumValue);
					divide.setRightExpression(countValue);
					averageValue = evalObject.eval(divide);
					//					} else if (currentValue instanceof LongValue) {
					//						// The column value is DoubleValue. Need to perform double datatype addition.
					//						// Multiplication object.
					//						Multiplication multiply = new Multiplication();
					//						// Set Operands.
					//						multiply.setLeftExpression(averageValue);
					//						multiply.setRightExpression(countValue);
					//						// Get the previous accumulation.
					//						PrimitiveValue sumValue = evalObject.eval(multiply);
					//						Addition add = new Addition();
					//						// Set operands.
					//						add.setLeftExpression(sumValue);
					//						add.setRightExpression(currentValue);
					//						// Calculate the new sum.
					//						sumValue = evalObject.eval(add);
					//						// Division object.
					//						Division divide = new Division();
					//						// Calculate the new count.
					//						add.setLeftExpression(countValue);
					//						add.setRightExpression(new LongValue(1));
					//						countValue = evalObject.eval(add);
					//						// Get the new average.
					//						divide.setLeftExpression(sumValue);
					//						divide.setRightExpression(countValue);
					//						averageValue = evalObject.eval(divide);
					//					}
					// Update the sum for the key in the HashMap.
					finalRowList.put(key, averageValue);
					// Update the count in the countHashMap.
					countRowList.put(key, countValue);
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				// Key is not present in the HashMap. Need to update the HashMap.
				if (currentValue instanceof LongValue) {
					// LongValue data type.
					try {
						finalRowList.put(key, new DoubleValue(currentValue.toDouble()));
					} catch (InvalidPrimitive e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					countRowList.put(key, new DoubleValue(1));
				} else {
					// Double value data type.
					finalRowList.put(key, currentValue);
					countRowList.put(key, new DoubleValue(1));
				}
			}
		}
		// Return the processed list for all the rows.
		return finalRowList;
	}

	private PrimitiveValue avg(Function groupByFunction) {
		PrimitiveValue result = null;
		PrimitiveValue count = null;
		PrimitiveValue inc = null;
		// The column object to grab the value from the current row.
		Column col = new Column();
		// Set the table for the column.
		col.setColumnName(groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase());
		for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
			if (groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase().equals(
					this.getTableSchema().getTabColumns().get(j).getColumnName().toString().toUpperCase())) {
				col.setTable(new Table(this.getRefTableName().get(j)));
			}
		}
		// Process all the rows in the for loop.
		for (int i = 0; i < this.rows.size(); i++) {
			evalOperator evalObject = new evalOperator(this.rows.get(i), this.getTableSchema(), this.getRefTableName());
			// Get the value from the row.
			PrimitiveValue currentValue = evalObject.eval(col);
			if (i == 0 /*&& currentValue instanceof DoubleValue*/) {
				try {
					result = new DoubleValue(currentValue.toDouble());
					count = new DoubleValue(1);
					inc = new DoubleValue(1);
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//			} else if(i == 0 && currentValue instanceof LongValue) {
				//				try {
				//					result = new LongValue(currentValue.toLong());
				//					count = new LongValue(1);
				//					inc = new LongValue(1);
				//				} catch (InvalidPrimitive e) {
				//					// TODO Auto-generated catch block
				//					e.printStackTrace();
				//				}
			} else {
				Addition addCount = new Addition();
				Multiplication mult = new Multiplication();
				Division div = new Division();
				addCount.setLeftExpression(count);
				addCount.setRightExpression(inc);
				try {
					// Get the current accumulation.
					mult.setLeftExpression(result);
					mult.setRightExpression(count);
					// Get the new count.
					PrimitiveValue newCount = evalObject.eval(addCount);
					// Update the accumulation with the current value.
					PrimitiveValue sumValue = evalObject.eval(mult);
					Addition add = new Addition();
					add.setLeftExpression(sumValue);
					add.setRightExpression(currentValue);
					PrimitiveValue newSumValue = evalObject.eval(add);
					div.setLeftExpression(newSumValue);
					div.setRightExpression(newCount);
					result = evalObject.eval(div);
					count = newCount;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	private LinkedHashMap<String, PrimitiveValue> sum(List<Integer> groupByIndexList, Function groupByFunction) {
		// Final collection that will contain all the keys and the aggregated values for
		// all the keys.
		LinkedHashMap<String, PrimitiveValue> finalRowList = new LinkedHashMap<String, PrimitiveValue>();
		// The column object to grab the value from the current row.
		Column col = new Column();
		// Set the table for the column.
		col.setColumnName(groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase());
		for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
			if (groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase().equals(
					this.getTableSchema().getTabColumns().get(j).getColumnName().toString().toUpperCase())) {
				col.setTable(new Table(this.getRefTableName().get(j)));
			}
		}
		// Process all the rows in the for loop.
		for (int i = 0; i < this.rows.size(); i++) {
			evalOperator evalObject = new evalOperator(this.rows.get(i), this.getTableSchema(), this.getRefTableName());
			// Current Value for the column (that needs to be aggregated) in the current
			// row.
			PrimitiveValue currentValue = null;
			// Prepare the list which will help building keys for HashMap.
			String[] hashKey = new String[groupByIndexList.size()];
			for (int j = 0; j < groupByIndexList.size(); j++) {
				hashKey[j] = rows.get(i)[groupByIndexList.get(j)].toString();
			}
			// Prepare the key.
			String key = "";
			for (int j = 0; j < hashKey.length; j++) {
				key = key + hashKey[j] + ";";
			}
			// Strip the last ; from the key.
			key = (String) key.subSequence(0, key.length() - 1);
			// Get the value from the HashMap for the current key. Will be null if the key
			// doesn't exist.
			PrimitiveValue sumValue = finalRowList.get(key);
			// Get the value from the row.
			currentValue = evalObject.eval(col);
			if (sumValue != null) {
				// Key is present in the HashMap. Need to check if we found the max value.
				try {
					if (currentValue instanceof DoubleValue) {
						// The column value is DoubleValue. Need to perform double datatype addition.
						// Addition object.
						Addition add = new Addition();
						// Set operands.
						add.setLeftExpression(sumValue);
						add.setRightExpression(currentValue);
						// Calculate the sum.
						sumValue = evalObject.eval(add);
					} else if (currentValue instanceof LongValue) {
						// The column value is LongValue. Need to perform long datatype addition.
						// Addition object.
						Addition add = new Addition();
						// Set operands.
						add.setLeftExpression(sumValue);
						add.setRightExpression(currentValue);
						// Calculate the sum.
						sumValue = evalObject.eval(add);
					}
					// Update the sum for the key in the HashMap.
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
		// Return the processed list for all the rows.
		return finalRowList;
	}

	private PrimitiveValue sum(Function groupByFunction) {
		PrimitiveValue result = null;
		// The column object to grab the value from the current row.
		Column col = new Column();
		// Set the table for the column.
		col.setColumnName(groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase());
		for (int j = 0; j < this.getTableSchema().getTabColumns().size(); j++) {
			if (groupByFunction.getParameters().getExpressions().get(0).toString().toUpperCase().equals(
					this.getTableSchema().getTabColumns().get(j).getColumnName().toString().toUpperCase())) {
				col.setTable(new Table(this.getRefTableName().get(j)));
			}
		}
		// Process all the rows in the for loop.
		for (int i = 0; i < this.rows.size(); i++) {
			evalOperator evalObject = new evalOperator(this.rows.get(i), this.getTableSchema(), this.getRefTableName());
			// Get the value from the row.
			PrimitiveValue currentValue = evalObject.eval(col);
			if (i == 0 && currentValue instanceof DoubleValue) {
				try {
					result = new DoubleValue(currentValue.toDouble());
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if(i == 0 && currentValue instanceof LongValue) {
				try {
					result = new LongValue(currentValue.toLong());
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				Addition add = new Addition();
				try {
					add.setLeftExpression(result);
					add.setRightExpression(currentValue);
					result = evalObject.eval(add);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	private void prepareOutputRowCollection(List<LinkedHashMap<String, PrimitiveValue>> processedRowList) {
		/*
		 * The final size of the output row is the number of elements that are being
		 * projected.
		 */
		int finalRowSize = oldSelectItems.size();
		// Get all the keys from the HashMap.
		Set<String> keySet = processedRowList.get(0).keySet();
		String[] keySetArray = keySet.toArray(new String[keySet.size()]);
		List<Object[]> tempRows = new ArrayList<Object[]>(10);
		// For all the keys, get the aggregated values and prepare the output row.
		for (int i = 0; i < keySetArray.length; i++) {
			String[] keyValues = keySetArray[i].split(";");
			Object[] tempRow = new Object[finalRowSize];
			// Index to track progress in the list of HashMap that contains aggregated
			// values.
			int functionValueIndex = 0;
			// Index to track progress in the keyValues array.
			int keyValueIndex = 0;
			// Index to track progress in the temporary output array.
			int tempRowIndex = 0;
			for (int j = 0; j < this.oldSelectItems.size(); j++) {
				if (((SelectExpressionItem) this.oldSelectItems.get(j)).getExpression() instanceof Function) {
					// Value to be grabbed from the HashMap.
					tempRow[tempRowIndex] = processedRowList.get(functionValueIndex).get(keySetArray[i]);
					functionValueIndex++;
				} else {
					// Value to be grabbed from the key value array.
					tempRow[tempRowIndex] = keyValues[keyValueIndex];
					keyValueIndex++;
				}
				tempRowIndex++;
			}
			// Add the output rows to the temporary row collection.
			tempRows.add(tempRow);
		}
		// Set the rows collection to the final output rows of this operator.
		this.rows = tempRows;
	}

	private void prepareOutputRowCollectionNoGrouping(List<PrimitiveValue> processedRowList) {
		Object[] tempRow = new Object[processedRowList.size()];
		int index = 0;
		for(PrimitiveValue aggregate : processedRowList) {
			tempRow[index++] = aggregate;
		}
		List<Object[]> tempRows = new ArrayList<Object[]>(5);
		tempRows.add(tempRow);
		this.rows = tempRows;
	}
}