package edu.buffalo.www.cse4562;

import java.util.HashMap;

import edu.buffalo.www.cse4562.operator.BaseOperator;
import edu.buffalo.www.cse4562.parser.SimpleParser;
import edu.buffalo.www.cse4562.parser.TreeOptimizer;
import net.sf.jsqlparser.statement.Statement;

/**
 * Takes a Statement object and returns an iterator (or array) of result tuples
 * Uses SimpleParser which returns a List of BaseOperator's child instances.
 *
 * All information about how to execute is contained in concrete Operator
 * classes
 */
public class SimpleQueryProcessor {

	private BaseOperator rootOperator;

	/**
	 * Map containing the schema objects This map currently encapsulates the
	 * Schema/Storage layer \ which will evolve into its own class for future phases
	 */
	private HashMap<String, TableSchema> schemaRegister;

	/**
	 */
	public SimpleQueryProcessor() {
		super();
		this.schemaRegister = new HashMap<String, TableSchema>();
		rootOperator = null;
	}

	public boolean processOne(Statement s) {
		// TODO Auto-generated method stub
		SimpleParser parser = new SimpleParser(this.schemaRegister);
		TreeOptimizer optimizer = new TreeOptimizer();
		boolean success = parser.parse(s);
		boolean pushdown = optimizer.splitTreeSelections(parser.getOperatorRoot());
		//boolean res = optimizer.optimizeSelectionPushdown(parser.getOperatorRoot());
		if (success) {
			setRootOperator(parser.getOperatorRoot());
			// sanity checks over rootOperator may go here
			// else after this function return, caller will expect to
			// be able to consume rows from rootOperator.
		} else {
			return false;
		}
		return true;
	}

	public BaseOperator getRootOperator() {
		return rootOperator;
	}

	public void setRootOperator(BaseOperator rootOperator) {
		this.rootOperator = rootOperator;
	}
}
