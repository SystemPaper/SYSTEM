package org.tool.system.example.ERExample;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.tool.system.example.util.InputDataSet;
import org.tool.system.example.util.Method;
import org.tool.system.incremental.incrementalER.*;



public class IncrementalERExample {



	public static void main(String args[]) throws Exception {

		Method method = Method.SRC_MAX_BOTH;
		InputDataSet dataset = InputDataSet.G;
		Double threshold = 0.80d;

		new IncrementalERExample().execute(method, dataset, threshold);
	}

	public void execute(Method method, InputDataSet dataset, Double threshold) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();

		String runConfigPath = getRunConfigPath (method, dataset);
		String testType = getTestType(method);

		switch (testType) {

			case "Entity_Batch":
				new EntityBatchER().execute(runConfigPath, method.ordinal(), threshold, dataset.toString());
				break;
			case "Entity_Incremental":
				new EntityIncrementalER().execute(runConfigPath, method.ordinal(), threshold, dataset.toString());
				break;
			case "Source_Batch":
				new SourceBatchER().execute(runConfigPath, method.ordinal()-11, threshold, dataset.toString());
				break;
			case "Source_Incremental":
				new SourceIncrementalER().execute(runConfigPath, method.ordinal()-11, threshold, dataset.toString());
				break;

		}


	}


	private String getRunConfigPath(Method method, InputDataSet dataset) {
		String path = "../SYSTEM/runConfigs/";
		String name = dataset.toString();
		if (method.toString().contains("SRC"))
			name+= "Src.xml";
		else
			name+= "Ent.xml";
		return path+name;
	}
	private String getTestType(Method method) {
		String entity_src = "Entity";
		if (method.toString().contains("SRC"))
			entity_src = "Source";
		String batch_incremental = "Incremental";
		if (method.toString().contains("BATCH"))
			batch_incremental = "Batch";
		return entity_src+"_"+batch_incremental;
	}

}
