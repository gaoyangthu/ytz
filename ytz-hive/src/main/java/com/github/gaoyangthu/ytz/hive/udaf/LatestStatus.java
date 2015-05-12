package com.github.gaoyangthu.ytz.hive.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/8/11 0011
 */
@Description(name = "LatestStatus",
	value = "_FUNC_(status, time) - return the latest status by their time",
	extended = "Example:\n:" +
		" > SELECT id, _FUNC_(status, time) FROM table_name GROuP BY id;")
public class LatestStatus extends AbstractGenericUDAFResolver {
	static final Log LOG = LogFactory.getLog(LatestStatus.class.getName());

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
		throws SemanticException {
		if (parameters.length != 2) {
			throw new UDFArgumentTypeException(parameters.length - 1,
				"Exactly two arguments are expected.");
		}
		if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,
				"Only primitive type arguments are accepted but "
					+ parameters[0].getTypeName() + " is passed.");
		}
		if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(1,
				"Only primitive type arguments are accepted but "
					+ parameters[1].getTypeName() + " is passed.");
		}
		return new GenericUDAFLastestStatusEvaluator();
	}

	public static class GenericUDAFLastestStatusEvaluator extends GenericUDAFEvaluator {
		// For PARTIAL1 and COMPLETE
		PrimitiveObjectInspector inputOI0;
		PrimitiveObjectInspector inputOI1;

		// For PARTIAL2 and FINAL
		private StructObjectInspector soi;
		private StructField statusField;
		private StructField timeField;
		private StringObjectInspector statusFieldOI;
		private LongObjectInspector timeFieldOI;

		// For PARTIAL1 and PARTIAL2
		private Object[] partialResult;

		// For FINAL and COMPLETE
		private Text result;

		@Override
		public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
			super.init(mode, parameters);

			// init input
			if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
				assert(parameters.length == 2);
				inputOI0 = (PrimitiveObjectInspector) parameters[0];
				inputOI1 = (PrimitiveObjectInspector) parameters[1];
			} else {
				assert(parameters.length == 1);
				soi = (StructObjectInspector) parameters[0];

				statusField = soi.getStructFieldRef("status");
				timeField = soi.getStructFieldRef("time");
				statusFieldOI = (StringObjectInspector) statusField.getFieldObjectInspector();
				timeFieldOI = (LongObjectInspector) timeField.getFieldObjectInspector();
			}
			// init output
			if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
				ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
				foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
				foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
				ArrayList<String> fname = new ArrayList<String>();
				fname.add("status");
				fname.add("time");
				partialResult = new Object[2];
				partialResult[0] = new Text("");
				partialResult[1] = new LongWritable(0);
				return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
			} else {
				setResult(new Text(""));
				return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
			}
		}

		static class LatestAgg implements AggregationBuffer {
			String status;
			Long time;
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			LatestAgg result = new LatestAgg();
			reset(result);
			return result;
		}

		@Override
		public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
			LatestAgg myagg = (LatestAgg) aggregationBuffer;
			myagg.status = "";
			myagg.time = 0L;
		}

		@Override
		public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
			assert(objects.length == 2);
			Object p0 = objects[0];
			Object p1 = objects[1];
			if (p0 != null && p1 != null) {
				LatestAgg myagg = (LatestAgg) aggregationBuffer;
				String status = PrimitiveObjectInspectorUtils.getString(p0, inputOI0);
				if (PrimitiveObjectInspectorUtils.getString(p1, inputOI1).length() == 0) {
					if (myagg.time == 0) {
						myagg.status = status;
					}
				} else {
					long time = PrimitiveObjectInspectorUtils.getLong(p1, inputOI1);
					if (time >= myagg.time) {
						myagg.status = status;
						myagg.time = time;
					}
				}
			}
		}

		@Override
		public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
			LatestAgg myagg = (LatestAgg) aggregationBuffer;
			((Text) partialResult[0]).set(myagg.status);
			((LongWritable) partialResult[1]).set(myagg.time);
			return partialResult;
		}

		@Override
		public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
			if (partial != null) {
				LatestAgg myagg = (LatestAgg) aggregationBuffer;
				Object partialStatus = soi.getStructFieldData(partial, statusField);
				Object partialTime = soi.getStructFieldData(partial, timeField);
				long timeA = myagg.time;
				long timeB = timeFieldOI.get(partialTime);
				if (timeA <= timeB) {
					myagg.status = statusFieldOI.getPrimitiveJavaObject(partialStatus);
					myagg.time = timeB;
				}
			}
		}

		@Override
		public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
			LatestAgg myagg = (LatestAgg) aggregationBuffer;
			getResult().set(myagg.status);
			return getResult();
		}

		public void setResult(Text result) {
			this.result = result;
		}

		public Text getResult() {
			return result;
		}
	}
}
