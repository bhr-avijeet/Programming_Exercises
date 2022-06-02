package com.npntraining.udfs.hands;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ToUpper extends UDF {

	public Text evaluate(Text text) {
		if (text != null) {
			String str = text.toString().toUpperCase();
			return new Text(str);
		} else {
			return new Text("null");
		}
	}
}
