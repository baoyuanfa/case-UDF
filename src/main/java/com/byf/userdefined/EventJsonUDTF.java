package com.byf.userdefined;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        List<String> structFieldNames = new ArrayList<>();
        List<ObjectInspector> structFieldObjectInterceptors = new ArrayList<>();


        structFieldNames.add("event_name");
        structFieldObjectInterceptors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        structFieldNames.add("event_json");
        structFieldObjectInterceptors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,structFieldObjectInterceptors);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String input = args[0].toString();

        if (StringUtils.isBlank(input)) {
            return;
        } else {
            try {
                JSONArray jsonArray = new JSONArray(input);

                if (jsonArray == null) {
                    return;
                }
                for (int i = 0; i < jsonArray.length(); i++) {
                    String[] result = new String[2];

                    try {
                        result[0] = jsonArray.getJSONObject(i).getString("en");
                        result[1] = jsonArray.getString(i);
                    } catch (JSONException e) {
                        continue;
                    }
                    forward(result);

                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }


    }

    @Override
    public void close() throws HiveException {

    }
}
