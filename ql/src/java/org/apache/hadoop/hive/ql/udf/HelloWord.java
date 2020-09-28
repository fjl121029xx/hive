package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * UDF to extract specfic parts from URL For example,
 * parse_url('http://facebook.com/path/p1.php?query=1', 'HOST') will return
 * 'facebook.com' For example,
 * parse_url('http://facebook.com/path/p1.php?query=1', 'PATH') will return
 * '/path/p1.php' parse_url('http://facebook.com/path/p1.php?query=1', 'QUERY')
 * will return 'query=1'
 * parse_url('http://facebook.com/path/p1.php?query=1#Ref', 'REF') will return
 * 'Ref' parse_url('http://facebook.com/path/p1.php?query=1#Ref', 'PROTOCOL')
 * will return 'http' Possible values are
 * HOST,PATH,QUERY,REF,PROTOCOL,AUTHORITY,FILE,USERINFO Also you can get a value
 * of particular key in QUERY, using syntax QUERY:&lt;KEY_NAME&gt; eg: QUERY:k1.
 */
@Description(
        name = "my_hello",
        value = "my_hello(String)",
        extended = "return new Text(\"HelloWord:\" + s.toString());"
)
public class HelloWord extends GenericUDF {

    //
    private StringObjectInspector stringObjectInspector01;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        ObjectInspector a = arguments[0];
        this.stringObjectInspector01 = (StringObjectInspector) a;
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        String a = this.stringObjectInspector01.getPrimitiveJavaObject(arguments[0].get());

        return a + "~hello world";
    }

    @Override
    public String getDisplayString(String[] children) {
        return children[0];
    }
}
