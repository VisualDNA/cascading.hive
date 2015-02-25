package cascading.hive

import java.io.{OutputStream, InputStream}
import java.util.Properties

import cascading.scheme.Scheme
import cascading.scheme.local.TextDelimited
import cascading.tuple.Fields
import com.twitter.scalding.FixedPathSource
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}

class ScaldingORCFile(path: String, fields: Array[String], types: Array[String], columns: String)
      extends FixedPathSource(path) {

    override def hdfsScheme = {
      new cascading.hive.ORCFile(fields, types, columns)
        .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
    }

    override def localScheme = {
      val selected = columns.split(",").map(_.toInt).map(fields(_))
      new TextDelimited(new Fields(selected.toSeq: _*), false, false, "\t", null, null)
        .asInstanceOf[Scheme[Properties, InputStream, OutputStream, _, _]]
    }


  }
