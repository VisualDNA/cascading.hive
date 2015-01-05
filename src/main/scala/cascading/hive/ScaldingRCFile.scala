package cascading.hive

import java.io.{OutputStream, InputStream}
import java.util.Properties

import cascading.scheme.local.TextDelimited
import cascading.scheme.Scheme
import com.twitter.scalding.FixedPathSource
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}
import cascading.tuple.Fields

case class ScaldingRCFile(path: String, fields: Array[String], types: Array[String], columns: String)
  extends FixedPathSource(path) {

  override def hdfsScheme = {
    new cascading.hive.RCFile(fields, types, columns)
      .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
  }

  override def localScheme = {
    val selected = columns.split(",").map(_.toInt).map(fields(_))
    new TextDelimited(new Fields(selected.toSeq: _*), false, false, "\t", null, null)
      .asInstanceOf[Scheme[Properties, InputStream, OutputStream, _, _]]
  }


  override def equals(that : Any) = that match {
    case that : ScaldingRCFile => {
      this.path.eq(that.path) &&
        this.fields.sameElements(that.fields) &&
        this.types.sameElements(that.types) &&
        this.columns.eq(that.columns)
    }
    case _ => false
  }

  override def hashCode() = {
    (41 * 41 * (41 * (41 + path.hashCode) + fields.hashCode) + types.hashCode) + columns.hashCode
  }

}

case class ScaldingRCFiles(paths: Seq[String], fields: Array[String], types: Array[String], columns: String)
  extends FixedPathSource(paths: _*) {

  override def hdfsScheme = {
    new cascading.hive.RCFile(fields, types, columns)
      .asInstanceOf[Scheme[JobConf,RecordReader[_,_],OutputCollector[_,_],_,_]]
  }

  override def localScheme = {
    val selected = columns.split(",").map(_.toInt).map(fields(_))
    new TextDelimited(new Fields(selected.toSeq: _*), false, false, "\t", null, null)
      .asInstanceOf[Scheme[Properties, InputStream, OutputStream, _, _]]
  }

  override def equals(that : Any) = that match {
    case that : ScaldingRCFiles => {
      this.paths.sameElements(that.paths) &&
        this.fields.sameElements(that.fields) &&
        this.types.sameElements(that.types) &&
        this.columns.equals(that.columns)
    }
    case _ => false
  }

  override def hashCode() = {
    (41 * 41 * (41 * (41 + paths.hashCode) + fields.hashCode) + types.hashCode) + columns.hashCode
  }

  override def toString = "RCFiles: { %s, %s; %s; %s ; %d}".format(
    paths.mkString(","),
    fields.mkString(","),
    types.mkString(","),
    columns.mkString(","),
    hashCode
  )
}