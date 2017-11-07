package excelreadwrite

import java.io._
import java.util

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileConstants, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, Reader, RecordReader}
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.mapred.OutputLogFilter
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._


/**
  created by Vaijnathp
  *
  */
class avroMerge{

  val fs = FileSystem.get(new Configuration)
  val inPath: Path = new Path("getSourcePath");
  val outPath: Path = new Path("getDestinationPath")
  val contents: Array[FileStatus] = fs.listStatus(inPath, new OutputLogFilter)
  fs.setWriteChecksum(false)
  val hasHeader =true
  val deleteSource=true

  def addExtension(fileType: String, outputFileName: String): String = {
    val split = outputFileName.split("\\.")
    if (!split(split.length - 1).equals(fileType))
      outputFileName + "." + fileType
    else
      outputFileName
  }

  def main(args: Array[String]): Unit = {
    val fileType=""
    fileType match {
      case "avro" => mergeAvroFile
    }
    if (deleteSource){
      if(!outPath.getName.equals(inPath.getName)){
        fs.delete(inPath,true)
      }
      else {
        contents.foreach(e=> if (!e.getPath.getName.equals(outPath.getName)){
         fs.delete(e.getPath,false)
        })
      }
    }
  }


  def mergeAvroFile: Unit = {
    val writer: DataFileWriter[GenericRecord] = new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord])
    var schema: Schema = null
    var inputCodec: String = null
    val metadata: util.Map[String, Array[Byte]] = new util.TreeMap[String, Array[Byte]]
    val output: BufferedOutputStream = new BufferedOutputStream(new BufferedOutputStream(fs.create(outPath)))
    contents foreach (folderContent => if (folderContent.isFile && folderContent.getPath.getName.endsWith(".avro")) {
      val input: InputStream = new BufferedInputStream(fs.open(folderContent.getPath))
      val reader: DataFileStream[GenericRecord] = new DataFileStream[GenericRecord](input, new GenericDatumReader[GenericRecord])
      if (schema == null) {
        schema = reader.getSchema
        extractAvroFileMetadata(writer, metadata, reader)
        inputCodec = reader.getMetaString(DataFileConstants.CODEC)
        if (inputCodec == null) inputCodec = DataFileConstants.NULL_CODEC
        writer.setCodec(CodecFactory.fromString(inputCodec))
        writer.create(schema, output)
      }
      else {
        if (!schema.equals(reader.getSchema)) {
          reader.close()
        }
        CompareAvroFileMetadata(metadata, reader, folderContent.getPath.getName)
        var thisCodec: String = reader.getMetaString(DataFileConstants.CODEC)
        if (thisCodec == null) thisCodec = DataFileConstants.NULL_CODEC
        if (!inputCodec.equals(thisCodec)) {
          reader.close()
        }
      }
      writer.appendAllFrom(reader, false)
      reader.close()
    })
    writer.close()
  }

  def CompareAvroFileMetadata(metadata: util.Map[String, Array[Byte]], reader: DataFileStream[GenericRecord], filename: String): Unit = {
    for (key <- reader.getMetaKeys) {
      if (!DataFileWriter.isReservedMeta(key)) {
        val metadatum: Array[Byte] = reader.getMeta(key)
        val writersMetadatum: Array[Byte] = metadata.get(key)
        if (!util.Arrays.equals(metadatum, writersMetadatum)) {
          reader.close()
        }
      }
    }
  }

  def extractAvroFileMetadata(writer: DataFileWriter[GenericRecord], metadata: util.Map[String, Array[Byte]], reader: DataFileStream[GenericRecord]): Unit = {
    for (key <- reader.getMetaKeys) {
      if (!DataFileWriter.isReservedMeta(key)) {
        val metadatum: Array[Byte] = reader.getMeta(key)
        metadata.put(key, metadatum)
        writer.setMeta(key, metadatum)
      }
    }
  }

}
