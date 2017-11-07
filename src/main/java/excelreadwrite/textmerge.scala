
package excelreadwrite

import java.io._
import java.util

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileConstants, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.mapred.OutputLogFilter

import scala.collection.JavaConversions._



/**
created by Vaijnathp
  *
  */
class textmerge{

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
      case "txt" => mergeTextFile
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

  def mergeTextFile: Unit = {
    hasHeader match {
      case true => textFileHasHeader(fs, inPath, outPath)
      case false => FileUtil.copyMerge(fs, inPath, fs, outPath, deleteSource, new Configuration(), null)
    }
  }

  def textFileHasHeader(fs: FileSystem, inPath: Path, outPath: Path): Unit = {
    val out: OutputStreamWriter = new OutputStreamWriter(fs.create(outPath))
    try {
      var i: Int = 0
      contents foreach (folderContent => {
        if (folderContent.isFile) {
          val in: InputStream = fs.open(folderContent.getPath)
          val br: BufferedReader = new BufferedReader(new InputStreamReader(in))
          if (i != 0)
            br.readLine()
          var line: String = br.readLine()
          while ((line != null)) {
            out.write(line + "\n")
            line = br.readLine()
          }
          in.close()
        }
        i += 1;
      })
    }
    catch {
      case e => new RuntimeException("Error while opening files " + e.getMessage)
    }
    finally {
      out.close()
    }
  }
}
