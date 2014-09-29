package com.guavus.acume.cache.watcher

import java.nio.file.WatchService
import java.nio.file.FileSystems
import java.nio.file.Path
//import com.guavus.acume.cache.configuration.AcumeConfiguration
import java.nio.file.StandardWatchEventKinds._
import java.nio.file.Paths
import java.nio.file.WatchKey
import java.nio.file.WatchEvent
import scala.collection.JavaConversions._
import com.guavus.acume.cache.utility.FileWrapper

object ORCWatcher {

//  val watcher: WatchService = FileSystems.getDefault().newWatchService();
//  val path = Paths.get(AcumeConfiguration.ORCBasePath.getValue());
//  path.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY)
  
//  def runWatcher() { 
//  
//    val fileWatcher = new MyWatchQueueReader(watcher);
//    val th = new Thread(fileWatcher, "FileWatcher");
//    th.start();
//    th.join();
//  }
  
  def main(args: Array[String]) { 
    
    val wch: WatchService = FileSystems.getDefault().newWatchService();
    val base = ""//AcumeConfiguration.ORCBasePath.getValue() + "/" + AcumeConfiguration.InstaInstanceId.getValue(); 
    val list = FileWrapper.listFolders(base, -1)
    list.foreach(Paths.get(_).register(wch, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY, OVERFLOW))
    val fileWatcher = new MyWatchQueueReader(wch);
    val th = new Thread(fileWatcher, "FileWatcher");
    th.start();
    th.join();
  }
}

class MyWatchQueueReader(myWatcher: WatchService) extends Runnable {
  
  override def run() {
    try {
      var key: WatchKey = myWatcher.take();
      while(key != null) {
        for (event <- key.pollEvents() if(!event.context().toString.startsWith("."))) {
          val _x: WatchEvent.Kind[Path] = event.kind().asInstanceOf[WatchEvent.Kind[Path]];
          _x match {
            //Only tracking create entries, it is not expected to modify old file.
            case ENTRY_CREATE => 
//            case OVERFLOW =>
          }
        }
        key.reset();
        key = myWatcher.take();
      }
    } catch { 
    case ex: InterruptedException => ex.printStackTrace();
    }
  }
}