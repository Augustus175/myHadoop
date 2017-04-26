package org.conan.myhadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by zhangzhibo on 17-4-26.
 */
public class HdfsExample {
    public static void testMkdirPath(String path) throws Exception {
        FileSystem fs = null;
        try {
            System.out.println("Create " + path + " on hdfs ...");
            Configuration con = new Configuration();
            Path myPath = new Path(path);
            fs = myPath.getFileSystem(con);
            fs.mkdirs(myPath);
            System.out.println("Create " + path + " on hdfs successfully ! ");
        }catch(Exception e)
        {
            System.out.println("Exception: " + e);
        }finally{
            if (fs != null) {
                fs.close();
            }
        }
    }
    public static void testDeletePath(String path) throws Exception{
        FileSystem fs = null;
        try{
            System.out.println("Delete " + path + " on hdfs ...");
            Configuration con = new Configuration();
            Path myPath = new Path(path);
            fs = myPath.getFileSystem(con);
            fs.delete(myPath,true);
            System.out.println("Delete " + path + " on hdfs successfully ! ");
        }catch(Exception e)
        {
            System.out.println("Exception: " + e);
        }finally{
            if (fs != null) {
                fs.close();
            }
        }
    }

    public static void main(String[] args) {
        try {
            String path = "hdfs://master:9000/test/mkdir-test";
            testMkdirPath(path);
//            testDeletePath(path);
        }catch (Exception e){
            System.out.println("Exceptions : "+e);
        }
        System.out.println("TimeStamp : "+System.currentTimeMillis());

    }
}
