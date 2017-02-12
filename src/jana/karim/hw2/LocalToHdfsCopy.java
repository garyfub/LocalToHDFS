package jana.karim.hw2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.util.logging.Level;
import java.util.logging.Logger;


public class LocalToHdfsCopy {

        public static void main(String[] args) throws IOException {
            File[] local_files;
            Path hdfs_fs_dir_path = null;
            String number_of_threads;
            FileSystem hdfs = null;
            File local_fs_dir;
            CompressionCodec compressionCode = new GzipCodec();

            Configuration hdfs_configuration = new Configuration();
            System.out.println(hdfs_configuration.get("fs.default.name"));

            boolean hdfs_dir_status = false;

            try{
                hdfs = FileSystem.get(hdfs_configuration);

            }catch(IOException ex){
                Logger.getLogger(jana.karim.hw2.LocalToHdfsCopy.class.getName()).log(Level.SEVERE, null, ex);
            }


            if(args.length != 3) {
                System.err.println("Need 3 arguments");
                System.exit(0);
            }


            local_fs_dir = new File(args[0]);

            if (!local_fs_dir.exists()){
                System.err.println("Source directory does not exist");
                System.exit(0);
            }

            hdfs_fs_dir_path = new Path(args[1]);

            try{
                if(hdfs.exists(hdfs_fs_dir_path)){
                    System.err.println("Destination directory already exists.  Please delete before running the program.");
                    System.exit(0);
                }
                else {

                    hdfs_dir_status = hdfs.mkdirs(hdfs_fs_dir_path);
                    System.out.println("Created directory succesfully:" + hdfs_dir_status);

                }
            }catch (IOException ex){
                Logger.getLogger(jana.karim.hw2.LocalToHdfsCopy.class.getName()).log(Level.SEVERE, null, ex);
            }


            local_files = local_fs_dir.listFiles();
            FileInputStream sourceFile = null;

            for (File f : local_files){

                System.out.println(f.getName());
                sourceFile = new FileInputStream(f);

                FSDataOutputStream fsDataOutputStream = hdfs.create(Path.mergePaths(hdfs_fs_dir_path,
                        new Path("/" + f.getName())));

                CompressionOutputStream compressedOutputStream =
                        compressionCode.createOutputStream(fsDataOutputStream);

                System.out.println(Path.mergePaths(hdfs_fs_dir_path,
                        new Path("/"+f.getName())).toUri().toString());

                FSDataInputStream fsDataInputStream  = FileSystem.getLocal(hdfs_configuration)
                        .open(new Path(f.getAbsolutePath()));


                sourceFile.close();
                IOUtils.copyBytes(fsDataInputStream, compressedOutputStream, hdfs_configuration);
                fsDataInputStream.close();
                fsDataOutputStream.close();
                compressedOutputStream.close();

            }

        }
    }


