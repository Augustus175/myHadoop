package org.conan.myhadoop.HadoopDefinitiveGuide.chap04;
import java.io.File;
import java.io.Serializable;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Created by zhangzhibo on 17-5-20.
 */
public class UserTest {
    public static void main(String[] args) throws Exception {

        ///无参构造函数，依赖设值注入
        User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);
        //user1没有对favorite color赋值
        ///注意：User并没有实现Java的Serializable接口
        System.out.println(user1 instanceof Serializable);
        ///重载的构造方法
        User user2 = new User("Ben", 7, "red");

        //使用builder构建对象
        User user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();

        System.out.println(user3.getName() + "," + user3.getFavoriteColor());


        ///1. 序列化包含Schema和3个User对象到/home/zhangzhibo/IdeaProjects/myHadoop/src/main/avro/users.avro文件
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File("/home/zhangzhibo/IdeaProjects/myHadoop/src/main/avro/users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();

        ///2. 从/home/zhangzhibo/IdeaProjects/myHadoop/src/main/avro/users.avro文件反序列化对象
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(new File("/home/zhangzhibo/IdeaProjects/myHadoop/src/main/avro/users.avro"), userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            user = dataFileReader.next(user);
            System.out.println(user);  ///输出JSON格式的数据，Avro对User类的toString进行了改写
        }
    }
}