package org.conan.myhadoop.HadoopDefinitiveGuide.chap04;

import org.apache.hadoop.io.Text;

/**
 * Created by zhangzhibo on 17-5-16.
 */
public class Texttest {
    public static void main(String[] args) {
        Text t = new Text("hadoop");
        String s = "hadoop";
        System.out.println(t.charAt(2));
        System.out.println(s.charAt(2));
        String ss = "\u0041\u00DF\u6771\uD801\uDC00";
        String ss2 = "\uD801\uDC00";
        System.out.println(ss);
        System.out.println(ss2);

        Text t1 = new Text("\u0041\u00DF\u6771\uD801\uDC00");
        System.out.println(t1);
    }
}
