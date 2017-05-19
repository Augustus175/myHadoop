package org.conan.myhadoop.HadoopDefinitiveGuide.chap04;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;


import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
/**
 * Created by zhangzhibo on 17-5-15.
 */
public class ComparableApp {
    public static void main(String[] args) {
        IntWritable w1 = new IntWritable(163);
        IntWritable w2 = new IntWritable(67);
        RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);
        assertThat(comparator.compare(w1,w2), greaterThan(0));
//        assertThat(comparator.compare(w1,w2),);
//        assertThat( 12, allOf( greaterThan(8), lessThan(16) ) );
    }
}
