package cascading.hcatalog;

import junit.framework.TestCase;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Anton Sorhus <anton.sorhus@visualdna.com>
 */
public class DataStorageLocationTest extends TestCase {

    public void testGetPartitions() throws Exception {

        DataStorageLocation instance = new DataStorageLocation("",new LinkedList<String>());

        String path = "hdfs://namenode.host.com/path/to/data/topic=pageviews/d=2014-01-01";
        List<String> values = new LinkedList<String>() {{
            add("pageviews");
            add("2014-01-01");
        }};

        Map<String, String> result = instance.getPartitions(path, values);

        assertEquals(result.get("d"), "2014-01-01");
        assertEquals(result.get("topic"), "pageviews");

    }
}
