package cascading.hcatalog;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Anton Sorhus <anton.sorhus@visualdna.com>
 */
public class DataStorageLocation implements Serializable {

    final String path;
    final Map<String, String> partitions;

    public DataStorageLocation(String path, List<String> values) {
        this.path = path;
        this.partitions = getPartitions(path, values);
    }

    public DataStorageLocation() {
        path = "";
        partitions = new HashMap<String, String>();
    }

    Map<String, String> getPartitions(String location, List<String> values) {
        try {
            Map<String, String> partitions = new HashMap<String, String>();
            File file = new File(new URI(location).getPath());
            while(file != null) {
                String name = file.getName();
                if(name.contains("=")) {
                    String[] split = name.split("=");
                    if(values.contains(split[1])) {
                        partitions.put(split[0], split[1]);
                    }
                }
                file = file.getParentFile();
            }
            return partitions;
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return new HashMap<String, String>();
    }

}
