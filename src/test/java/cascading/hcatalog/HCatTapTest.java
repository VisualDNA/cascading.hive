/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.hcatalog;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.planner.PlannerException;
import cascading.operation.Identity;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Coerce;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import junitx.framework.FileAssert;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HCatTapTest {
	private FlowConnector connector;
	private String hcatOut;
	private String hcatIn;
	private String hcatResultFields;
	private String resultPath;

	@Before
	public void setUp() throws Exception {
		connector = new HadoopFlowConnector(new Properties());
		hcatOut = "src/test/resources/data/hcatout.txt";
		hcatIn = "src/test/resources/data/sample_07.csv";
		hcatResultFields = "src/test/resources/data/hcat_result_fields.txt";
		resultPath = "output/";
	}

	@After
	public void tearDown() throws Exception {
		connector = null;
		hcatOut = null;
		hcatIn = null;
		hcatResultFields = null;
		resultPath = null;
	}

	@Test
	public void testDataIn() {
		HCatTap source = new HCatTap("sample_07");
		Lfs output = new Lfs(new TextDelimited(false, "|"), resultPath,
				SinkMode.REPLACE);
        Each pipe = new Each("test", new Identity(new Fields("code", "description", "total_emp", "salary")));
		Flow flow = connector.connect(source, output, pipe);
		flow.complete();

		FileAssert.assertEquals(new File(hcatOut),
				new File(resultPath + "part-00000"));
	}

	@Test
	public void testDataOut() {
		Lfs input = new Lfs(new TextDelimited(new Fields("code", "description",
				"total_emp", "salary"), "|"), hcatOut);
		HCatTap output = new HCatTap("sample_08", resultPath);

		Coerce pipe = new Coerce(new Pipe("test"), new Fields("total_emp"),
				Integer.class);
		pipe = new Coerce(pipe, new Fields("salary"), Integer.class);

		Flow flow = connector.connect(input, output, pipe);
		flow.complete();

		FileAssert.assertEquals(new File(resultPath + "part-00000"), new File(hcatIn));
	}

	@Test
	public void testDataInWithSouceFields() {
		HCatTap source = new HCatTap("sample_07", new Fields("code", "salary"));
		Lfs output = new Lfs(new TextDelimited(false, "|"), resultPath,
				SinkMode.REPLACE);

		Flow flow = connector.connect(source, output, new Pipe("convert"));
		flow.complete();

		FileAssert.assertEquals(new File(hcatResultFields),
				new File(resultPath + "part-00000"));
	}
	
	@Test(expected = PlannerException.class)
	public void testDataInWithInvalidSouceFields() {
		HCatTap source = new HCatTap("sample_07", new Fields("a", "b"));
		Lfs output = new Lfs(new TextDelimited(false, "|"), resultPath,
				SinkMode.REPLACE);

		Flow flow = connector.connect(source, output, new Pipe("convert"));
		flow.complete();
	}

    @Test
    public void testOrcInOut() throws IOException {
        HCatTap source = new HCatTap("test_orc");
        HCatTap output = new HCatTap("test_orc", "output/");
        Pipe pipe = new Pipe("testOrc");
        pipe = new Each(pipe, new Fields("col1"), new ExpressionFilter("col1 > 3", Integer.TYPE));
        Flow flow = connector.connect(source, output, pipe);
        flow.complete();

        TupleEntryIterator iterator = output.openForRead(flow.getFlowProcess());
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count ++;
        }
        assertTrue(count == 7);

        List<DataStorageLocation> location = CascadingHCatUtil.getDataStorageLocation(
            MetaStoreUtils.DEFAULT_DATABASE_NAME, "test_orc", null, (JobConf) flow.getFlowProcess
                ().getConfigCopy()
        );
        assertEquals(location.get(0).path, resultPath);
    }
}
