import com.alibaba.datax.common.util.Configuration;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxsplit.base.DataxSplitCallback;
import ni.danny.dataxsplit.hbase11.HBase11DataxSplitServiceImpl;

import java.util.List;
@Slf4j
public class Test {
    public static void main(String[] args){
        String json="{\n" +
                "    \"jobId\":\"0009999999\",\n" +
                "    \"splitStrategy\":{\n" +
                "        \"type\":\"oracleReaderStrategy\",\n" +
                "        \"strategy\":{\n" +
                "            \"splits\":[\n" +
                "                [\n" +
                "                    \"0\",\n" +
                "                    \"10000\"\n" +
                "                ],\n" +
                "                [\n" +
                "                    \"10000\",\n" +
                "                    \"20000\"\n" +
                "                ],\n" +
                "                [\n" +
                "                    \"20000\",\n" +
                "                    \"30000\"\n" +
                "                ],\n" +
                "                [\n" +
                "                    \"30000\",\n" +
                "                    \"40000\"\n" +
                "                ],\n" +
                "                [\n" +
                "                    \"40000\",\n" +
                "                    \"50000\"\n" +
                "                ],\n" +
                "                [\n" +
                "                    \"50000\",\n" +
                "                    \"60000\"\n" +
                "                ],\n" +
                "                [\n" +
                "                    \"60000\",\n" +
                "                    \"70000\"\n" +
                "                ],\n" +
                "                [\n" +
                "                    \"70000\",\n" +
                "                    \"999999\"\n" +
                "                ]\n" +
                "            ],\n" +
                "            \"column\":\"ID\"\n" +
                "        }\n" +
                "    },\n" +
                "    \"job\":{\n" +
                "        \"setting\":{\n" +
                "            \"speed\":{\n" +
                "                \"channel\":1\n" +
                "            }\n" +
                "        },\n" +
                "        \"content\":[\n" +
                "            {\n" +
                "                \"reader\":{\n" +
                "                    \"name\":\"hbase11xreader\",\n" +
                "                    \"parameter\":{\n" +
                "                        \"hbaseConfig\":{\n" +
                "                            \"hbase.zookeeper.quorum\":\"xxxf\"\n" +
                "                        },\n" +
                "                        \"table\":\"users\",\n" +
                "                        \"encoding\":\"utf-8\",\n" +
                "                        \"mode\":\"normal\",\n" +
                "                        \"column\":[\n" +
                "                            {\n" +
                "                                \"name\":\"rowkey\",\n" +
                "                                \"type\":\"string\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"name\":\"info: age\",\n" +
                "                                \"type\":\"string\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"name\":\"info: birthday\",\n" +
                "                                \"type\":\"date\",\n" +
                "                                \"format\":\"yyyy-MM-dd\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"name\":\"info: company\",\n" +
                "                                \"type\":\"string\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"name\":\"address: contry\",\n" +
                "                                \"type\":\"string\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"name\":\"address: province\",\n" +
                "                                \"type\":\"string\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"name\":\"address: city\",\n" +
                "                                \"type\":\"string\"\n" +
                "                            }\n" +
                "                        ],\n" +
                "                        \"range\":{\n" +
                "                            \"startRowkey\":\"\",\n" +
                "                            \"endRowkey\":\"\",\n" +
                "                            \"isBinaryRowkey\":true\n" +
                "                        }\n" +
                "                    }\n" +
                "                },\n" +
                "                \"writer\":{\n" +
                "                    \"name\":\"txtfilewriter\",\n" +
                "                    \"parameter\":{\n" +
                "                        \"path\":\"/Users/shf/workplace/datax_test/hbase11xreader/result\",\n" +
                "                        \"fileName\":\"qiran\",\n" +
                "                        \"writeMode\":\"truncate\"\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "}";

        HBase11DataxSplitServiceImpl impl = new HBase11DataxSplitServiceImpl();
        impl.split(json, new DataxSplitCallback() {
            public void callback(List<Configuration> configurationList) {
                for(Configuration te:configurationList){
                    log.info("[==>{}<==]",te);
                }
            }
        });

    }
}
