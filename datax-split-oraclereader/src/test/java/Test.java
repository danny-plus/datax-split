import com.alibaba.datax.common.util.Configuration;
import ni.danny.dataxsplit.base.DataxSplitCallback;
import ni.danny.dataxsplit.oracle.OracleDataxSplitServiceImpl;

import java.util.List;

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
                "                \"channel\":2\n" +
                "            }\n" +
                "        },\n" +
                "        \"content\":[\n" +
                "            {\n" +
                "                \"reader\":{\n" +
                "                    \"name\":\"oraclereader\",\n" +
                "                    \"parameter\":{\n" +
                "                        \"username\":\"cjtest\",\n" +
                "                        \"password\":\"cjtest\",\n" +
                "                        \"connection\":[\n" +
                "                            {\n" +
                "                                \"jdbcUrl\":[\n" +
                "                                    \"jdbc:oracle:thin:@192.168.8.2:1521:orcl\"\n" +
                "                                ],\n" +
                "                                \"querySql\":[\n" +
                "                                    \"select INFO from CJTEST.DATAX_TEST \"\n" +
                "                                ]\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    }\n" +
                "                },\n" +
                "                \"writer\":{\n" +
                "                    \"name\":\"oraclewriter\",\n" +
                "                    \"parameter\":{\n" +
                "                        \"username\":\"cjtest\",\n" +
                "                        \"password\":\"cjtest\",\n" +
                "                        \"column\":[\n" +
                "                            \"info\"\n" +
                "                        ],\n" +
                "                        \"preSql\":[\n" +
                "                           \n" +
                "                        ],\n" +
                "                        \"connection\":[\n" +
                "                            {\n" +
                "                                \"jdbcUrl\":\"jdbc:oracle:thin:@192.168.8.2:1521:orcl\",\n" +
                "                                \"table\":[\n" +
                "                                    \"CJTEST.DATAX_FROM \"\n" +
                "                                ]\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "}";

        OracleDataxSplitServiceImpl impl = new OracleDataxSplitServiceImpl();
        impl.split(json, new DataxSplitCallback() {
            public void callback(List<Configuration> configurationList) {
                int i=0;
                for(Configuration configuration:configurationList){
                    i++;
                    System.out.println(i);
                    System.out.println(configuration);
                }
            }
        });

    }
}
