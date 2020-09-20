package ni.danny.dataxsplit.oracle;

import com.alibaba.datax.common.util.Configuration;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxsplit.base.impl.DataxSplitServiceImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * @author bingobing
 */
@Slf4j
public class OracleDataxSplitServiceImpl extends DataxSplitServiceImpl {
    @Override
    protected List<Configuration> split(Configuration splitStrategy,Configuration content) {
        log.debug("splitStrategy = [{}]",splitStrategy);
        log.debug("content = [{}]",content);
        List<Configuration> resultList = new ArrayList<Configuration>();
            Configuration reader = content.getConfiguration(OracleKey.READER_PARAMETER);
            String readerName = content.getString(OracleKey.READER_NAME);

            if(!OracleKey.ORACLE_READER.equals(readerName.toLowerCase())){
                resultList.add(content);
                return resultList;
            }
        List<List> splits = splitStrategy.get(OracleKey.SPLIT_STRATEGY_SPLITS,ArrayList.class);
        String column = splitStrategy.getString(OracleKey.SPLIT_STRATEGY_COLUMN);
        for(int i=0,z=splits.size();i<z;i++){

            List<Configuration> connectionList = reader.getListConfiguration(OracleKey.CONNECTION);
            List<Configuration> newConnectionList = new ArrayList<Configuration>(connectionList.size());
            for(int j=0,y=connectionList.size();j<y;j++){
                Configuration connection = connectionList.get(j);
                Configuration newConnection = connection.clone();
                List<String> querySqls = connection.getList(OracleKey.QUERYSQL,String.class);
                List<String> newQuerySqls = new ArrayList<String>(querySqls.size());
                for(int k=0,x=querySqls.size();k<x;k++){
                    String sql = querySqls.get(k);
                    StringBuilder newSql = new StringBuilder(sql);
                    if(!sql.toUpperCase().contains("WHERE")){
                        newSql.append(" WHERE ");
                    }else{
                        newSql.append(" AND ");
                    }

                    newSql.append( column+">= '"+splits.get(i).get(0)+"'" );
                    if(splits.get(i).size()==2){
                        newSql.append(" AND "+column+" <'"+splits.get(i).get(1)+"'");
                    }
                    newQuerySqls.add(newSql.toString());
                }
                newConnection.set(OracleKey.QUERYSQL,newQuerySqls);
                newConnectionList.add(newConnection);
            }
            Configuration newContent = content.clone();
            Configuration newParameter = content.getConfiguration(OracleKey.READER_PARAMETER).clone();
            newParameter.set(OracleKey.CONNECTION,newConnectionList);
            newContent.set(OracleKey.READER_PARAMETER,newParameter);
            resultList.add(newContent);
        }
        return resultList;
    }
}
