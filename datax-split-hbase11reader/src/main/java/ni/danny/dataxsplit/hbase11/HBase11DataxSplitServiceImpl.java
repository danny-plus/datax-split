package ni.danny.dataxsplit.hbase11;

import com.alibaba.datax.common.util.Configuration;
import lombok.extern.slf4j.Slf4j;
import ni.danny.dataxsplit.base.impl.DataxSplitServiceImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * @author bingobing
 */
@Slf4j
public class HBase11DataxSplitServiceImpl extends DataxSplitServiceImpl {
    @Override
    protected List<Configuration> split(Configuration splitStrategy, Configuration content) {
        List<Configuration> resultList = new ArrayList<Configuration>();
        String readerName = content.getString(HBase11Key.READER_NAME);
        if(!HBase11Key.HBASE11_READER.equals(readerName.toLowerCase())){
            resultList.add(content);
            return resultList;
        }
        List<List> splits = splitStrategy.get(HBase11Key.SPLIT_STRATEGY_SPLITS,ArrayList.class);
        for(int i=0,z=splits.size();i<z;i++){
            log.info("splits===>[{}]",splits.get(i));
            Configuration parameter = content.getConfiguration(HBase11Key.READER_PARAMETER);
            Configuration newParameter = parameter.clone();
            newParameter.set(HBase11Key.RANGE_START,splits.get(i).get(0));
            if(splits.get(i).size()==2){
                newParameter.set(HBase11Key.RANGE_END,splits.get(i).get(1));
            }
            log.info("newParameter===>[{}]",newParameter);
            Configuration newContent = content.clone();
            newContent.set(HBase11Key.READER_PARAMETER,newParameter);
            log.info("newContent===>[{}]",newContent);
            resultList.add(newContent);
        }
        return resultList;
    }
}
