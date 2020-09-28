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
        if(splits==null||splits.isEmpty()){

            String splitStartPos = splitStrategy.getString(HBase11Key.START_POS);
            String hbaseSplitTypeCode = splitStrategy.getString(HBase11Key.HBASE_TABLE_SPLIT_TYPE);
            HBaseSplitTypeEnum hbaseSplitType = HBaseSplitTypeEnum.getSplitTypeByCode(hbaseSplitTypeCode);
            splits = new ArrayList<List>(hbaseSplitType.getNumber());
            for(int i=0;i<hbaseSplitType.getNumber();i++){
                List<String> tmpList = new ArrayList<String>(2);
                String start = i+"_"+splitStartPos;
                String end = i+"_"+splitStartPos;
                if(HBaseSplitTypeEnum.HUNDRED.equals(hbaseSplitType)&&i<10){
                    start = "0"+start;
                    end = "0"+end;
                }
                tmpList.add(start);
                tmpList.add(end);
                splits.add(tmpList);
            }
        }
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

    public String name() {
        return "hbase11";
    }
}
