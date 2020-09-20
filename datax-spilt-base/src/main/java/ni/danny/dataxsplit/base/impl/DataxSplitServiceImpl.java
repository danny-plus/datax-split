package ni.danny.dataxsplit.base.impl;

import com.alibaba.datax.common.util.Configuration;
import ni.danny.dataxsplit.base.DataxSplitCallback;
import ni.danny.dataxsplit.base.DataxSplitService;
import ni.danny.dataxsplit.base.Key;

import java.util.ArrayList;
import java.util.List;

/**
 * @author bingobing
 */
public abstract class DataxSplitServiceImpl implements DataxSplitService {
    @Override
    public final void split(String dataxJson, DataxSplitCallback callback) {
        Configuration configuration = init(dataxJson);
        List<Configuration> list = new ArrayList<>();
        List<Configuration> contentList = configuration.getListConfiguration(Key.JOB_CONTENT);
        Configuration splitStrategy = configuration.getConfiguration(Key.SPLIT_STRATEGY);
        for(int i=0,z=contentList.size();i<z;i++){
            List<Configuration> newContentList = split(splitStrategy,contentList.get(i));
            for(int j=0,y=newContentList.size();j<y;j++){
                List tempContentList = new ArrayList();
                tempContentList.add(newContentList.get(j));
                Configuration newConfiguration = configuration.clone();
                newConfiguration.set(Key.JOB_CONTENT,tempContentList);
                newConfiguration.set(Key.TASK_ID,(i+1)*(j+1));
                newConfiguration.remove(Key.SPLIT_STRATEGY);
                list.add(newConfiguration);
            }
        }
        callback.callback(list);
    }

    private final Configuration init(String dataxJson) {
      return Configuration.from(dataxJson);
    }

    protected abstract List<Configuration> split(Configuration splitStrategy,Configuration content);

}
