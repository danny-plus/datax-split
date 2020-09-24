package ni.danny.dataxsplit.base;

import com.alibaba.datax.common.util.Configuration;
import java.util.List;

/**
 * @author bingobing
 */
public interface DataxSplitService{
    void split(String dataxJson,DataxSplitCallback callback);
    String name();
}
