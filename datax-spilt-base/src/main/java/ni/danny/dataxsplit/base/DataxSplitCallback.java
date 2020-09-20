package ni.danny.dataxsplit.base;

import com.alibaba.datax.common.util.Configuration;

import java.util.List;

/**
 * @author bingobing
 */
public interface DataxSplitCallback {
    /**
     * 各继承模块各自实现具体拆解逻辑
     */
    void callback(List<Configuration> configurationList);
}
