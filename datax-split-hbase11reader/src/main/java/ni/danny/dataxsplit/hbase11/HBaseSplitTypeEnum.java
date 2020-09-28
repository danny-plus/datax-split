package ni.danny.dataxsplit.hbase11;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * @author danny_ni
 */
@Getter
@AllArgsConstructor
public enum HBaseSplitTypeEnum {
    ONE("O",1),
    TEN("T",10),
    HUNDRED("H",100);
    private String code;
    private int number;

    public static HBaseSplitTypeEnum getSplitTypeByCode(String code){
        for(HBaseSplitTypeEnum typeEnum : HBaseSplitTypeEnum.values()){
            if(StringUtils.equals(code, typeEnum.getCode())){
                return typeEnum;
            }
        }
        return null;
    }

}
